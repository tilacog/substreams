package pipeline

import (
	"bytes"
	"context"
	"fmt"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/streamingfast/substreams/pipeline/outputs"
	"github.com/streamingfast/substreams/state"
	"github.com/streamingfast/substreams/wasm"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	ttrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type ErrorExecutor struct {
	message    string
	stackTrace []string
}

func (e *ErrorExecutor) Error() string {
	b := bytes.NewBuffer(nil)

	b.WriteString(e.message)

	if len(e.stackTrace) > 0 {
		// stack trace section will also contain the logs of the execution
		b.WriteString("\n----- stack trace -----\n")
		for _, stackTraceLine := range e.stackTrace {
			b.WriteString(stackTraceLine)
			b.WriteString("\n")
		}
	}

	return b.String()
}

type ModuleExecutor interface {
	// Name returns the name of the module as defined in the manifest.
	Name() string

	// String returns the module executor representation, usually its name directly.
	String() string

	// Reset the wasm instance, avoid propagating logs.
	Reset()

	run(ctx context.Context, vals map[string][]byte, clock *pbsubstreams.Clock, cursor string) error

	moduleLogs() (logs []string, truncated bool)
	moduleOutputData() pbsubstreams.ModuleOutputData
	getCurrentExecutionStack() []string
}

type BaseExecutor struct {
	moduleName string
	wasmModule *wasm.Module
	wasmInputs []*wasm.Input
	cache      *outputs.OutputCache
	isOutput   bool // whether output is enabled for this module
	entrypoint string
	tracer     ttrace.Tracer
}

var _ ModuleExecutor = (*MapperModuleExecutor)(nil)

type MapperModuleExecutor struct {
	BaseExecutor
	outputType   string
	mapperOutput []byte
}

var _ ModuleExecutor = (*StoreModuleExecutor)(nil)

// Name implements ModuleExecutor
func (e *MapperModuleExecutor) Name() string {
	return e.moduleName
}

func (e *MapperModuleExecutor) String() string {
	return e.moduleName
}

type StoreModuleExecutor struct {
	BaseExecutor
	outputStore *state.Store
}

// Name implements ModuleExecutor
func (e *StoreModuleExecutor) Name() string {
	return e.moduleName
}

func (e *StoreModuleExecutor) String() string {
	return e.moduleName
}

func (e *MapperModuleExecutor) run(ctx context.Context, vals map[string][]byte, clock *pbsubstreams.Clock, cursor string) error {
	ctx, span := e.tracer.Start(ctx, "exec_map")
	span.SetAttributes(attribute.String("module", e.moduleName))
	defer span.End()

	output, found := e.cache.Get(clock)
	if found {
		e.mapperOutput = output
		span.SetStatus(codes.Ok, "cache_hit")
		return nil
	}

	if err := e.wasmMapCall(ctx, vals, clock); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if err := e.cache.Set(clock, cursor, e.mapperOutput); err != nil {
		return fmt.Errorf("setting mapper output to cache at block %d: %w", clock.Number, err)
	}

	span.SetStatus(codes.Ok, "module_executed")
	return nil
}

func (e *StoreModuleExecutor) run(ctx context.Context, vals map[string][]byte, clock *pbsubstreams.Clock, cursor string) error {
	ctx, span := e.tracer.Start(ctx, "exec_store")
	span.SetAttributes(attribute.String("module", e.moduleName))
	defer span.End()

	output, found := e.cache.Get(clock)

	if found {
		deltas := &pbsubstreams.StoreDeltas{}
		err := proto.Unmarshal(output, deltas)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("unmarshalling output deltas: %w", err)
		}
		e.outputStore.Deltas = deltas.Deltas
		for _, delta := range deltas.Deltas {
			e.outputStore.ApplyDelta(delta)
		}
		span.SetStatus(codes.Ok, "cache_hit")
		return nil
	}

	if err := e.wasmStoreCall(ctx, vals, clock); err != nil {
		return err
	}

	deltas := &pbsubstreams.StoreDeltas{
		Deltas: e.outputStore.Deltas,
	}
	data, err := proto.Marshal(deltas)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("caching: marshalling delta: %w", err)
	}
	if err = e.cache.Set(clock, cursor, data); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("setting delta to cache at block %d: %w", clock.Number, err)
	}

	span.SetStatus(codes.Ok, "module_executed")

	return nil
}

func (e *MapperModuleExecutor) wasmMapCall(ctx context.Context, vals map[string][]byte, clock *pbsubstreams.Clock) (err error) {
	var vm *wasm.Instance
	if vm, err = e.wasmCall(ctx, vals, clock); err != nil {
		return err
	}

	name := e.moduleName
	if vm != nil {
		out := vm.Output()
		vals[name] = out
		e.mapperOutput = out

	} else {
		// This means wasm execution was skipped because all inputs were empty.
		vals[name] = nil
		e.mapperOutput = nil
	}
	return nil
}

func (e *StoreModuleExecutor) wasmStoreCall(ctx context.Context, vals map[string][]byte, clock *pbsubstreams.Clock) (err error) {
	if _, err := e.wasmCall(ctx, vals, clock); err != nil {
		return err
	}

	return nil
}

func (e *BaseExecutor) wasmCall(ctx context.Context, vals map[string][]byte, clock *pbsubstreams.Clock) (instance *wasm.Instance, err error) {
	hasInput := false
	for _, input := range e.wasmInputs {
		switch input.Type {
		case wasm.InputSource:
			val := vals[input.Name]
			if len(val) != 0 {
				input.StreamData = val
				hasInput = true
			} else {
				input.StreamData = nil
			}
		case wasm.InputStore:
			hasInput = true
		case wasm.OutputStore:

		default:
			panic(fmt.Sprintf("Invalid input type %d", input.Type))
		}
	}

	// This allows us to skip the execution of the VM if there are no inputs.
	// This assumption should either be configurable by the manifest, or clearly documented:
	//  state builders will not be called if their input streams are 0 bytes length (and there'e no
	//  state store in read mode)
	if hasInput {
		instance, err = e.wasmModule.NewInstance(clock, e.wasmInputs)
		if err != nil {
			return nil, fmt.Errorf("new wasm instance: %w", err)
		}

		if err = instance.Execute(); err != nil {
			errExecutor := ErrorExecutor{
				message:    err.Error(),
				stackTrace: instance.ExecutionStack,
			}
			return nil, fmt.Errorf("block %d: module %q: wasm execution failed: %v", clock.Number, e.moduleName, errExecutor.Error())
		}
		err = instance.Module.Heap.Clear()
		if err != nil {
			return nil, fmt.Errorf("block %d: module %q: wasm heap clear failed: %w", clock.Number, e.moduleName, err)
		}
	}
	return
}

func (e *StoreModuleExecutor) moduleLogs() (logs []string, truncated bool) {
	if instance := e.wasmModule.CurrentInstance; instance != nil {
		return instance.Logs, instance.ReachedLogsMaxByteCount()
	}
	return
}

func (e *StoreModuleExecutor) moduleOutputData() pbsubstreams.ModuleOutputData {
	if len(e.outputStore.Deltas) != 0 {
		return &pbsubstreams.ModuleOutput_StoreDeltas{
			StoreDeltas: &pbsubstreams.StoreDeltas{Deltas: e.outputStore.Deltas},
		}
	}
	return nil
}

func (e *StoreModuleExecutor) getCurrentExecutionStack() []string {
	return e.wasmModule.CurrentInstance.ExecutionStack
}

// func (e *StoreModuleExecutor) appendOutput(moduleOutputs []*pbsubstreams.ModuleOutput) []*pbsubstreams.ModuleOutput {
// 	if !e.isOutput {
// 		return moduleOutputs
// 	}

// 	var logs []string
// 	logsTruncated := false
// 	if instance := e.wasmModule.CurrentInstance; instance != nil {
// 		logs = instance.Logs
// 		logsTruncated = instance.ReachedLogsMaxByteCount()
// 	}

// 	if len(e.outputStore.Deltas) != 0 || len(logs) != 0 {
// 		zlog.Debug("append to output, store")
// 		moduleOutputs = append(moduleOutputs, &pbsubstreams.ModuleOutput{
// 			Name: e.moduleName,
// 			Data: &pbsubstreams.ModuleOutput_StoreDeltas{
// 				StoreDeltas: &pbsubstreams.StoreDeltas{Deltas: e.outputStore.Deltas},
// 			},
// 			Logs:            logs,
// 			IsLogsTruncated: logsTruncated,
// 		})
// 	}

// 	return moduleOutputs
// }

func (e *StoreModuleExecutor) Reset() { e.wasmModule.CurrentInstance = nil }

func (e *MapperModuleExecutor) Reset() { e.wasmModule.CurrentInstance = nil }

func (e *MapperModuleExecutor) moduleLogs() (logs []string, truncated bool) {
	if instance := e.wasmModule.CurrentInstance; instance != nil {
		return instance.Logs, instance.ReachedLogsMaxByteCount()
	}
	return
}

func (e *MapperModuleExecutor) moduleOutputData() pbsubstreams.ModuleOutputData {
	if e.mapperOutput != nil {
		return &pbsubstreams.ModuleOutput_MapOutput{
			MapOutput: &anypb.Any{TypeUrl: "type.googleapis.com/" + e.outputType, Value: e.mapperOutput},
		}
	}
	return nil
}

func (e *MapperModuleExecutor) getCurrentExecutionStack() []string {
	return e.wasmModule.CurrentInstance.ExecutionStack
}

// func (e *MapperModuleExecutor) appendOutput(moduleOutputs []*pbsubstreams.ModuleOutput) []*pbsubstreams.ModuleOutput {
// 	if !e.isOutput {
// 		return moduleOutputs
// 	}

// 	var logs []string
// 	logsTruncated := false
// 	if instance := e.wasmModule.CurrentInstance; instance != nil {
// 		logs = instance.Logs
// 		logsTruncated = instance.ReachedLogsMaxByteCount()
// 	}

// 	if e.mapperOutput != nil || len(logs) != 0 {
// 		zlog.Debug("append to output, map")
// 		moduleOutputs = append(moduleOutputs, &pbsubstreams.ModuleOutput{
// 			Name: e.moduleName,
// 			Data: &pbsubstreams.ModuleOutput_MapOutput{
// 				MapOutput: &anypb.Any{TypeUrl: "type.googleapis.com/" + e.outputType, Value: e.mapperOutput},
// 			},
// 			Logs:            logs,
// 			IsLogsTruncated: logsTruncated,
// 		})
// 	}

// 	return moduleOutputs
// }

func OptimizeExecutors(moduleOutputCache map[string]*outputs.OutputCache, moduleExecutors []ModuleExecutor, requestedOutputStores []string) (optimizedModuleExecutors []ModuleExecutor, skipBlockSource bool) {

	return nil, false
}
