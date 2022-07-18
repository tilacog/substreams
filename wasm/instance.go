package wasm

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/tetratelabs/wazero/sys"

	"github.com/tetratelabs/wazero/api"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"github.com/streamingfast/substreams/state"
)

type Instance struct {
	//memory       *wasmer.Memory
	heap *Heap
	//store        *wasmer.Store
	inputStores  []state.Reader
	outputStore  *state.Store
	updatePolicy pbsubstreams.Module_KindStore_UpdatePolicy

	valueType  string
	entrypoint api.Function

	clock *pbsubstreams.Clock

	args         []uint64 // to the `entrypoint` function
	returnValue  []byte
	panicError   *PanicError
	functionName string
	moduleName   string

	Logs          []string
	LogsByteCount uint64
}

func (i *Instance) Heap() *Heap {
	return i.heap
}

func (i *Instance) Execute(ctx context.Context) (err error) {
	if _, err = i.entrypoint.Call(ctx, i.args...); err != nil {
		if extern, ok := err.(*sys.ExitError); ok {
			if extern.ExitCode() == 0 {
				return nil
			}
		}
		if i.panicError != nil {
			fmt.Println("Panic error:", i.panicError)
			return i.panicError
		}
		return fmt.Errorf("executing entrypoint %q: %w", i.functionName, err)
	}

	return nil
}

func (i *Instance) ExecuteWithArgs(ctx context.Context, args ...uint64) (err error) {
	if _, err = i.entrypoint.Call(ctx, args...); err != nil {
		if extern, ok := err.(*sys.ExitError); ok {
			if extern.ExitCode() == 0 {
				return nil
			}
		}

		if i.panicError != nil {
			return i.panicError
		}
		return fmt.Errorf("executing with args entrypoint %q: %w", i.functionName, err)
	}
	return nil
}

func (i *Instance) WriteOutputToHeap(ctx context.Context, memory api.Memory, outputPtr uint32, value []byte) error {

	valuePtr, err := i.heap.Write(ctx, memory, value)
	if err != nil {
		return fmt.Errorf("writting value to heap: %w", err)
	}
	returnValue := make([]byte, 8)
	binary.LittleEndian.PutUint32(returnValue[0:4], valuePtr)
	binary.LittleEndian.PutUint32(returnValue[4:], uint32(len(value)))

	_, err = i.heap.WriteAtPtr(ctx, memory, returnValue, outputPtr)
	if err != nil {
		return fmt.Errorf("writing response at valuePtr %d: %w", valuePtr, err)
	}

	return nil
}

func (i *Instance) Err() error {
	return i.panicError
}

func (i *Instance) Output() []byte {
	return i.returnValue
}

func (i *Instance) SetOutputStore(store *state.Store) {
	i.outputStore = store
}

const maxLogByteCount = 128 * 1024 // 128 KiB

func (i *Instance) ReachedLogsMaxByteCount() bool {
	return i.LogsByteCount >= maxLogByteCount
}
