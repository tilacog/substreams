package block

import (
	"fmt"
	"strings"

	"github.com/streamingfast/bstream"
	"go.uber.org/zap/zapcore"
)

type Range struct {
	StartBlock        uint64
	ExclusiveEndBlock uint64
}

func (r *Range) String() string {
	return fmt.Sprintf("[%d, %d)", r.StartBlock, r.ExclusiveEndBlock)
}

func (r *Range) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddUint64("start_block", r.StartBlock)
	enc.AddUint64("end_block", r.ExclusiveEndBlock)

	return nil
}

func (r *Range) Contains(blockRef bstream.BlockRef) bool {
	return blockRef.Num() >= r.StartBlock && blockRef.Num() < r.ExclusiveEndBlock
}

func (r *Range) Next(size uint64) *Range {
	return &Range{
		StartBlock:        r.ExclusiveEndBlock,
		ExclusiveEndBlock: r.ExclusiveEndBlock + size,
	}
}

func (r *Range) Previous(size uint64) *Range {
	return &Range{
		StartBlock:        r.StartBlock - size,
		ExclusiveEndBlock: r.StartBlock,
	}
}

func (r *Range) Equals(other *Range) bool {
	return r.StartBlock == other.StartBlock && r.ExclusiveEndBlock == other.ExclusiveEndBlock
}

func (r *Range) Size() uint64 {
	return r.ExclusiveEndBlock - r.StartBlock
}

func (r *Range) Split(chunkSize uint64) []*Range {
	var res []*Range
	if r.ExclusiveEndBlock-r.StartBlock <= chunkSize {
		res = append(res, r)
		return res
	}

	currentStart := r.StartBlock
	currentEnd := r.StartBlock + chunkSize

	for {
		res = append(res, &Range{
			StartBlock:        currentStart,
			ExclusiveEndBlock: currentEnd,
		})

		if currentEnd >= r.ExclusiveEndBlock {
			break
		}

		currentStart = currentEnd
		currentEnd = currentStart + chunkSize
		if currentEnd > r.ExclusiveEndBlock {
			currentEnd = r.ExclusiveEndBlock
		}
	}

	return res
}

type Ranges []*Range

func (r Ranges) String() string {
	var rs []string
	for _, i := range r {
		rs = append(rs, i.String())
	}
	return strings.Join(rs, ",")
}

func (r Ranges) Len() int {
	return len(r)
}

func (r Ranges) Less(i, j int) bool {
	return r[i].StartBlock < r[j].StartBlock
}

func (r Ranges) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
