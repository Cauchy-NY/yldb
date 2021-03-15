package sstable

import (
	"bytes"
	"encoding/binary"

	"github.com/Cauchy-NY/yldb/ikey"
)

type BlockBuilder struct {
	buf     bytes.Buffer
	counter uint32
}

func (builder *BlockBuilder) Reset() {
	builder.counter = 0
	builder.buf.Reset()
}

func (builder *BlockBuilder) add(key, val []byte) {
	builder.counter++
	entry := ikey.NewEntry(key, val)
	_ = entry.EncodeTo(&builder.buf)
}

func (builder *BlockBuilder) finish() []byte {
	_ = binary.Write(&builder.buf, binary.LittleEndian, builder.counter)
	return builder.buf.Bytes()
}

func (builder *BlockBuilder) currentSizeEstimate() int {
	return builder.buf.Len()
}

func (builder *BlockBuilder) isEmpty() bool {
	return builder.buf.Len() == 0
}
