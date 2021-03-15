package sstable

import (
	"os"

	"github.com/Cauchy-NY/yldb/config"
)

type TableBuilder struct {
	file               *os.File
	offset             uint32
	numEntries         int32
	dataBlockBuilder   BlockBuilder
	indexBlockBuilder  BlockBuilder
	pendingIndexEntry  bool
	pendingIndexHandle indexBlockHandle
	errs               []error
}

func NewTableBuilder(fileName string) (*TableBuilder, error) {
	var builder TableBuilder
	var err error
	builder.file, err = os.Create(fileName)
	if err != nil {
		return nil, err
	}
	builder.pendingIndexEntry = false
	return &builder, nil
}

func (builder *TableBuilder) FileSize() uint32 {
	return builder.offset
}

func (builder *TableBuilder) Add(key, val []byte) {
	if len(builder.errs) != 0 {
		return
	}

	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.add(
			builder.pendingIndexHandle.lastKey,
			builder.pendingIndexHandle.handle.encodeHandleToBytes(),
		)
		builder.pendingIndexEntry = false
	}

	builder.pendingIndexHandle.lastKey = key

	builder.numEntries++
	builder.dataBlockBuilder.add(key, val)
	if builder.dataBlockBuilder.currentSizeEstimate() > config.MaxBlockSize {
		builder.flush()
	}
}

func (builder *TableBuilder) Finish() {
	// 必要的话处理最后的dataBlock
	builder.flush()
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.add(
			builder.pendingIndexHandle.lastKey,
			builder.pendingIndexHandle.handle.encodeHandleToBytes(),
		)
		builder.pendingIndexEntry = false
	}

	var footer Footer
	footer.IndexHandle = builder.writeBlock(&builder.indexBlockBuilder)
	_ = footer.encodeTo(builder.file)

	if err := builder.file.Close(); err != nil {
		builder.errs = append(builder.errs, err)
	}
}

func (builder *TableBuilder) flush() {
	if builder.dataBlockBuilder.isEmpty() {
		return
	}
	// 深拷贝
	oldKey := builder.pendingIndexHandle.lastKey
	newKey := make([]byte, len(oldKey))
	copy(newKey, oldKey)
	builder.pendingIndexHandle.lastKey = newKey

	builder.pendingIndexHandle.handle = builder.writeBlock(&builder.dataBlockBuilder)

	builder.pendingIndexEntry = true
}

func (builder *TableBuilder) writeBlock(blockBuilder *BlockBuilder) BlockHandle {
	content := blockBuilder.finish()
	blockHandle := BlockHandle{
		Offset: builder.offset,
		Size:   uint32(len(content)),
	}
	builder.offset += uint32(len(content))

	if _, err := builder.file.Write(content); err != nil {
		builder.errs = append(builder.errs, err)
	}
	if err := builder.file.Sync(); err != nil {
		builder.errs = append(builder.errs, err)
	}

	blockBuilder.Reset()
	return blockHandle
}

func (builder *TableBuilder) fileSize() uint32 {
	return builder.offset
}
