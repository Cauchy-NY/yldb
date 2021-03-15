package sstable

import (
	"bytes"
	"encoding/binary"

	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/utils"
)

//---------------------------------block----------------------------------------

type block struct {
	entrys []ikey.Entry
}

func newBlock(buf []byte) *block {
	var block block
	data := bytes.NewBuffer(buf)
	counter := binary.LittleEndian.Uint32(buf[len(buf)-4:])

	for i := uint32(0); i < counter; i++ {
		var e ikey.Entry
		if err := e.DecodeFrom(data); err != nil {
			return nil
		}
		block.entrys = append(block.entrys, e)
	}

	return &block
}

func (b *block) iterator() *BlockIterator {
	return &BlockIterator{
		block: b,
		index: 0,
		cmp:   utils.NewDefaultComparator(),
	}
}

//------------------------------BlockHandle--------------------------------------

type BlockHandle struct {
	Offset uint32
	Size   uint32
}

func (handle *BlockHandle) encodeHandleToBytes() []byte {
	p := make([]byte, 8)
	binary.LittleEndian.PutUint32(p, handle.Offset)
	binary.LittleEndian.PutUint32(p[4:], handle.Size)
	return p
}

func (handle *BlockHandle) DecodeFromBytes(p []byte) {
	if len(p) == 8 {
		handle.Offset = binary.LittleEndian.Uint32(p)
		handle.Size = binary.LittleEndian.Uint32(p[4:])
	}
}

//-----------------------------indexBlockHandle-----------------------------------

type indexBlockHandle struct {
	lastKey []byte
	handle  BlockHandle
}

func (index *indexBlockHandle) SetBlockHandle(blockHandle BlockHandle) {
	index.lastKey = blockHandle.encodeHandleToBytes()
}

func (index *indexBlockHandle) GetBlockHandle(buf []byte) BlockHandle {
	var blockHandle BlockHandle
	blockHandle.DecodeFromBytes(buf)
	return blockHandle
}
