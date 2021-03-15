package sstable

import (
	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/utils"
)

//----------------------------------TableIterator----------------------------------

type TableIterator struct {
	table           *SSTable
	dataBlockHandle BlockHandle
	dataIter        *BlockIterator
	indexIter       *BlockIterator
	cmp             utils.Comparator
}

func (it *TableIterator) Valid() bool {
	return it.dataIter != nil && it.dataIter.Valid()
}

func (it *TableIterator) InternalKey() ikey.InternalKey {
	return it.dataIter.InternalKey()
}

func (it *TableIterator) UserKey() []byte {
	return it.dataIter.UserKey()
}

func (it *TableIterator) Value() []byte {
	return it.dataIter.Value()
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *TableIterator) Next() {
	it.dataIter.Next()
	it.skipEmptyDataBlocksForward()
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *TableIterator) Prev() {
	it.dataIter.Prev()
	it.skipEmptyDataBlocksBackward()
}

// Advance to the first entry with a key >= target
func (it *TableIterator) Seek(target []byte) {
	// Index Block的block_data字段中，每一条记录的key都满足：
	// 大于等于Data Block的所有key，并且小于后面所有Data Block的key
	// 因为Seek是查找key>=target的第一条记录，所以当index_iter_找到时，
	// 该index_inter_对应的data_iter_所管理的Data Block中所有记录的
	// key都小于等于target，如果需要在下一个Data Block中seek，而下一个Data Block
	// 中的第一条记录就满足key>=target

	it.indexIter.Seek(target)
	it.initDataBlock()
	if it.dataIter != nil {

		it.dataIter.Seek(target)
	}
	it.skipEmptyDataBlocksForward()
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *TableIterator) SeekToFirst() {
	it.indexIter.SeekToFirst()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToFirst()
	}
	it.skipEmptyDataBlocksForward()
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *TableIterator) SeekToLast() {
	it.indexIter.SeekToLast()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToLast()
	}
	it.skipEmptyDataBlocksBackward()
}

func (it *TableIterator) skipEmptyDataBlocksForward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}
		it.indexIter.Next()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToFirst()
		}
	}
}

func (it *TableIterator) skipEmptyDataBlocksBackward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}
		it.indexIter.Prev()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToLast()
		}
	}
}

func (it *TableIterator) initDataBlock() {
	if !it.indexIter.Valid() {
		it.dataIter = nil
	} else {
		var index indexBlockHandle
		index.lastKey = it.indexIter.InternalKey()
		tmpBlockHandle := index.GetBlockHandle(it.indexIter.Value())

		if it.dataIter != nil && it.dataBlockHandle == tmpBlockHandle {
			// 如果同一个迭代器已经被构建，什么都不需要处理
		} else {
			it.dataIter = it.table.readBlock(tmpBlockHandle).iterator()
			it.dataBlockHandle = tmpBlockHandle
		}
	}
}

//----------------------------------TableIterator----------------------------------

//----------------------------------BlockIterator----------------------------------

type BlockIterator struct {
	block *block
	index int
	cmp   utils.Comparator
}

func (it *BlockIterator) Valid() bool {
	return it.index >= 0 && it.index < len(it.block.entrys)
}

func (it *BlockIterator) InternalKey() ikey.InternalKey {
	return it.block.entrys[it.index].Ikey()
}

func (it *BlockIterator) UserKey() []byte {
	return it.block.entrys[it.index].Ikey().UserKey()
}

func (it *BlockIterator) Value() []byte {
	return it.block.entrys[it.index].Val()
}

func (it *BlockIterator) Next() {
	it.index++
}

func (it *BlockIterator) Prev() {
	it.index--
}

func (it *BlockIterator) Seek(target []byte) {
	left := 0
	right := len(it.block.entrys) - 1
	for left < right {
		mid := (left + right) / 2
		if it.cmp.Compare(it.block.entrys[mid].Ikey().UserKey(), target) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	//if left == len(it.block.entrys)-1 {
	//	if it.cmp.Compare(it.block.entrys[left].Ikey().UserKey(), target) < 0 {
	//		// not found
	//		left++
	//	}
	//}
	it.index = right
}

func (it *BlockIterator) SeekToFirst() {
	it.index = 0
}

func (it *BlockIterator) SeekToLast() {
	if len(it.block.entrys) > 0 {
		it.index = len(it.block.entrys) - 1
	}
}

//----------------------------------BlockIterator----------------------------------
