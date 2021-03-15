package memdb

import (
	"sync"

	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/utils"
)

type MemTable struct {
	list     *SkipList
	mutex    sync.RWMutex
	memUsage uint64
}

func NewMemTable(cmp utils.Comparator) *MemTable {
	return &MemTable{
		list:     newSkipList(cmp),
		mutex:    sync.RWMutex{},
		memUsage: 0,
	}
}

func (mem MemTable) Get(key []byte) (value []byte, err error) {
	mem.mutex.RLock()
	defer mem.mutex.RUnlock()

	return mem.list.Get(key)
}

func (mem MemTable) Set(key, value []byte) error {
	mem.mutex.Lock()
	defer mem.mutex.Unlock()

	err := mem.list.Set(key, value)
	if err == nil {
		mem.memUsage += uint64(len(key) + len(value))
	}
	return err
}

func (mem *MemTable) Contains(key []byte) bool {
	mem.mutex.RLock()
	defer mem.mutex.RUnlock()
	return mem.list.Contains(key)
}

func (mem *MemTable) ApproximateMemoryUsage() uint64 {
	return mem.memUsage
}

func (mem *MemTable) Iterator() *MemIterator {
	return &MemIterator{
		mem:  mem,
		node: mem.list.head.next(),
	}
}

type MemIterator struct {
	mem  *MemTable
	node *node
}

func (it *MemIterator) Valid() bool {
	return it.node != nil
}

func (it *MemIterator) InternalKey() ikey.InternalKey {
	return it.node.key
}

func (it *MemIterator) UserKey() []byte {
	return ikey.InternalKey(it.node.key).UserKey()
}

func (it *MemIterator) Value() []byte {
	return it.node.val
}

func (it *MemIterator) Next() {
	it.mem.mutex.RLock()
	defer it.mem.mutex.RUnlock()

	it.node = it.node.next()
}

func (it *MemIterator) Prev() {
	it.mem.mutex.RLock()
	defer it.mem.mutex.RUnlock()

	it.node = it.node.prev()
}

func (it *MemIterator) Seek(target []byte) {
	it.mem.mutex.RLock()
	defer it.mem.mutex.RUnlock()

	lookUpKey := ikey.MakeLookUpKey(target)
	it.node, _ = it.mem.list.findGreaterOrEqual(lookUpKey)
}

func (it *MemIterator) SeekToFirst() {
	it.mem.mutex.RLock()
	defer it.mem.mutex.RUnlock()

	it.node = it.mem.list.head.next()
}

func (it *MemIterator) SeekToLast() {
	it.mem.mutex.RLock()
	defer it.mem.mutex.RUnlock()

	it.node = it.mem.list.getLastNode()
}
