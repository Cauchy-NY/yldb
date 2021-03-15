package version

import (
	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/sstable"
	"github.com/Cauchy-NY/yldb/utils"
)

type MergeIterator struct {
	cmp     utils.Comparator
	list    []*sstable.TableIterator
	current *sstable.TableIterator
}

func (it *MergeIterator) findSmallest() {
	var smallest *sstable.TableIterator = nil
	for i := 0; i < len(it.list); i++ {
		if it.list[i].Valid() {
			if smallest == nil {
				smallest = it.list[i]
			} else if it.cmp.Compare(smallest.UserKey(), it.list[i].UserKey()) > 0 {
				smallest = it.list[i]
			}
		}
	}
	it.current = smallest
}

func (it *MergeIterator) Valid() bool {
	return it.current != nil && it.current.Valid()
}

func (it *MergeIterator) InternalKey() ikey.InternalKey {
	return it.current.InternalKey()
}

func (it *MergeIterator) UserKey() []byte {
	return it.current.InternalKey().UserKey()
}

func (it *MergeIterator) Value() []byte {
	return it.current.Value()
}

func (it *MergeIterator) Next() {
	if it.current != nil {
		it.current.Next()
	}
	it.findSmallest()
}

func (it *MergeIterator) Prev() {
	panic("implement me")
}

func (it *MergeIterator) Seek(target []byte) {
	panic("implement me")
}

func (it *MergeIterator) SeekToFirst() {
	for i := 0; i < len(it.list); i++ {
		it.list[i].SeekToFirst()
	}
	it.findSmallest()
}

func (it *MergeIterator) SeekToLast() {
	panic("implement me")
}
