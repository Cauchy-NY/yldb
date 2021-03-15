package sstable

import (
	"io"
	"os"

	"github.com/Cauchy-NY/yldb/errors"
	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/utils"
)

type SSTable struct {
	index  *block
	footer Footer
	file   *os.File
}

func Open(fileName string) (*SSTable, error) {
	var table SSTable
	var err error
	if table.file, err = os.Open(fileName); err != nil {
		return nil, err
	}

	stat, _ := table.file.Stat()
	footerSize := int64(table.footer.size())
	if stat.Size() < footerSize {
		return nil, errors.ErrSSTableFileTooShort
	}

	if _, err = table.file.Seek(-footerSize, io.SeekEnd); err != nil {
		return nil, err
	}
	if err = table.footer.decodeFrom(table.file); err != nil {
		return nil, err
	}
	// Read the index block
	table.index = table.readBlock(table.footer.IndexHandle)
	return &table, nil
}

func (table *SSTable) Get(key []byte) ([]byte, error) {
	it := table.Iterator()
	it.Seek(key)
	if it.Valid() {
		internalKey := it.InternalKey()
		if it.cmp.Compare(key, internalKey.UserKey()) == 0 {
			// 判断valueType
			if internalKey.Kind() == ikey.InternalKeyKindSet {
				return it.Value(), nil
			} else {
				return nil, errors.ErrSSTableDeletion
			}
		}
	}
	return nil, errors.ErrSSTableNotFound
}

func (table *SSTable) Iterator() *TableIterator {
	return &TableIterator{
		table:     table,
		indexIter: table.index.iterator(),
		cmp:       utils.NewDefaultComparator(),
	}
}

func (table *SSTable) readBlock(blockHandle BlockHandle) *block {
	buf := make([]byte, blockHandle.Size)
	n, err := table.file.ReadAt(buf, int64(blockHandle.Offset))
	if err != nil || uint32(n) != blockHandle.Size {
		return nil
	}
	return newBlock(buf)
}
