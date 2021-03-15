package sstable

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/Cauchy-NY/yldb/ikey"
)

var (
	dbName   = "../test_data/test_sstable"
	fileName = dbName + "/" + "000123.ldb"

	smallestNum = 99
	largestNum  = 999
)

func setup() {
	// 先往磁盘写数据
	_ = os.MkdirAll(dbName, 0755)
	builder, err := NewTableBuilder(fileName)
	if err != nil {
		fmt.Println("Err:", err)
	}

	for i := smallestNum; i <= largestNum; i++ {
		numKey := strconv.Itoa(i)
		numVal := strconv.Itoa(i + 1)
		internalKey := ikey.MakeInternalKey(nil, []byte(numKey), ikey.InternalKeyKindSet, uint64(i))
		builder.Add(internalKey, []byte(numVal))
	}
	builder.Finish()
}

func TestSSTable(t *testing.T) {
	setup()

	var table *SSTable
	var err error
	if table, err = Open(fileName); err != nil {
		fmt.Println(err)
	}
	fmt.Println(table.footer.IndexHandle.Offset)
	fmt.Println(table.footer.IndexHandle.Size)

	it := table.Iterator()
	it.Seek([]byte("666"))
	if it.Valid() {
		if string(it.Value()) != "667" {
			t.Fail()
		}
	} else {
		t.Fail()
	}
}
