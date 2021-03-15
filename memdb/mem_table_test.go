package memdb

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/Cauchy-NY/yldb/ikey"
)

var (
	testLen = 10
)

func setup() *MemTable {
	mem := NewMemTable(nil)
	for i := 0; i < testLen; i++ {
		key := strconv.Itoa(i)
		iKey := ikey.MakeInternalKey(nil, []byte(key), ikey.InternalKeyKindSet, uint64(i))
		val := []byte(strconv.Itoa(i + 1))
		_ = mem.Set(iKey, val)
	}
	return mem
}

func TestGet(t *testing.T) {
	mem := setup()
	val, err := mem.Get([]byte("6"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "7" {
		t.Fatal()
	}
}

func TestOrder(t *testing.T) {
	mem := setup()
	it := mem.Iterator()
	it.SeekToFirst()
	for it.Valid() {
		fmt.Println(fmt.Sprintf("key:%s, val:%s", it.InternalKey(), it.Value()))
		it.Next()
	}
}

func TestPostOrder(t *testing.T) {
	mem := setup()
	it := mem.Iterator()
	it.SeekToLast()
	for it.Valid() {
		fmt.Println(fmt.Sprintf("key:%s, val:%s", it.InternalKey(), it.Value()))
		it.Prev()
	}
}

func TestSeek(t *testing.T) {
	mem := setup()
	it := mem.Iterator()
	it.Seek([]byte("6"))
	if !it.Valid() || string(it.Value()) != "7" {
		fmt.Println(it.Value())
		t.Fatal()
	}
}
