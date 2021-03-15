package version

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/memdb"
	"github.com/Cauchy-NY/yldb/sstable"
	"github.com/Cauchy-NY/yldb/utils"
)

var (
	dbName01 = "../test_data/test_version/01"
	dbName02 = "../test_data/test_version/02"
)

func setup() *Version {
	version := NewVersion(dbName01, nil)
	version.files[0] = append(version.files[0], createFileMetadata(106, 1, 10))
	version.files[0] = append(version.files[0], createFileMetadata(105, 5, 15))
	version.files[0] = append(version.files[0], createFileMetadata(104, 10, 20))

	version.files[1] = append(version.files[1], createFileMetadata(103, 30, 60))
	version.files[1] = append(version.files[1], createFileMetadata(102, 70, 90))

	version.files[2] = append(version.files[2], createFileMetadata(101, 150, 300))
	return version
}

func createFileMetadata(fileNum uint64, smallest, largest int) *FileMetaData {
	// 先往磁盘写数据
	_ = os.MkdirAll(dbName01, 0755)
	name := utils.TableFileName(dbName01, fileNum)
	builder, _ := sstable.NewTableBuilder(name)
	var keys []ikey.InternalKey
	cmp := utils.NewDefaultComparator()

	for i := smallest; i <= largest; i++ {
		numKey := strconv.Itoa(i)
		numVal := strconv.Itoa(i)
		internalKey := ikey.MakeInternalKey(nil, []byte(numKey), ikey.InternalKeyKindSet, uint64(i))
		keys = append(keys, internalKey)
		builder.Add(internalKey, []byte(numVal))
	}
	builder.Finish()

	sort.Slice(keys, func(i, j int) bool {
		if cmp.Compare(keys[i], keys[j]) < 0 {
			return true
		} else {
			return false
		}
	})

	return &FileMetaData{
		number:   fileNum,
		smallest: keys[0],
		largest:  keys[len(keys)-1],
	}
}

func TestVersionGet(t *testing.T) {
	version := setup() // 先写入数据

	key := []byte("13")
	if value, err := version.Get(key); string(value) != string(key) {
		t.Fatal(err)
	} else {
		fmt.Println(fmt.Sprintf("key:%s, val:%s", string(key), string(value)))
	}

	key = []byte("19")
	if value, err := version.Get(key); string(value) != string(key) {
		t.Fatal(err)
	} else {
		fmt.Println(fmt.Sprintf("key:%s, val:%s", string(key), string(value)))
	}

	key = []byte("36")
	if value, err := version.Get(key); string(value) != string(key) {
		t.Fatal(err)
	} else {
		fmt.Println(fmt.Sprintf("key:%s, val:%s", string(key), string(value)))
	}

	key = []byte("88")
	if value, err := version.Get(key); string(value) != string(key) {
		t.Fatal(err)
	} else {
		fmt.Println(fmt.Sprintf("key:%s, val:%s", string(key), string(value)))
	}

	key = []byte("166")
	if value, err := version.Get(key); string(value) != string(key) {
		t.Fatal(err)
	} else {
		fmt.Println(fmt.Sprintf("key:%s, val:%s", string(key), string(value)))
	}
}

func TestVersionSaveAndLoad(t *testing.T) {
	_ = os.MkdirAll(dbName02, 0755)
	version := NewVersion(dbName02, nil)
	memTable := memdb.NewMemTable(nil)

	key := ikey.MakeInternalKey(nil, []byte("apple"), ikey.InternalKeyKindSet, 12)
	value := []byte("red")
	_ = memTable.Set(key, value)
	key = ikey.MakeInternalKey(nil, []byte("cherry"), ikey.InternalKeyKindSet, 12)
	value = []byte("red")
	_ = memTable.Set(key, value)
	key = ikey.MakeInternalKey(nil, []byte("peach"), ikey.InternalKeyKindSet, 12)
	value = []byte("yellow")
	_ = memTable.Set(key, value)
	key = ikey.MakeInternalKey(nil, []byte("grape"), ikey.InternalKeyKindSet, 12)
	value = []byte("red")
	_ = memTable.Set(key, value)
	key = ikey.MakeInternalKey(nil, []byte("plum"), ikey.InternalKeyKindSet, 12)
	value = []byte("purple")
	_ = memTable.Set(key, value)

	_ = version.WriteLevel0Table(memTable)
	n, err := version.Save()
	if err != nil {
		t.Fatal(err)
	}
	newVersion, err := Load(dbName02, n)
	if err != nil {
		t.Fatal(err)
	}

	value, err = newVersion.Get([]byte("peach"))
	if err != nil || string(value) != "yellow" {
		fmt.Println(err, string(value))
		t.Fatal(err)
	}
}
