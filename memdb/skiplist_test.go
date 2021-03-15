package memdb

import (
	"fmt"
	"testing"

	"github.com/Cauchy-NY/yldb/config"
	"github.com/Cauchy-NY/yldb/errors"
	"github.com/Cauchy-NY/yldb/ikey"
)

func TestRandomLevel(t *testing.T) {
	// 测试生成的level是否在给定范围内
	s := newSkipList(nil)
	retryTimes := 100
	for i := 0; i < retryTimes; i++ {
		randomLevel := s.randomLevel()
		if randomLevel <= 0 || randomLevel > config.SkipListMaxLevel {
			t.Fatalf("%v.RandomLevel: got %v， out of range", i, randomLevel)
		}
	}
}

func TestBasic(t *testing.T) {
	s := newSkipList(nil)

	// 1.测试空表
	val, err := s.Get([]byte("cherry"))
	if string(val) != "" || err != errors.ErrMemTableNotFound {
		t.Fatalf("1.get: got (%q, %v), want (%q, %v)", val, err, "", errors.ErrMemTableNotFound)
	}
	if got, want := s.length, 0; got != want {
		t.Fatalf("2.length: got %v, want %v", got, want)
	}

	// 2.测试添加和获取
	iKey := ikey.MakeInternalKey(nil, []byte("cherry"), ikey.InternalKeyKindSet, 1)
	val = []byte("red")
	_ = s.Set(iKey, val)

	iKey = ikey.MakeInternalKey(nil, []byte("peach"), ikey.InternalKeyKindSet, 1)
	val = []byte("yellow")
	_ = s.Set(iKey, val)

	iKey = ikey.MakeInternalKey(nil, []byte("peach"), ikey.InternalKeyKindSet, 1)
	val = []byte("yellow")
	_ = s.Set(iKey, val)

	iKey = ikey.MakeInternalKey(nil, []byte("grape"), ikey.InternalKeyKindSet, 1)
	val = []byte("red")
	_ = s.Set(iKey, val)

	iKey = ikey.MakeInternalKey(nil, []byte("plum"), ikey.InternalKeyKindSet, 1)
	val = []byte("purple")
	_ = s.Set(iKey, val)

	val, err = s.Get([]byte("cherry"))
	if string(val) != "red" || err != nil {
		t.Fatalf("3.get: got (%q, %v), want (%q, %v)", val, err, "red", error(nil))
	}
	val, err = s.Get([]byte("peach"))
	if string(val) != "yellow" || err != nil {
		t.Fatalf("4.get: got (%q, %v), want (%q, %v)", val, err, "yellow", error(nil))
	}
	val, err = s.Get([]byte("grape"))
	if string(val) != "red" || err != nil {
		t.Fatalf("5.get: got (%q, %v), want (%q, %v)", val, err, "red", error(nil))
	}
	val, err = s.Get([]byte("plum"))
	if string(val) != "purple" || err != nil {
		t.Fatalf("6.get: got (%q, %v), want (%q, %v)", val, err, "purple", error(nil))
	}
	if got, want := s.length, 4; got != want {
		t.Fatalf("7.length: got %v, want %v", got, want)
	}

	// 3.测试修改
	iKey = ikey.MakeInternalKey(nil, []byte("grape"), ikey.InternalKeyKindSet, 1)
	val = []byte("purple")
	_ = s.Set(iKey, val)
	val, err = s.Get([]byte("grape"))
	if string(val) != "purple" || err != nil {
		t.Fatalf("8.get: got (%q, %v), want (%q, %v)", val, err, "purple", error(nil))
	}
	if got, want := s.length, 4; got != want {
		t.Fatalf("9.length: got %v, want %v", got, want)
	}

	// 4.测试删除
	if succ, err := s.Delete([]byte("grape")); succ != true || err != nil {
		t.Fatalf("10.delete: succ %v, err %v", succ, err)
	}
	if succ, err := s.Delete([]byte("grape")); succ == true || err != errors.ErrMemTableNotFound {
		t.Fatalf("11.delete: succ %v, err %v", succ, err)
	}
	if got, want := s.length, 3; got != want {
		t.Fatalf("12.length: got %v, want %v", got, want)
	}

	// 5.测试删除后添加
	iKey = ikey.MakeInternalKey(nil, []byte("grape"), ikey.InternalKeyKindSet, 1)
	val = []byte("purple")
	_ = s.Set(iKey, val)

	val, err = s.Get([]byte("grape"))
	if string(val) != "purple" || err != nil {
		t.Fatalf("13.get: got (%q, %v), want (%q, %v)", val, err, "purple", error(nil))
	}
	if got, want := s.length, 4; got != want {
		t.Fatalf("14.length: got %v, want %v", got, want)
	}

	// 6.测试获取不在跳表中的键
	val, err = s.Get([]byte("apple"))
	if string(val) != "" || err != errors.ErrMemTableNotFound {
		t.Fatalf("15.get: got (%q, %v), want (%q, %v)", val, err, "red", errors.ErrMemTableNotFound)
	}

	// 7.测试contains
	exist := s.Contains([]byte("grape"))
	if !exist {
		t.Fatalf("16.get: got %v, want %v", exist, true)
	}

	// 8.测试表内顺序
	for cur := s.head.forwards[0]; cur != nil; cur = cur.forwards[0] {
		fmt.Printf("key: %v, val: %v\n", string(cur.key), string(cur.val))
	}
}
