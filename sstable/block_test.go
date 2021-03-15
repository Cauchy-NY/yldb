package sstable

import (
	"testing"

	"github.com/Cauchy-NY/yldb/ikey"
)

func TestBlock(t *testing.T) {
	var builder BlockBuilder

	internalKey := ikey.MakeInternalKey(nil, []byte("apple"), ikey.InternalKeyKindSet, 1)
	builder.add(internalKey, []byte("red"))
	internalKey = ikey.MakeInternalKey(nil, []byte("peach"), ikey.InternalKeyKindSet, 2)
	builder.add(internalKey, []byte("yellow"))
	internalKey = ikey.MakeInternalKey(nil, []byte("plum"), ikey.InternalKeyKindSet, 3)
	builder.add(internalKey, []byte("purple"))
	p := builder.finish()

	block := newBlock(p)
	it := block.iterator()

	it.Seek([]byte("apple"))
	if it.Valid() {
		if string(it.Value()) != "red" {
			t.Fail()
		}
	} else {
		t.Fail()
	}
}
