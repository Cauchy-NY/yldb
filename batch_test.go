package yldb

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/Cauchy-NY/yldb/ikey"
)

func TestBatch(t *testing.T) {
	testCases := []struct {
		kind  ikey.InternalKeyKind
		key   string
		value string
	}{
		{ikey.InternalKeyKindSet, "roses", "red"},
		{ikey.InternalKeyKindSet, "violets", "blue"},
		{ikey.InternalKeyKindDelete, "roses", ""},
		{ikey.InternalKeyKindSet, "", ""},
		{ikey.InternalKeyKindSet, "", "non-empty"},
		{ikey.InternalKeyKindDelete, "", ""},
		{ikey.InternalKeyKindSet, "grass", "green"},
		{ikey.InternalKeyKindSet, "grass", "greener"},
		{ikey.InternalKeyKindSet, "eleventy", strings.Repeat("!!11!", 100)},
		{ikey.InternalKeyKindDelete, "nosuchkey", ""},
		{ikey.InternalKeyKindSet, "binarydata", "\x00"},
		{ikey.InternalKeyKindSet, "binarydata", "\xff"},
	}
	var b Batch
	for _, tc := range testCases {
		if tc.kind == ikey.InternalKeyKindDelete {
			b.Delete([]byte(tc.key))
		} else {
			b.Set([]byte(tc.key), []byte(tc.value))
		}
	}
	iter := b.iterator()
	for _, tc := range testCases {
		kind, k, v, ok := iter.next()
		if !ok {
			t.Fatalf("next returned !ok: test case = %q", tc)
		}
		key, value := string(k), string(v)
		if kind != tc.kind || key != tc.key || value != tc.value {
			t.Errorf("got (%d, %q, %q), want (%d, %q, %q)",
				kind, key, value, tc.kind, tc.key, tc.value)
		}
	}
	if len(iter) != 0 {
		t.Errorf("iterator was not exhausted: remaining bytes = %q", iter)
	}
}

func TestBatchIncrement(t *testing.T) {
	testCases := []uint32{
		0x00000000,
		0x00000001,
		0x00000002,
		0x0000007f,
		0x00000080,
		0x000000fe,
		0x000000ff,
		0x00000100,
		0x00000101,
		0x000001ff,
		0x00000200,
		0x00000fff,
		0x00001234,
		0x0000fffe,
		0x0000ffff,
		0x00010000,
		0x00010001,
		0x000100fe,
		0x000100ff,
		0x00020100,
		0x03fffffe,
		0x03ffffff,
		0x04000000,
		0x04000001,
		0x7fffffff,
		0xfffffffe,
		0xffffffff,
	}
	for _, tc := range testCases {
		var buf [12]byte
		binary.LittleEndian.PutUint32(buf[8:12], tc)
		b := Batch{buf[:]}
		b.increment()
		got := binary.LittleEndian.Uint32(buf[8:12])
		want := tc + 1
		if tc == 0xffffffff {
			want = tc
		}
		if got != want {
			t.Errorf("input=%d: got %d, want %d", tc, got, want)
		}
	}
}
