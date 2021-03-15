package ikey

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/Cauchy-NY/yldb/errors"
	"github.com/Cauchy-NY/yldb/utils"
)

const (
	InternalKeyKindDelete InternalKeyKind = 0
	InternalKeyKindSet    InternalKeyKind = 1

	InternalKeyKindMax InternalKeyKind = 1

	//InternalKeySeqNumMax = uint64(1<<56 - 1)
)

type InternalKey []byte

type InternalKeyKind uint8

// 根据user_key、操作类型、序列号生成internal_key，格式为
// user_key(n bytes) | kind(1 byte) | seqNum<<8(7 bytes)
// 这样设计的目的是同一个key的一系列操作会按seq顺序排列
// 若dst足够大，则返回dst[:cap(dst)]，否则将返回一个新声明的足够大的buffer
func MakeInternalKey(dst InternalKey, userKey []byte, kind InternalKeyKind, seqNum uint64) InternalKey {
	if cap(dst) < len(userKey)+8 {
		n := 256
		for n < len(userKey)+8 {
			n *= 2
		}
		dst = make(InternalKey, n)
	}
	ikey := dst[:len(userKey)+8]
	i := copy(ikey, userKey)
	ikey[i+0] = uint8(kind)
	ikey[i+1] = uint8(seqNum)
	ikey[i+2] = uint8(seqNum >> 8)
	ikey[i+3] = uint8(seqNum >> 16)
	ikey[i+4] = uint8(seqNum >> 24)
	ikey[i+5] = uint8(seqNum >> 32)
	ikey[i+6] = uint8(seqNum >> 40)
	ikey[i+7] = uint8(seqNum >> 48)
	return ikey
}

func MakeLookUpKey(key []byte) InternalKey {
	return MakeInternalKey(nil, key, InternalKeyKindSet, math.MaxUint64)
}

// 判断internal_key是否合法
func (key InternalKey) Valid() bool {
	i := len(key) - 8
	return i >= 0 && InternalKeyKind(key[i]) <= InternalKeyKindMax
}

// 返回user_key，如果internal_key不合法可能会panic
func (key InternalKey) UserKey() []byte {
	return []byte(key[:len(key)-8])
}

// 返回internal_key的操作类型，如果internal_key不合法可能会panic
func (key InternalKey) Kind() InternalKeyKind {
	return InternalKeyKind(key[len(key)-8])
}

// 返回internal_key的序列号，如果internal_key不合法可能会panic
func (key InternalKey) SeqNum() uint64 {
	i := len(key) - 7
	n := uint64(key[i+0])
	n |= uint64(key[i+1]) << 8
	n |= uint64(key[i+2]) << 16
	n |= uint64(key[i+3]) << 24
	n |= uint64(key[i+4]) << 32
	n |= uint64(key[i+5]) << 40
	n |= uint64(key[i+6]) << 48
	return n
}

type InternalKeyComparator struct {
	userCmp utils.Comparator
}

func NewInternalKeyComparator(userCmp utils.Comparator) InternalKeyComparator {
	cmp := InternalKeyComparator{
		userCmp: userCmp,
	}
	if userCmp == nil {
		cmp.userCmp = utils.NewDefaultComparator()
	}
	return cmp
}

func (i InternalKeyComparator) Compare(a, b []byte) int {
	ak, bk := InternalKey(a), InternalKey(b)
	if !ak.Valid() {
		if bk.Valid() {
			return -1
		}
		return bytes.Compare(a, b)
	}
	if !bk.Valid() {
		return 1
	}
	if x := i.userCmp.Compare(ak.UserKey(), bk.UserKey()); x != 0 {
		return x
	}
	if an, bn := ak.SeqNum(), bk.SeqNum(); an < bn {
		return +1
	} else if an > bn {
		return -1
	}
	if ai, bi := ak.Kind(), bk.Kind(); ai < bi {
		return +1
	} else if ai > bi {
		return -1
	}
	return 0
}

func (i InternalKeyComparator) Name() string {
	return "yldb.InternalKeyComparator"
}

type Entry struct {
	ikey []byte
	val  []byte
}

func NewEntry(key, val []byte) *Entry {
	return &Entry{
		ikey: key,
		val:  val,
	}
}

func (e *Entry) Ikey() InternalKey {
	return e.ikey
}

func (e *Entry) Val() []byte {
	return e.val
}

func (e *Entry) EncodeTo(w io.Writer) error {
	var errs []error
	errs = append(errs, binary.Write(w, binary.LittleEndian, int32(len(e.ikey))))
	errs = append(errs, binary.Write(w, binary.LittleEndian, e.ikey))
	errs = append(errs, binary.Write(w, binary.LittleEndian, int32(len(e.val))))
	errs = append(errs, binary.Write(w, binary.LittleEndian, e.val))

	for _, err := range errs {
		if err != nil {
			return errors.ErrEntryEncodeError
		}
	}
	return nil
}

func (e *Entry) DecodeFrom(r io.Reader) error {
	var errs []error
	var length int32

	errs = append(errs, binary.Read(r, binary.LittleEndian, &length))
	e.ikey = make([]byte, length)
	errs = append(errs, binary.Read(r, binary.LittleEndian, &e.ikey))

	errs = append(errs, binary.Read(r, binary.LittleEndian, &length))
	e.val = make([]byte, length)
	errs = append(errs, binary.Read(r, binary.LittleEndian, &e.val))

	for _, err := range errs {
		if err != nil {
			return errors.ErrEntryDecodeError
		}
	}
	return nil
}
