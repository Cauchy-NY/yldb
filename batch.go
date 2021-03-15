package yldb

import (
	"encoding/binary"

	"github.com/Cauchy-NY/yldb/ikey"
)

const (
	batchHeaderLen    = 12
	invalidBatchCount = 1<<32 - 1
)

// Batch 是一系列Set和Get的集合
type Batch struct {
	// Batch头部：
	// - 首8字节：小端模式的操作序列号
	// - 次4字节：小端模式的操作数量
	// Batch内容：
	// - 1字节：操作类型 Set(1) Delete(0)
	// - k/v 长度
	// - k/v 内容
	data []byte
}

func (b *Batch) Set(key, value []byte) {
	if len(b.data) == 0 {
		b.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderLen + 1)
	}
	if b.increment() {
		b.data = append(b.data, byte(ikey.InternalKeyKindSet))
		b.appendKV(key)
		b.appendKV(value)
	}
}

func (b *Batch) Delete(key []byte) {
	if len(b.data) == 0 {
		b.init(len(key) + binary.MaxVarintLen64 + batchHeaderLen + 1)
	}
	if b.increment() {
		b.data = append(b.data, byte(ikey.InternalKeyKindDelete))
		b.appendKV(key)
	}
}

func (b *Batch) init(cap int) {
	n := 256
	for n < cap {
		n *= 2
	}
	b.data = make([]byte, batchHeaderLen, n)
}

// 返回序列号（8字节的小端模式）
func (b *Batch) seqNumData() []byte {
	return b.data[:8]
}

// 返回操作序列的数量（4字节的小端模式）
func (b *Batch) countData() []byte {
	return b.data[8:12]
}

// 统计Batch的操作序列数量
// 若数量过大溢出，则将其设置为"\xff\xff\xff\xff"，并返回false
func (b *Batch) increment() bool {
	p := b.countData()
	for i := range p {
		p[i]++
		if p[i] != 0x00 {
			return true
		}
	}
	// 当countData溢出时，返回"\xff\xff\xff\xff"
	p[0] = 0xff
	p[1] = 0xff
	p[2] = 0xff
	p[3] = 0xff
	return false
}

func (b *Batch) setSeqNum(seqNum uint64) {
	binary.LittleEndian.PutUint64(b.seqNumData(), seqNum)
}

func (b *Batch) seqNum() uint64 {
	return binary.LittleEndian.Uint64(b.seqNumData())
}

func (b *Batch) count() uint32 {
	return binary.LittleEndian.Uint32(b.countData())
}

func (b *Batch) appendKV(kv []byte) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(kv)))
	b.data = append(b.data, buf[:n]...)
	b.data = append(b.data, kv...)
}

func (b *Batch) iterator() BatchIterator {
	return b.data[batchHeaderLen:]
}

type BatchIterator []byte

// 返回这个batch的下一个操作
func (t *BatchIterator) next() (kind ikey.InternalKeyKind, userKey []byte, value []byte, ok bool) {
	p := *t
	if len(p) == 0 {
		return 0, nil, nil, false
	}
	kind, *t = ikey.InternalKeyKind(p[0]), p[1:]
	if kind > ikey.InternalKeyKindMax {
		return 0, nil, nil, false
	}
	userKey, ok = t.nextStr()
	if !ok {
		return 0, nil, nil, false
	}
	if kind == ikey.InternalKeyKindSet {
		value, ok = t.nextStr()
		if !ok {
			return 0, nil, nil, false
		}
	}
	return kind, userKey, value, true
}

func (t *BatchIterator) nextStr() (s []byte, ok bool) {
	p := *t
	u, numBytes := binary.Uvarint(p)
	if numBytes <= 0 {
		return nil, false
	}
	p = p[numBytes:]
	if u > uint64(len(p)) {
		return nil, false
	}
	s, *t = p[:u], p[u:]
	return s, true
}
