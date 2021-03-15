package yldb

import "github.com/Cauchy-NY/yldb/ikey"

type DB interface {
	Get(key []byte) (value []byte, err error)

	Set(key, value []byte) error

	Delete(key []byte) error

	Find(key []byte) Iterator

	Close() error
}

type Iterator interface {
	// 返回迭代器所在节点是否合法
	Valid() bool

	// 当前节点合法时，返回当前节点的InternalKey
	InternalKey() ikey.InternalKey

	// 当前节点合法时，返回当前节点的UserKey
	UserKey() []byte

	// 当前节点合法时，返回当前节点的Value
	Value() []byte

	// 迭代器前进到下一个节点
	Next()

	// 迭代器前进到上一个节点
	Prev()

	// 迭代器定位到第一个key>=target的节点
	Seek(target []byte)

	// 迭代器定位到第一个节点
	SeekToFirst()

	// 迭代器定位到最后一个节点
	SeekToLast()
}
