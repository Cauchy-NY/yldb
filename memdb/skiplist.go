package memdb

import (
	"math/rand"
	"time"

	"github.com/Cauchy-NY/yldb/config"
	"github.com/Cauchy-NY/yldb/errors"
	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/utils"
)

type node struct {
	key      []byte
	val      []byte
	level    int
	forwards []*node
	backward *node
	isDelete bool
}

func newNode(key, val []byte, level int) *node {
	forwards := make([]*node, config.SkipListMaxLevel)
	node := node{
		key:      key,
		val:      val,
		level:    level,
		forwards: forwards,
		backward: nil,
		isDelete: false,
	}
	return &node
}

func (node *node) prev() *node {
	return node.backward
}

func (node *node) next() *node {
	return node.forwards[0]
}

type SkipList struct {
	height  int
	length  int
	cmp     utils.Comparator
	userCmp utils.Comparator
	head    *node
}

func newSkipList(cmp utils.Comparator) *SkipList {
	s := SkipList{
		height:  1,
		length:  0,
		userCmp: cmp,
		head:    newNode([]byte(""), []byte(""), config.SkipListMaxLevel),
	}
	if cmp == nil {
		s.userCmp = utils.NewDefaultComparator()
	}
	s.cmp = ikey.NewInternalKeyComparator(s.userCmp)
	return &s
}

func (s *SkipList) Get(key []byte) ([]byte, error) {
	lookUpKey := ikey.MakeLookUpKey(key)
	node, _ := s.findGreaterOrEqual(lookUpKey)
	if node == nil || node.isDelete {
		return nil, errors.ErrMemTableNotFound
	}
	if s.userCmp.Compare(ikey.InternalKey(node.key).UserKey(), key) == 0 {
		return node.val, nil
	}
	return nil, errors.ErrMemTableNotFound
}

func (s *SkipList) Set(key, value []byte) error {
	node, prevNodes := s.findGreaterOrEqual(key)

	if node == nil || s.cmp.Compare(node.key, key) != 0 { // key不在跳表中，插入新节点
		level := s.randomLevel()
		newNode := newNode(key, value, level)

		for i := level - 1; i >= 0; i-- {
			newNode.forwards[i] = prevNodes[i].forwards[i]
			prevNodes[i].forwards[i] = newNode
		}

		newNode.backward = prevNodes[0]
		if newNode.forwards[0] != nil {
			newNode.forwards[0].backward = newNode
		}

		s.length++
	} else { // key已经存在，更新val值
		if node.isDelete { // 恢复被删除的key
			node.isDelete = false
			s.length++
		}
		node.val = value
	}

	return nil
}

func (s *SkipList) Delete(key []byte) (bool, error) {
	lookUpKey := ikey.MakeLookUpKey(key)
	node, _ := s.findGreaterOrEqual(lookUpKey)

	if node != nil && s.userCmp.Compare(ikey.InternalKey(node.key).UserKey(), key) == 0 && !node.isDelete {
		node.isDelete = true
		s.length--
		return true, nil
	}
	return false, errors.ErrMemTableNotFound
}

func (s *SkipList) Contains(key []byte) bool {
	lookUpKey := ikey.MakeLookUpKey(key)
	node, _ := s.findGreaterOrEqual(lookUpKey)
	if node != nil && !node.isDelete &&
		s.userCmp.Compare(ikey.InternalKey(node.key).UserKey(), key) == 0 {
		return true
	}
	return false
}

func (s *SkipList) getLastNode() *node {
	x := s.head
	level := s.height - 1
	for true {
		next := x.forwards[level]
		if next == nil {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
	return nil
}

// 返回大于或等于key的节点，和该节点的前置节点集合
// 会返回被标记删除的key！
func (s *SkipList) findGreaterOrEqual(key []byte) (*node, []*node) {
	cur := s.head
	prevNodes := make([]*node, config.SkipListMaxLevel)
	for i := 0; i < len(prevNodes); i++ {
		prevNodes[i] = s.head
	}

	for i := s.height - 1; i >= 0; i-- {
		for cur.forwards[i] != nil && s.cmp.Compare(cur.forwards[i].key, key) < 0 {
			cur = cur.forwards[i]
		}
		prevNodes[i] = cur
	}
	cur = cur.forwards[0]

	return cur, prevNodes
}

// 返回1~maxLevel之间的数，且：
// 1/2 的概率返回 1
// 1/4 的概率返回 2
// 1/8 的概率返回 3... 以此类推
func (s *SkipList) randomLevel() int {
	level := 1
	rand.Seed(time.Now().UnixNano())
	for rand.Intn(2) == 0 && level < config.SkipListMaxLevel {
		level++
	}
	return level
}
