package version

import "github.com/Cauchy-NY/yldb/errors"

type LRUCache struct {
	head, tail *Node
	items      map[interface{}]*Node
	len        int
	size       int
}

type Node struct {
	key, val   interface{}
	prev, next *Node
}

func newLRU(size int) (*LRUCache, error) {
	if size <= 0 {
		return nil, errors.ErrLRUCacheSizeNegative
	}
	head := &Node{nil, nil, nil, nil}
	tail := &Node{nil, nil, nil, nil}
	head.next = tail
	tail.next = head
	return &LRUCache{
		head:  head,
		tail:  tail,
		items: make(map[interface{}]*Node),
		size:  size,
		len:   0,
	}, nil
}

func (cache *LRUCache) Clear() {
	for key := range cache.items {
		delete(cache.items, key)
	}
	cache.head.next = cache.tail
	cache.tail.prev = cache.head
	cache.len = 0
}

func (cache *LRUCache) Len() int {
	return cache.len
}

func (cache *LRUCache) Contains(key interface{}) bool {
	_, exist := cache.items[key]
	return exist
}

// 从最老到最新
func (cache *LRUCache) Keys() []interface{} {
	keys := make([]interface{}, len(cache.items))
	i := 0
	for node := cache.tail.prev; node != cache.head; node = node.prev {
		keys[i] = node.key
		i++
	}
	return keys
}

func (cache *LRUCache) RemoveOldest() (interface{}, interface{}, bool) {
	node := cache.tail.prev
	if node != nil && node != cache.head {
		cache.Remove(node.key)
		return node.key, node.val, true
	}
	return nil, nil, false
}

func (cache *LRUCache) GetOldest() (interface{}, interface{}, bool) {
	node := cache.tail.prev
	if node != nil && node != cache.head {
		return node.key, node.val, true
	}
	return nil, nil, false
}

func (cache *LRUCache) Set(key, val interface{}) {
	items := cache.items
	head, tail := cache.head, cache.tail
	if node, exist := items[key]; exist {
		node.val = val
		cache.moveToHead(node)
	} else {
		node := &Node{key: key, val: val, prev: nil, next: nil}
		if cache.len == cache.size {
			delete(items, tail.prev.key)
			tail.prev.prev.next = tail
			tail.prev = tail.prev.prev
		} else {
			cache.len++
		}
		node.next = head.next
		node.prev = head
		head.next.prev = node
		head.next = node
		items[key] = node
	}
}

// 返回value，而不会更新Node顺序
func (cache *LRUCache) Peek(key interface{}) (interface{}, bool) {
	if node, exist := cache.items[key]; exist {
		return node.val, exist
	}
	return nil, false
}

func (cache *LRUCache) Get(key interface{}) (interface{}, bool) {
	if node, exist := cache.items[key]; exist {
		cache.moveToHead(node)
		return node.val, exist
	}
	return nil, false
}

func (cache *LRUCache) moveToHead(node *Node) {
	head := cache.head
	// 从当前位置删除节点
	node.prev.next = node.next
	node.next.prev = node.prev
	// 将节点插入头部
	node.next = head.next
	head.next.prev = node
	node.prev = head
	head.next = node
}

func (cache *LRUCache) Remove(key interface{}) bool {
	if node, exist := cache.items[key]; exist {
		node.prev.next = node.next
		node.next.prev = node.prev
		delete(cache.items, node.key)
		return true
	}
	return false
}
