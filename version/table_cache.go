package version

import (
	"sync"

	"github.com/Cauchy-NY/yldb/config"
	"github.com/Cauchy-NY/yldb/sstable"
	"github.com/Cauchy-NY/yldb/utils"
)

type TableCache struct {
	mu     sync.Mutex
	dbName string
	cache  *LRUCache
}

func NewTableCache(dbName string) *TableCache {
	lruCache, _ := newLRU(config.MaxOpenFiles - config.NumNonTableCacheFiles)
	return &TableCache{
		mu:     sync.Mutex{},
		dbName: dbName,
		cache:  lruCache,
	}
}

func (tableCache *TableCache) Evict(fileNum uint64) {
	tableCache.cache.Remove(fileNum)
}

func (tableCache *TableCache) Get(fileNum uint64, key []byte) ([]byte, error) {
	table, err := tableCache.findTable(fileNum)
	if table != nil {
		return table.Get(key)
	}
	return nil, err
}

func (tableCache *TableCache) findTable(fileNum uint64) (*sstable.SSTable, error) {
	tableCache.mu.Lock()
	defer tableCache.mu.Unlock()

	if table, ok := tableCache.cache.Get(fileNum); ok {
		return table.(*sstable.SSTable), nil
	} else {
		ssTable, err := sstable.Open(utils.TableFileName(tableCache.dbName, fileNum))
		tableCache.cache.Set(fileNum, ssTable)
		return ssTable, err
	}
}

func (tableCache *TableCache) iterator(fileNum uint64) *sstable.TableIterator {
	table, _ := tableCache.findTable(fileNum)
	if table != nil {
		return table.Iterator()
	}
	return nil
}
