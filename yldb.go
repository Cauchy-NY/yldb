package yldb

import (
	"os"
	"sync"
	"time"

	"github.com/Cauchy-NY/yldb/config"
	"github.com/Cauchy-NY/yldb/errors"
	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/memdb"
	"github.com/Cauchy-NY/yldb/utils"
	"github.com/Cauchy-NY/yldb/version"
)

type YLDB struct {
	name       string
	mem        *memdb.MemTable
	imm        *memdb.MemTable
	current    *version.Version
	mutex      sync.Mutex
	cond       *sync.Cond
	compacting bool
	closed     bool
}

func Open(dbName string) (*YLDB, error) {
	err := os.MkdirAll(dbName, 0755)
	if err != nil {
		return nil, err
	}
	db := &YLDB{
		name:       dbName,
		mem:        memdb.NewMemTable(nil),
		imm:        nil,
		mutex:      sync.Mutex{},
		compacting: false,
		closed:     false,
	}
	db.cond = sync.NewCond(&db.mutex)
	num := db.ReadCurrentFile()
	if num > 0 {
		v, err := version.Load(dbName, num)
		if err != nil {
			return nil, err
		}
		db.current = v
	} else {
		db.current = version.NewVersion(dbName, nil)
	}
	return db, nil
}

func (db *YLDB) Close() {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	for db.compacting {
		db.cond.Wait()
	}
}

func (db *YLDB) Get(key []byte, opts *utils.ReadOptions) ([]byte, error) {
	// todo 增加LastSeq&VersionSet机制，实现MVCC，取消全局锁
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.mem != nil { // 1.先查内存中的MemTable
		if val, err := db.mem.Get(key); err != errors.ErrMemTableNotFound {
			return val, nil
		}
	}

	if db.imm != nil { // 2.再查内存中的ImmTable
		if val, err := db.imm.Get(key); err != errors.ErrMemTableNotFound {
			return val, nil
		}
	}

	if db.current != nil { // 3.最后对磁盘上的数据按Level由新到旧依次查询
		if val, err := db.current.Get(key); err != errors.ErrVersionNotFound {
			return val, nil
		}
	}

	return nil, errors.ErrDBNotFound
}

func (db *YLDB) Set(key, value []byte, opts *utils.WriteOptions) error {
	var batch Batch
	batch.Set(key, value)
	return db.Apply(batch, opts)
}

func (db *YLDB) Delete(key []byte, opts *utils.WriteOptions) error {
	var batch Batch
	batch.Delete(key)
	return db.Apply(batch, opts)
}

func (db *YLDB) Apply(batch Batch, opts *utils.WriteOptions) error {
	if len(batch.data) == 0 {
		return nil
	}
	n := batch.count()
	if n == invalidBatchCount {
		return errors.ErrBatchInvalid
	}

	db.mutex.Lock()
	defer db.mutex.Unlock()

	// 保证现在有MemTable有足够空间进行写入操作
	if err := db.makeRoomForWrite(); err != nil {
		return err
	}

	// todo batch写入 append log

	// batch添加到MemTable中
	it := batch.iterator()
	for {
		kind, userKey, value, ok := it.next()
		if !ok {
			break
		}

		seqNum := db.current.NextSeq()
		internalKey := ikey.MakeInternalKey(nil, userKey, kind, seqNum)
		// 暂时不考虑MemTable在batch set操作中出现的error
		_ = db.mem.Set(internalKey, value)
	}

	return nil
}

func (db *YLDB) makeRoomForWrite() error {
	for true {
		if db.current.NumLevelFiles(0) >= config.L0SlowdownWritesTrigger {
			// 调整写入速度
			db.mutex.Unlock()
			time.Sleep(config.SlowdownSleepTime)
			db.mutex.Lock()
		} else if db.mem.ApproximateMemoryUsage() <= config.WriteBufferSize {
			// 当前MemTable未满，可以写入
			return nil
		} else if db.imm != nil {
			// 当前MemTable满了，且ImmTable尚在Compaction
			db.cond.Wait()
		} else {
			// MemTable转换为ImmTable并触发Compaction
			// todo log文件转换
			db.imm = db.mem
			db.mem = memdb.NewMemTable(nil)
			db.maybeScheduleCompaction()
		}
	}
	return nil
}
