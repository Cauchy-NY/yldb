package version

import (
	"github.com/Cauchy-NY/yldb/errors"
	"log"

	"github.com/Cauchy-NY/yldb/config"
	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/memdb"
	"github.com/Cauchy-NY/yldb/sstable"
	"github.com/Cauchy-NY/yldb/utils"
)

type Compaction struct {
	// 合并的上层Level
	level int
	// inputs[0] 待合并的上层SST Files
	// inputs[1] 合并到下层的SST Files
	inputs [2][]*FileMetaData
}

func (compaction *Compaction) isTrivialMove() bool {
	return len(compaction.inputs[0]) == 1 && len(compaction.inputs[1]) == 0
}

func (compaction *Compaction) Log() {
	log.Printf("Compaction, Level:%d\n", compaction.level)
	for i := 0; i < len(compaction.inputs[0]); i++ {
		log.Printf("inputs[0]: %d\n", compaction.inputs[0][i].number)
	}
	for i := 0; i < len(compaction.inputs[1]); i++ {
		log.Printf("inputs[1]: %d\n", compaction.inputs[1][i].number)
	}
}

func (version *Version) WriteLevel0Table(imm *memdb.MemTable) error {
	meta := &FileMetaData{
		allowSeeks: 1 << 30,
		number:     version.nextFileNumber,
	}
	version.nextFileNumber++

	builder, err := sstable.NewTableBuilder(utils.TableFileName(version.tableCache.dbName, meta.number))
	if builder == nil || err != nil {
		return err
	}

	it := imm.Iterator()
	it.SeekToFirst()
	if it.Valid() {
		meta.smallest = it.InternalKey()
		for ; it.Valid(); it.Next() {
			meta.largest = it.InternalKey()
			builder.Add(it.InternalKey(), it.Value())
		}
		builder.Finish()
		meta.fileSize = uint64(builder.FileSize())
	}

	// 优化：如果新写入磁盘的ImmTable数据范围和L0层SST文件没有交集，则说明在L0层没有ImmTable内任何Key的OldValue
	//      即由ImmTable新生成的SST文件可以写入LN层，N∈[0, MaxMemCompactLevel)，且0-N层都没有与其相交的SST文件
	level := 0
	if !version.overlapInLevel(level, meta.smallest.UserKey(), meta.largest.UserKey()) {
		for ; level < config.MaxMemCompactLevel; level++ {
			if version.overlapInLevel(level+1, meta.smallest.UserKey(), meta.largest.UserKey()) {
				break
			}
		}
	}

	version.addMetaFile(level, meta)

	return nil
}

func (version *Version) overlapInLevel(level int, smallestKey, largestKey []byte) bool {
	numFiles := len(version.files[level])
	if numFiles == 0 {
		return false
	}
	if level == 0 {
		for i := 0; i < numFiles; i++ {
			file := version.files[level][i]
			if version.cmp.Compare(smallestKey, file.largest.UserKey()) > 0 ||
				version.cmp.Compare(largestKey, file.smallest.UserKey()) < 0 {
				continue
			} else {
				return true
			}
		}
	} else {
		index := version.findFile(version.files[level], smallestKey)
		if index < numFiles && version.cmp.Compare(largestKey, version.files[level][index].smallest.UserKey()) > 0 {
			return true
		}
	}
	return false
}

func (version Version) deleteMetaFile(level int, meta *FileMetaData) {
	// todo 旧文件物理删除
	numFiles := len(version.files[level])
	for i := 0; i < numFiles; i++ {
		if version.files[level][i].number == meta.number {
			version.files[level] = append(version.files[level][:i], version.files[level][i+1:]...)
			log.Printf("DeleteFile, Level:%d, Num:%d, %s-%s",
				level, meta.number,
				string(meta.smallest.UserKey()),
				string(meta.largest.UserKey()),
			)
			break
		}
	}
}

func (version *Version) addMetaFile(level int, meta *FileMetaData) {
	log.Printf("AddFile, Level:%d, Num:%d, %s-%s",
		level, meta.number,
		string(meta.smallest.UserKey()),
		string(meta.largest.UserKey()),
	)

	if level == 0 {
		// level0不需要归并
		version.files[level] = append(version.files[level], meta)
	} else {
		numFiles := len(version.files[level])
		index := version.findFile(version.files[level], meta.smallest.UserKey())
		if index >= numFiles {
			version.files[level] = append(version.files[level], meta)
		} else {
			var tmp []*FileMetaData
			tmp = append(tmp, version.files[level][:index]...)
			tmp = append(tmp, meta)
			version.files[level] = append(tmp, version.files[level][index:]...)
		}
	}
}

func (version *Version) DoCompactionWork() bool {
	compaction := version.pickCompaction()
	if compaction == nil {
		return false
	}
	log.Printf("DoCompactionWork begin\n")
	defer log.Printf("DoCompactionWork end\n")
	compaction.Log()
	if compaction.isTrivialMove() {
		// Move file to next level
		version.deleteMetaFile(compaction.level, compaction.inputs[0][0])
		version.addMetaFile(compaction.level+1, compaction.inputs[0][0])
		return true
	}

	var list []*FileMetaData
	var cur ikey.InternalKey
	it := version.iterator(compaction)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		var meta FileMetaData
		meta.allowSeeks = 1 << 30
		meta.number = version.nextFileNumber
		version.nextFileNumber++
		builder, _ := sstable.NewTableBuilder(utils.TableFileName(version.tableCache.dbName, meta.number))

		meta.smallest = it.InternalKey()
		for ; it.Valid(); it.Next() {
			if cur != nil {
				// 去除重复的记录
				ret := version.cmp.Compare(it.InternalKey().UserKey(), cur.UserKey())
				if ret == 0 {
					continue
				} else if ret < 0 {
					log.Printf("Error: %v", errors.ErrMajorCompactionError)
					log.Fatalf("%s < %s", string(it.InternalKey().UserKey()), string(cur.UserKey()))
				}
				cur = it.InternalKey()
			}
			meta.largest = it.InternalKey()
			builder.Add(it.InternalKey(), it.Value())
			if builder.FileSize() > config.MaxFileSize {
				break
			}
		}
		builder.Finish()
		meta.fileSize = uint64(builder.FileSize())

		list = append(list, &meta)
	}
	return false
}

func (version *Version) pickCompaction() *Compaction {
	var compaction Compaction
	compaction.level = version.pickCompactionLevel()
	if compaction.level < 0 {
		return nil
	}
	var smallest, largest ikey.InternalKey
	if compaction.level == 0 {
		// 简化处理：Level0整层进行Compaction
		compaction.inputs[0] = append(compaction.inputs[0], version.files[compaction.level]...)
		smallest = compaction.inputs[0][0].smallest
		largest = compaction.inputs[0][0].largest
		for _, file := range compaction.inputs[0] {
			if version.cmp.Compare(file.largest.UserKey(), largest.UserKey()) > 0 {
				largest = file.largest
			}
			if version.cmp.Compare(file.smallest.UserKey(), smallest.UserKey()) < 0 {
				smallest = file.smallest
			}
		}
	} else {
		// 从上一次compaction之后的文件开始选取文件进行compaction
		// 上一次选取的compaction的文件记录在version.compactPointer[level]
		for _, file := range version.files[compaction.level] {
			if version.compactPointer[compaction.level] == nil ||
				version.cmp.Compare(file.largest.UserKey(), version.compactPointer[compaction.level].UserKey()) > 0 {
				compaction.inputs[0] = append(compaction.inputs[0], file)
				break
			}
		}
		// 上一次compaction的文件是该层最后一个文件，则将选择该层第一个文件进行compaction
		if len(compaction.inputs[0]) == 0 {
			compaction.inputs[0] = append(compaction.inputs[0], version.files[compaction.level][0])
		}
		smallest = compaction.inputs[0][0].smallest
		largest = compaction.inputs[0][0].largest
	}

	// 选择下一次范围重叠的文件进行compaction
	for _, file := range version.files[compaction.level+1] {
		if version.cmp.Compare(file.largest.UserKey(), smallest.UserKey()) < 0 ||
			version.cmp.Compare(file.smallest.UserKey(), largest.UserKey()) > 0 {
			// "file"的key在合并范围之外，跳过
		} else {
			compaction.inputs[1] = append(compaction.inputs[1], file)
		}
	}
	return &compaction
}

func (version *Version) pickCompactionLevel() int {
	// 通过计算每层的score，选出最急需Compaction的Level
	// 若第LevelN得分最大且大于1.0，则第N层为所选Compaction的Level
	// Level0分数计算和该层文件数相关，其他各层分数计算与文件大小相关
	// 若所有Level的score均小于1.0，则返回-1
	compactionLevel := -1
	bestScore := 1.0
	score := 0.0
	for level := 0; level < config.NumLevels-1; level++ {
		if level == 0 {
			score = float64(len(version.files[0])) / float64(config.L0CompactionTrigger)
		} else {
			score = float64(totalFileSize(version.files[level])) / maxBytesForLevel(level)
		}

		if score > bestScore {
			bestScore = score
			compactionLevel = level
		}

	}
	return compactionLevel
}

func totalFileSize(files []*FileMetaData) uint64 {
	var sum uint64
	for i := 0; i < len(files); i++ {
		sum += files[i].fileSize
	}
	return sum
}

func maxBytesForLevel(level int) float64 {
	result := config.L1FileMaxBytes
	for level > 1 {
		result *= 10
		level--
	}
	return result
}

func (version *Version) iterator(c *Compaction) *MergeIterator {
	var list []*sstable.TableIterator
	for i := 0; i < len(c.inputs[0]); i++ {
		list = append(list, version.tableCache.iterator(c.inputs[0][i].number))
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		list = append(list, version.tableCache.iterator(c.inputs[1][i].number))
	}
	it := &MergeIterator{
		list: list,
		cmp:  version.cmp,
	}
	return it
}
