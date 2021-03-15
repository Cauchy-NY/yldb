package version

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"sort"

	"github.com/Cauchy-NY/yldb/config"
	"github.com/Cauchy-NY/yldb/errors"
	"github.com/Cauchy-NY/yldb/ikey"
	"github.com/Cauchy-NY/yldb/utils"
)

type Version struct {
	tableCache     *TableCache
	nextFileNumber uint64
	seq            uint64
	files          [config.NumLevels][]*FileMetaData
	compactPointer [config.NumLevels]ikey.InternalKey
	cmp            utils.Comparator
}

func NewVersion(dbName string, cmp utils.Comparator) *Version {
	version := &Version{
		tableCache:     NewTableCache(dbName),
		nextFileNumber: 1,
		cmp:            cmp,
	}
	if cmp == nil {
		version.cmp = utils.NewDefaultComparator()
	}
	return version
}

func Load(dbName string, number uint64) (*Version, error) {
	fileName := utils.DescriptorFileName(dbName, number)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	version := NewVersion(dbName, nil)
	return version, version.DecodeFrom(file)
}

func (version *Version) Save() (uint64, error) {
	fileNumber := version.nextFileNumber
	fileName := utils.DescriptorFileName(version.tableCache.dbName, version.nextFileNumber)
	version.nextFileNumber++
	file, err := os.Create(fileName)
	if err != nil {
		return fileNumber, err
	}
	defer file.Close()
	return fileNumber, version.EncodeTo(file)
}

func (version *Version) Copy() *Version {
	copyVersion := &Version{
		tableCache:     version.tableCache,
		nextFileNumber: version.nextFileNumber,
		seq:            version.seq,
		cmp:            version.cmp,
	}
	for level := 0; level < config.NumLevels; level++ {
		copyVersion.files[level] = make([]*FileMetaData, len(version.files[level]))
		copy(copyVersion.files[level], version.files[level])
	}
	return copyVersion
}

func (version *Version) EncodeTo(w io.Writer) error {
	var errs []error

	errs = append(errs, binary.Write(w, binary.LittleEndian, version.nextFileNumber))
	errs = append(errs, binary.Write(w, binary.LittleEndian, version.seq))
	for level := 0; level < config.NumLevels; level++ {
		numFiles := len(version.files[level])
		errs = append(errs, binary.Write(w, binary.LittleEndian, int32(numFiles)))

		for i := 0; i < numFiles; i++ {
			errs = append(errs, version.files[level][i].EncodeTo(w))
		}
	}

	for _, err := range errs {
		if err != nil {
			return errors.ErrVersionEncodeError
		}
	}
	return nil
}

func (version *Version) DecodeFrom(r io.Reader) error {
	var errs []error

	errs = append(errs, binary.Read(r, binary.LittleEndian, &version.nextFileNumber))
	errs = append(errs, binary.Read(r, binary.LittleEndian, &version.seq))
	var numFiles int32
	for level := 0; level < config.NumLevels; level++ {
		errs = append(errs, binary.Read(r, binary.LittleEndian, &numFiles))
		version.files[level] = make([]*FileMetaData, numFiles)
		for i := 0; i < int(numFiles); i++ {
			var meta FileMetaData
			errs = append(errs, meta.DecodeFrom(r))
			version.files[level][i] = &meta
		}
	}

	for _, err := range errs {
		if err != nil {
			return errors.ErrVersionDecodeError
		}
	}
	return nil
}

func (version *Version) Log() {
	for level := 0; level < config.NumLevels; level++ {
		log.Printf("Version Level %v:\n", level)
		for i := 0; i < len(version.files[level]); i++ {
			log.Println(utils.TableFileName(version.tableCache.dbName, version.files[level][i].number))
		}
	}
}

func (version *Version) NumLevelFiles(l int) int {
	return len(version.files[l])
}

func (version *Version) NextSeq() uint64 {
	version.seq++
	return version.seq
}

func (version *Version) Get(ukey []byte) ([]byte, error) {
	var searchFiles []*FileMetaData // user_key可能存在的文件集合

	for level := 0; level < config.NumLevels; level++ {
		numFiles := len(version.files[level])
		if numFiles == 0 {
			continue
		}
		if level == 0 {
			// level0各文件的key范围可能存在重叠
			for i := 0; i < numFiles; i++ {
				file := version.files[level][i]
				if version.cmp.Compare(ukey, file.smallest.UserKey()) >= 0 &&
					version.cmp.Compare(ukey, file.largest.UserKey()) <= 0 {
					searchFiles = append(searchFiles, file)
				}
			}
			if len(searchFiles) == 0 {
				continue
			}
			// level0中文件按时间倒排，以最新的数据副本为准
			sort.Slice(searchFiles, func(i, j int) bool {
				return searchFiles[i].number > searchFiles[j].number
			})
		} else {
			// 从level1开始每层的各文件key范围之间不存在重叠
			files := version.files[level]
			index := version.findFile(files, ukey)
			if index < numFiles && version.cmp.Compare(files[index].smallest.UserKey(), ukey) <= 0 &&
				version.cmp.Compare(files[index].largest.UserKey(), ukey) >= 0 {
				// 该文件中可能含有user_key
				searchFiles = append(searchFiles, version.files[level][index])
			}
		}
		for _, file := range searchFiles {
			if value, err := version.tableCache.Get(file.number, ukey); err != errors.ErrSSTableNotFound {
				return value, err
			}
		}
		searchFiles = searchFiles[:0] // 该层搜索文件清空
	}
	return nil, errors.ErrVersionNotFound
}

// 在LN(N>0)层二分查找可能含有user_key的文件
// 当user_key小于该层所有key时，return 0
// 当user_key大于该层所有key时，return len(files)
func (version *Version) findFile(files []*FileMetaData, ukey []byte) int {
	left := 0
	right := len(files)
	for left < right {
		mid := (left + right) / 2
		file := files[mid]
		if version.cmp.Compare(file.largest.UserKey(), ukey) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return right
}
