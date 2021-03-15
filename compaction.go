package yldb

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/Cauchy-NY/yldb/errors"
	"github.com/Cauchy-NY/yldb/utils"
)

func (db *YLDB) maybeScheduleCompaction() {
	if db.compacting || db.closed {
		return
	}
	if db.imm == nil {
		return
	}
	db.compacting = true
	go db.compact()
}

func (db *YLDB) compact() {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	db.backgroundCompact()

	db.compacting = false
	// 之前一次的compaction可能在某一层产生过多的文件，所以可能还需要一次compaction（暂时不这么做）
	// db.maybeScheduleCompaction()
	db.cond.Broadcast()
}

// compaction主要逻辑
func (db *YLDB) backgroundCompact() {
	imm := db.imm
	version := db.current.Copy()
	db.mutex.Unlock()

	// minor compaction
	if imm != nil {
		err := version.WriteLevel0Table(imm)
		log.Printf("Error: %v, Caused by: %v", errors.ErrMinorCompactionError, err)
	}

	// major compaction
	for version.DoCompactionWork() {
		version.Log()
	}

	descriptorNumber, _ := version.Save()
	db.SetCurrentFile(descriptorNumber)
	db.mutex.Lock()
	db.imm = nil
	db.current = version
}

func (db *YLDB) SetCurrentFile(number uint64) {
	tmp := utils.TempFileName(db.name, number)
	_ = ioutil.WriteFile(tmp, []byte(fmt.Sprintf("%d", number)), 0600)
	_ = os.Rename(tmp, utils.CurrentFileName(db.name))
}

func (db *YLDB) ReadCurrentFile() uint64 {
	b, err := ioutil.ReadFile(utils.CurrentFileName(db.name))
	if err != nil {
		return 0
	}
	descriptorNumber, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return descriptorNumber
}
