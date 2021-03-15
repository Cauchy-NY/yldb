package config

import "time"

const (
	// 跳表最大层数
	SkipListMaxLevel = 12

	// 持久化ss_table文件最大层数
	NumLevels = 7

	MaxOpenFiles          = 1000
	NumNonTableCacheFiles = 10

	// 调节写入速度
	L0SlowdownWritesTrigger = 8
	SlowdownSleepTime       = time.Duration(1000) * time.Microsecond

	// Compaction相关
	L0CompactionTrigger = 4
	WriteBufferSize     = 4 << 20
	MaxMemCompactLevel  = 2
	L1FileMaxBytes      = 10.0 * 1048576.0
	MaxFileSize         = 2 << 20

	MaxBlockSize = 4 * 1024
)
