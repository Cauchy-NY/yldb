package utils

type ReadOptions struct {
	// 保留待用
}

type WriteOptions struct {
	// 保留待用
	sync bool
}

func (o *WriteOptions) GetSync() bool {
	return o != nil && o.sync
}
