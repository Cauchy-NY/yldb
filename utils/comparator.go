package utils

import (
	"bytes"
)

type Comparator interface {
	Compare(a, b []byte) int
	Name() string
}

type DefaultComparator struct{}

func NewDefaultComparator() DefaultComparator {
	return DefaultComparator{}
}

func (d DefaultComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (d DefaultComparator) Name() string {
	return "yldb.DefaultComparator"
}
