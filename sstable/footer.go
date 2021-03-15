package sstable

import (
	"encoding/binary"
	"io"

	"github.com/Cauchy-NY/yldb/errors"
)

const (
	kTableMagicNumber uint64 = 0xdb4775248b80fb57
)

type Footer struct {
	MetaIndexHandle BlockHandle
	IndexHandle     BlockHandle
}

func (footer *Footer) size() int {
	// add magic Size
	return binary.Size(footer) + 8
}

func (footer *Footer) encodeTo(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, footer); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, kTableMagicNumber); err != nil {
		return err
	}
	return nil
}

func (footer *Footer) decodeFrom(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, footer); err != nil {
		return err
	}
	var magic uint64
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return err
	}
	if magic != kTableMagicNumber {
		return errors.ErrFooterBadMagicNumber
	}
	return nil
}
