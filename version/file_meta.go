package version

import (
	"encoding/binary"
	"io"

	"github.com/Cauchy-NY/yldb/errors"
	"github.com/Cauchy-NY/yldb/ikey"
)

type FileMetaData struct {
	allowSeeks uint64
	number     uint64
	fileSize   uint64
	smallest   ikey.InternalKey
	largest    ikey.InternalKey
}

func (meta *FileMetaData) EncodeTo(w io.Writer) error {
	var errs []error
	errs = append(errs, binary.Write(w, binary.LittleEndian, meta.allowSeeks))
	errs = append(errs, binary.Write(w, binary.LittleEndian, meta.fileSize))
	errs = append(errs, binary.Write(w, binary.LittleEndian, meta.number))
	errs = append(errs, binary.Write(w, binary.LittleEndian, int32(len(meta.smallest))))
	errs = append(errs, binary.Write(w, binary.LittleEndian, meta.smallest))
	errs = append(errs, binary.Write(w, binary.LittleEndian, int32(len(meta.largest))))
	errs = append(errs, binary.Write(w, binary.LittleEndian, meta.largest))

	for _, err := range errs {
		if err != nil {
			return errors.ErrFileMetaEncodeError
		}
	}
	return nil
}

func (meta *FileMetaData) DecodeFrom(r io.Reader) error {
	var errs []error
	var length int32

	errs = append(errs, binary.Read(r, binary.LittleEndian, &meta.allowSeeks))
	errs = append(errs, binary.Read(r, binary.LittleEndian, &meta.fileSize))
	errs = append(errs, binary.Read(r, binary.LittleEndian, &meta.number))

	errs = append(errs, binary.Read(r, binary.LittleEndian, &length))
	meta.smallest = make([]byte, length)
	errs = append(errs, binary.Read(r, binary.LittleEndian, &meta.smallest))

	errs = append(errs, binary.Read(r, binary.LittleEndian, &length))
	meta.largest = make([]byte, length)
	errs = append(errs, binary.Read(r, binary.LittleEndian, &meta.largest))

	for _, err := range errs {
		if err != nil {
			return errors.ErrFileMetaDecodeError
		}
	}
	return nil
}
