package errors

import "errors"

var (
	// MemTable errors
	ErrMemTableNotFound = errors.New("YLDB.Error.MemTable.NotFound")

	// IKey & Entry errors
	ErrEntryEncodeError = errors.New("YLDB.Error.Entry.EncodeError")
	ErrEntryDecodeError = errors.New("YLDB.Error.Entry.DecodeError")

	// SSTable errors
	ErrSSTableFileTooShort  = errors.New("YLDB.Error.SSTable.TooShort")
	ErrSSTableDeletion      = errors.New("YLDB.Error.SSTable.AlreadyDeletionError")
	ErrSSTableNotFound      = errors.New("YLDB.Error.SSTable.NotFound")
	ErrFooterBadMagicNumber = errors.New("YLDB.Error.SSTable.BadMagicNumber")

	// Version errors
	ErrLRUCacheSizeNegative = errors.New("YLDB.Error.LRUCache.SizeNegative")
	ErrVersionNotFound      = errors.New("YLDB.Error.Version.NotFound")
	ErrFileMetaEncodeError  = errors.New("YLDB.Error.FileMeta.EncodeError")
	ErrFileMetaDecodeError  = errors.New("YLDB.Error.FileMeta.DecodeError")
	ErrVersionEncodeError   = errors.New("YLDB.Error.Version.EncodeError")
	ErrVersionDecodeError   = errors.New("YLDB.Error.Version.DecodeError")

	// Compaction errors
	ErrMinorCompactionError = errors.New("YLDB.Error.Compaction.MinorCompactionError")
	ErrMajorCompactionError = errors.New("YLDB.Error.Compaction.MajorCompactionError")

	// Batch errors
	ErrBatchInvalid = errors.New("YLDB.Error.Batch.Invalid")

	// DB errors
	ErrDBNotFound = errors.New("YLDB.Error.DB.NotFound")
)
