package golmdb

/*
#include <lmdb.h>
*/
import "C"
import (
	"fmt"
	"syscall"
)

// The version of LMDB that has been linked against.
const Version = C.MDB_VERSION_STRING

// Used in calls to NewLMDB() and NewManagedLMDB()
type EnvironmentFlag C.uint

// Environment flags.
//
// NB WriteMap is not exported because it's incompatible with nested
// transactions, and this binding internally relies on nested txns.
//
// See
// http://www.lmdb.tech/doc/group__mdb__env.html and
// http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340
const (
	FixedMap   = EnvironmentFlag(C.MDB_FIXEDMAP)
	NoSubDir   = EnvironmentFlag(C.MDB_NOSUBDIR)
	NoSync     = EnvironmentFlag(C.MDB_NOSYNC)
	ReadOnly   = EnvironmentFlag(C.MDB_RDONLY)
	NoMetaSync = EnvironmentFlag(C.MDB_NOMETASYNC)
	// Not exported because it's incompatible with nested txns
	writeMap    = EnvironmentFlag(C.MDB_WRITEMAP)
	MapAsync    = EnvironmentFlag(C.MDB_MAPASYNC)
	NoTLS       = EnvironmentFlag(C.MDB_NOTLS)
	NoLock      = EnvironmentFlag(C.MDB_NOLOCK)
	NoReadAhead = EnvironmentFlag(C.MDB_NORDAHEAD)
	NoMemLimit  = EnvironmentFlag(C.MDB_NOMEMINIT)
)

// Used in calls to ReadOnlyTxn.DBRef()
type DatabaseFlag C.uint

// Database flags
//
// See http://www.lmdb.tech/doc/group__mdb__dbi__open.html
const (
	ReverseKey = DatabaseFlag(C.MDB_REVERSEKEY)
	DupSort    = DatabaseFlag(C.MDB_DUPSORT)
	IntegerKey = DatabaseFlag(C.MDB_INTEGERKEY)
	DupFixed   = DatabaseFlag(C.MDB_DUPFIXED)
	IntegerDup = DatabaseFlag(C.MDB_INTEGERDUP)
	ReverseDup = DatabaseFlag(C.MDB_REVERSEDUP)
	Create     = DatabaseFlag(C.MDB_CREATE)
)

// Used in calls to ReadWriteTxn.Put(), ReadWriteTxn.PutDupSort(), Cursor.Put(), and Cursor.PutDupSort()
type PutFlag C.uint

// Put flags
//
// See http://www.lmdb.tech/doc/group__mdb__put.html
const (
	NoOverwrite = PutFlag(C.MDB_NOOVERWRITE)
	NoDupData   = PutFlag(C.MDB_NODUPDATA)
	Current     = PutFlag(C.MDB_CURRENT)
	reserve     = PutFlag(C.MDB_RESERVE) // not exported as the API doesn't support it
	Append      = PutFlag(C.MDB_APPEND)
	AppendDup   = PutFlag(C.MDB_APPENDDUP)
	multiple    = PutFlag(C.MDB_MULTIPLE) // not exported as the API doesn't support it
)

// Used in calls to Cursor.GetAndMove
type cursorOp C.uint

// Cursor ops
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127
const (
	first    = cursorOp(C.MDB_FIRST)
	firstDup = cursorOp(C.MDB_FIRST_DUP)

	last    = cursorOp(C.MDB_LAST)
	lastDup = cursorOp(C.MDB_LAST_DUP)

	getCurrent = cursorOp(C.MDB_GET_CURRENT)

	getBoth      = cursorOp(C.MDB_GET_BOTH)
	getBothRange = cursorOp(C.MDB_GET_BOTH_RANGE)

	set      = cursorOp(C.MDB_SET) // Move to the given key. Don't return anything.
	setKey   = cursorOp(C.MDB_SET_KEY)
	setRange = cursorOp(C.MDB_SET_RANGE)

	next      = cursorOp(C.MDB_NEXT)
	nextDup   = cursorOp(C.MDB_NEXT_DUP)
	nextNoDup = cursorOp(C.MDB_NEXT_NODUP)

	prev      = cursorOp(C.MDB_PREV)
	prevDup   = cursorOp(C.MDB_PREV_DUP)
	prevNoDup = cursorOp(C.MDB_PREV_NODUP)

	getMultiple  = cursorOp(C.MDB_GET_MULTIPLE)
	nextMultiple = cursorOp(C.MDB_NEXT_MULTIPLE)
)

// Copy flags. http://www.lmdb.tech/doc/group__mdb__copy.html
const copyCompact = C.MDB_CP_COMPACT

// An LMDB error. See the Return Codes in the Constants section.
type LMDBError C.int

// Return codes
//
// KeyExist and NotFound are return codes you may well encounter and
// expect to deal with in application code. The rest of them probably
// indicate something has gone terribly wrong.
//
// See
// http://www.lmdb.tech/doc/group__errors.html
const (
	success         = C.MDB_SUCCESS
	KeyExist        = LMDBError(C.MDB_KEYEXIST)
	NotFound        = LMDBError(C.MDB_NOTFOUND)
	PageNotFound    = LMDBError(C.MDB_PAGE_NOTFOUND)
	Corrupted       = LMDBError(C.MDB_CORRUPTED)
	PanicMDB        = LMDBError(C.MDB_PANIC)
	VersionMismatch = LMDBError(C.MDB_VERSION_MISMATCH)
	Invalid         = LMDBError(C.MDB_INVALID)
	MapFull         = LMDBError(C.MDB_MAP_FULL)
	DBsFull         = LMDBError(C.MDB_DBS_FULL)
	ReadersFull     = LMDBError(C.MDB_READERS_FULL)
	TLSFull         = LMDBError(C.MDB_TLS_FULL)
	TxnFull         = LMDBError(C.MDB_TXN_FULL)
	CursorFull      = LMDBError(C.MDB_CURSOR_FULL)
	PageFull        = LMDBError(C.MDB_PAGE_FULL)
	MapResized      = LMDBError(C.MDB_MAP_RESIZED)
	Incompatible    = LMDBError(C.MDB_INCOMPATIBLE)
	BadRSlot        = LMDBError(C.MDB_BAD_RSLOT)
	BadTxt          = LMDBError(C.MDB_BAD_TXN)
	BadValSize      = LMDBError(C.MDB_BAD_VALSIZE)
	BadDBI          = LMDBError(C.MDB_BAD_DBI)
)

const minErrno, maxErrno = C.MDB_KEYEXIST, C.MDB_LAST_ERRCODE

func (self LMDBError) Error() string {
	str := C.GoString(C.mdb_strerror(C.int(self)))
	if str == "" {
		return fmt.Sprintf(`LMDB Error: %d`, int(self))
	}
	return str
}

func asError(code C.int) error {
	if code == success {
		return nil
	}
	// If you check the url http://www.lmdb.tech/doc/group__errors.html
	// it should show that the return codes form a contiguous sequence,
	// and that maxErrno is inclusive as it's an alias of BadDBI
	if minErrno <= code && code <= maxErrno {
		return LMDBError(code)
	}
	return syscall.Errno(code)
}
