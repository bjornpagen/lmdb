package lmdb

/*
#include <stdlib.h>
#include <memory.h>
#include <lmdb.h>
#include "golmdb.h"
*/
import "C"
import (
	"sync/atomic"
	"unsafe"
)

// Cursors allow you to walk over a database, or sections of them.
type ReadOnlyCursor struct {
	cursor         *C.MDB_cursor
	resizeRequired *uint32
}

// A ReadWriteCursor extends ReadOnlyCursor with methods for mutating
// the database.
type ReadWriteCursor struct {
	ReadOnlyCursor
}

// Create a new read-only cursor.
//
// You should call Close() on each cursor before the end of the
// transaction.  The exact rules for cursor lifespans are more
// complex, and are documented at
// http://www.lmdb.tech/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa
// but it's simplest if you treat each cursor as scoped to the
// lifespan of its transaction, and you explicitly Close() each cursor
// before the end of the transaction.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa
func (txn *ReadOnlyTxn) NewCursor(db DBRef) (*ReadOnlyCursor, error) {
	if atomic.LoadUint32(txn.resizeRequired) == 1 {
		return nil, MapFull
	}
	var cursor *C.MDB_cursor
	err := asError(C.mdb_cursor_open(txn.txn, C.MDB_dbi(db), &cursor))
	if err != nil {
		return nil, err
	}
	return &ReadOnlyCursor{cursor: cursor, resizeRequired: txn.resizeRequired}, nil
}

// Create a new read-write cursor.
//
// You should call Close() on each cursor before the end of the
// transaction.  The exact rules for cursor lifespans are more
// complex, and are documented at
// http://www.lmdb.tech/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa
// but it's simplest if you treat each cursor as scoped to the
// lifespan of its transaction, and you explicitly Close() each cursor
// before the end of the transaction.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa
func (txn *ReadWriteTxn) NewCursor(db DBRef) (*ReadWriteCursor, error) {
	var cursor *C.MDB_cursor
	err := asError(C.mdb_cursor_open(txn.txn, C.MDB_dbi(db), &cursor))
	if err != nil {
		return nil, err
	}
	return &ReadWriteCursor{ReadOnlyCursor{cursor: cursor, resizeRequired: txn.resizeRequired}}, nil
}

// Close the current cursor.
//
// You should call Close() on each cursor before the end of the
// transaction in which it was created.
func (cursor *ReadOnlyCursor) Close() {
	C.mdb_cursor_close(cursor.cursor)
	cursor.cursor = nil
}

func (cursor *ReadOnlyCursor) moveAndGet0(op cursorOp) (key, val []byte, err error) {
	if atomic.LoadUint32(cursor.resizeRequired) == 1 {
		return nil, nil, MapFull
	}
	var keyVal, valVal value
	err = asError(C.mdb_cursor_get(cursor.cursor, (*C.MDB_val)(&keyVal), (*C.MDB_val)(&valVal), C.MDB_cursor_op(op)))
	if err != nil {
		return nil, nil, err
	}

	return keyVal.bytesNoCopy(), valVal.bytesNoCopy(), nil
}

func (cursor *ReadOnlyCursor) moveAndGet1(op cursorOp, keyIn []byte) (key, val []byte, err error) {
	if atomic.LoadUint32(cursor.resizeRequired) == 1 {
		return nil, nil, MapFull
	}
	var keyVal, valVal value
	err = asError(C.golmdb_mdb_cursor_get1(cursor.cursor,
		(*C.char)(unsafe.Pointer(&keyIn[0])), C.size_t(len(keyIn)),
		(*C.MDB_val)(&keyVal), (*C.MDB_val)(&valVal), C.MDB_cursor_op(op)))
	if err != nil {
		return nil, nil, err
	}

	return keyVal.bytesNoCopy(), valVal.bytesNoCopy(), nil
}

func (cursor *ReadOnlyCursor) moveAndGet2(op cursorOp, keyIn, valIn []byte) (val []byte, err error) {
	if atomic.LoadUint32(cursor.resizeRequired) == 1 {
		return nil, MapFull
	}
	var valVal value
	err = asError(C.golmdb_mdb_cursor_get2(cursor.cursor,
		(*C.char)(unsafe.Pointer(&keyIn[0])), C.size_t(len(keyIn)),
		(*C.char)(unsafe.Pointer(&valIn[0])), C.size_t(len(valIn)),
		(*C.MDB_val)(&valVal), C.MDB_cursor_op(op)))

	if err != nil {
		return nil, err
	}

	return valVal.bytesNoCopy(), nil
}

// Move to the first key-value pair of the database.
//
// Do not write into the returned key or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) First() (key, val []byte, err error) {
	return cursor.moveAndGet0(first)
}

// Only for DupSort. Move to the first key-value pair without changing
// the current key.
//
// Do not write into the returned val byte slice. Doing so will cause
// a segfault.
func (cursor *ReadOnlyCursor) FirstInSameKey() (val []byte, err error) {
	_, val, err = cursor.moveAndGet0(firstDup)
	return val, err
}

// Move to the last key-value pair of the database.
//
// Do not write into the returned key or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) Last() (key, val []byte, err error) {
	return cursor.moveAndGet0(last)
}

// Only for DupSort. Move to the last key-value pair without changing
// the current key.
//
// Do not write into the returned val byte slice. Doing so will cause
// a segfault.
func (cursor *ReadOnlyCursor) LastInSameKey() (val []byte, err error) {
	_, val, err = cursor.moveAndGet0(lastDup)
	return val, err
}

// Get the current key-value pair of the cursor.
//
// Do not write into the returned key or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) Current() (key, val []byte, err error) {
	return cursor.moveAndGet0(getCurrent)
}

// Move to the next key-value pair.
//
// For DupSort databases, move to the next value of the current
// key, if there is one, otherwise the first value of the next
// key.
//
// Do not write into the returned key or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) Next() (key, val []byte, err error) {
	return cursor.moveAndGet0(next)
}

// Only for DupSort. Move to the next key-value pair, but only if the
// key is the same as the current key.
//
// Do not write into the returned key or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) NextInSameKey() (key, val []byte, err error) {
	return cursor.moveAndGet0(nextDup)
}

// Only for DupSort. Move to the first key-value pair of the next key.
//
// Do not write into the returned key or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) NextKey() (key, val []byte, err error) {
	return cursor.moveAndGet0(nextNoDup)
}

// Move to the previous key-value pair.
//
// For DupSort databases, move to the previous value of the current
// key, if there is one, otherwise the last value of the previous
// key.
//
// Do not write into the returned key or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) Prev() (key, val []byte, err error) {
	return cursor.moveAndGet0(prev)
}

// Only for DupSort. Move to the previous key-value pair, but only if
// the key is the same as the current key.
//
// Do not write into the returned key or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) PrevInSameKey() (key, val []byte, err error) {
	return cursor.moveAndGet0(prevDup)
}

// Only for DupSort. Move to the last key-value pair of the previous
// key.
//
// Do not write into the returned key or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) PrevKey() (key, val []byte, err error) {
	return cursor.moveAndGet0(prevNoDup)
}

// Move to the key-value pair indicated by the given key.
//
// If the exact key doesn't exist, returns NotFound.
//
// For DupSort databases, move to the first value of the given key.
//
// Do not write into the returned val byte slice. Doing so will cause
// a segfault.
func (cursor *ReadOnlyCursor) SeekExactKey(key []byte) (val []byte, err error) {
	_, val, err = cursor.moveAndGet1(setKey, key)
	return val, err
}

// Move to the key-value pair indicated by the given key.
//
// If the exact key doesn't exist, move to the nearest key greater
// than the given key.
//
// Do not write into the returned keyOut or val byte slices. Doing so
// will cause a segfault.
func (cursor *ReadOnlyCursor) SeekGreaterThanOrEqualKey(keyIn []byte) (keyOut, val []byte, err error) {
	return cursor.moveAndGet1(setRange, keyIn)
}

// Only for DupSort. Move to the key-value pair indicated.
//
// If the exact key-value pair doesn't exist, return NotFound.
func (cursor *ReadOnlyCursor) SeekExactKeyAndValue(keyIn, valIn []byte) (err error) {
	_, err = cursor.moveAndGet2(getBoth, keyIn, valIn)
	return err
}

// Only for DupSort. Return the number values with the current key.
func (cursor *ReadOnlyCursor) Count() (count uint64, err error) {
	if atomic.LoadUint32(cursor.resizeRequired) == 1 {
		return 0, MapFull
	}
	err = asError(C.mdb_cursor_count(cursor.cursor, (*C.size_t)(&count)))
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Only for DupSort. Move to the key-value pair indicated.
//
// If the exact key-value pair doesn't exist, move to the nearest
// value in the same key greater than the given value. I.e. this will
// not move to a greater key, only a greater value.
//
// If there is no such value within the current key, return NotFound.
func (cursor *ReadOnlyCursor) SeekGreaterThanOrEqualKeyAndValue(keyIn, valIn []byte) (valOut []byte, err error) {
	return cursor.moveAndGet2(getBothRange, keyIn, valIn)
}

// Delete the key-value pair at the cursor.
//
// The only possible flag is NoDupData which is only for DupSort
// databases, and means "delete all values for the current key".
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga26a52d3efcfd72e5bf6bd6960bf75f95
func (cursor *ReadWriteCursor) Delete(flags PutFlag) error {
	return asError(C.mdb_cursor_del(cursor.cursor, C.uint(flags)))
}
