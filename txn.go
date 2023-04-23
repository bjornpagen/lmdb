package golmdb

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

// A single LMDB database can contain several top-level named
// "databases". These can be created and accessed by using the DBRef()
// method on ReadOnlyTxn and ReadWriteTxn. The DBRef is a reference to
// such a named top-level "database". They cannot be nested further,
// and you ideally only want to use a handful of these.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#gac08cad5b096925642ca359a6d6f0562a
type DBRef C.MDB_dbi

type value C.MDB_val

// this is for getting a Go-slice from memory owned by C. Go will not
// try and garbage collect it as it's memory owned by C.
func (self *value) bytesNoCopy() []byte {
	return unsafe.Slice((*byte)(self.mv_data), self.mv_size)
}

type ReadOnlyTxn struct {
	txn            *C.MDB_txn
	resizeRequired *uint32
}

// A ReadWriteTxn extends ReadOnlyTxn with methods for mutating the
// database.
type ReadWriteTxn struct {
	ReadOnlyTxn
}

// DBRef gets a reference to a named database within the LMDB. If you
// provide the flag Create then it'll be created if it doesn't already
// exist (provided you're in an Update transaction).
//
// If you call this from an Update and it succeeds, then once that txn
// commits, the DBRef can be used by other transactions (both Updates
// and Views) until it is terminated/closed.
//
// If you call this from a View and it succeeds, then the DBRef is
// only valid until the end of that View transaction.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#gac08cad5b096925642ca359a6d6f0562a
func (self *ReadOnlyTxn) DBRef(name string, flags DatabaseFlag) (DBRef, error) {
	if atomic.LoadUint32(self.resizeRequired) == 1 {
		return 0, MapFull
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	var dbRef C.MDB_dbi
	err := asError(C.mdb_dbi_open(self.txn, cName, C.uint(flags), &dbRef))
	if err != nil {
		return 0, err
	}
	return DBRef(dbRef), nil
}

// Empty the database. All key-value pairs are removed from the
// database.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#gab966fab3840fc54a6571dfb32b00f2db
func (self *ReadWriteTxn) Empty(db DBRef) error {
	return self.emptyOrDrop(db, 0)
}

// Drop the database. Not only are all key-value pairs removed from
// the database, but the database itself is removed, which means
// calling DBRef(name,0) will fail: the database will need to be
// recreated before it can be used again.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#gab966fab3840fc54a6571dfb32b00f2db
func (self *ReadWriteTxn) Drop(db DBRef) error {
	return self.emptyOrDrop(db, 1)
}

func (self *ReadWriteTxn) emptyOrDrop(db DBRef, flag C.int) error {
	return asError(C.mdb_drop(self.txn, C.MDB_dbi(db), flag))
}

// Get the value corresponding to the key from the database.
//
// The returned bytes are owned by the database. Do not modify
// them. They are valid only until a subsequent update operation, or
// the end of the transaction. If you need the value around longer
// than that, you must take a copy.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#ga8bf10cd91d3f3a83a34d04ce6b07992d
func (self *ReadOnlyTxn) Get(db DBRef, key []byte) ([]byte, error) {
	if atomic.LoadUint32(self.resizeRequired) == 1 {
		return nil, MapFull
	}
	var data value
	err := asError(C.golmdb_mdb_get(
		self.txn, C.MDB_dbi(db),
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
		(*C.MDB_val)(&data)))
	if err != nil {
		return nil, err
	}
	return data.bytesNoCopy(), nil
}

// Put a key-value pair into the database.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#ga4fa8573d9236d54687c61827ebf8cac0
func (self *ReadWriteTxn) Put(db DBRef, key, val []byte, flags PutFlag) error {
	if len(val) == 0 {
		return asError(C.golmdb_mdb_put(
			self.txn, C.MDB_dbi(db),
			(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
			nil, C.size_t(0),
			C.uint(flags)))
	} else {
		return asError(C.golmdb_mdb_put(
			self.txn, C.MDB_dbi(db),
			(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
			(*C.char)(unsafe.Pointer(&val[0])), C.size_t(len(val)),
			C.uint(flags)))
	}
}

// Delete a key-value pair from the database.
//
// The val is only necessary if you're using DupSort. If not, it's
// fine to use nil as val.
//
// See
// http://www.lmdb.tech/doc/group__mdb.html#gab8182f9360ea69ac0afd4a4eaab1ddb0
func (self *ReadWriteTxn) Delete(db DBRef, key, val []byte) error {
	if len(val) == 0 {
		return asError(C.golmdb_mdb_del(
			self.txn, C.MDB_dbi(db),
			(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
			nil, C.size_t(0)))

	} else {
		return asError(C.golmdb_mdb_del(
			self.txn, C.MDB_dbi(db),
			(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)),
			(*C.char)(unsafe.Pointer(&val[0])), C.size_t(len(val))))
	}
}
