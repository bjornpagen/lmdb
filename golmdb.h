/* golmdb.h
 *
 * This code was originally written by, and is copyright, Bryan
 * Matsuo, at
 * https://github.com/bmatsuo/lmdb-go/blob/master/lmdb/lmdbgo.c and
 * licensed under BSD 3-clause.
 *
 * I have made some changes to cursor_get so that they obey
 * the cgo contract and do not copy go pointers into other go
 * pointers. Those areas, copyright Matthew Sackman.
 */
#ifndef _GOLMDB_H_
#define _GOLMDB_H_

#include <lmdb.h>

/* Proxy functions for lmdb get/put operations. The functions are defined to
 * take char* values instead of void* to keep cgo from cheking their data for
 * nested pointers and causing a couple of allocations per argument.
 *
 * For more information see github issues for more information about the
 * problem and the decision.
 *      https://github.com/golang/go/issues/14387
 *      https://github.com/golang/go/issues/15048
 *      https://github.com/bmatsuo/lmdb-go/issues/63
 * */
int golmdb_mdb_get(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, MDB_val *val);
int golmdb_mdb_put(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags);
int golmdb_mdb_del(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn);
int golmdb_mdb_cursor_get1(MDB_cursor *cur, char *kdata, size_t kn, MDB_val *key, MDB_val *val, MDB_cursor_op op);
int golmdb_mdb_cursor_get2(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, MDB_val *val, MDB_cursor_op op);
int golmdb_mdb_cursor_put(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags);

#endif
