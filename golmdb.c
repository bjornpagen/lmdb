/* golmdb.c
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
#include <lmdb.h>
#include "golmdb.h"

#define GOLMDB_SET_VAL(val, size, data) \
  *(val) = (MDB_val){.mv_size = (size), .mv_data = (data)}

int golmdb_mdb_get(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, MDB_val *val) {
  MDB_val key;
  GOLMDB_SET_VAL(&key, kn, kdata);
  return mdb_get(txn, dbi, &key, val);
}

int golmdb_mdb_put(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags) {
  MDB_val key, val;
  GOLMDB_SET_VAL(&key, kn, kdata);
  GOLMDB_SET_VAL(&val, vn, vdata);
  return mdb_put(txn, dbi, &key, &val, flags);
}

int golmdb_mdb_del(MDB_txn *txn, MDB_dbi dbi, char *kdata, size_t kn, char *vdata, size_t vn) {
  MDB_val key, val;
  GOLMDB_SET_VAL(&key, kn, kdata);
  GOLMDB_SET_VAL(&val, vn, vdata);
  return mdb_del(txn, dbi, &key, &val);
}

int golmdb_mdb_cursor_get1(MDB_cursor *cur, char *kdata, size_t kn, MDB_val *key, MDB_val *val, MDB_cursor_op op) {
  MDB_val localKey;
  int rc;
  GOLMDB_SET_VAL(&localKey, kn, kdata);
  rc = mdb_cursor_get(cur, &localKey, val, op);
  if (rc == MDB_SUCCESS) {
    if (kdata != localKey.mv_data) {
      *key = localKey;
    }
  }
  return rc;
}

int golmdb_mdb_cursor_get2(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, MDB_val *val, MDB_cursor_op op) {
  MDB_val localKey, localVal;
  int rc;
  GOLMDB_SET_VAL(&localKey, kn, kdata);
  GOLMDB_SET_VAL(&localVal, vn, vdata);
  rc = mdb_cursor_get(cur, &localKey, &localVal, op);
  if (rc == MDB_SUCCESS) {
    if (vdata != localVal.mv_data) {
      *val = localVal;
    }
  }
  return rc;
}

int golmdb_mdb_cursor_put(MDB_cursor *cur, char *kdata, size_t kn, char *vdata, size_t vn, unsigned int flags) {
  MDB_val key, val;
  GOLMDB_SET_VAL(&key, kn, kdata);
  GOLMDB_SET_VAL(&val, vn, vdata);
  return mdb_cursor_put(cur, &key, &val, flags);
}
