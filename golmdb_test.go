package golmdb_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"os"
	"testing"
	"unsafe"

	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"golang.org/x/term"

	"wellquite.org/golmdb"
)

func NewTestLogger(tb testing.TB) zerolog.Logger {
	consoleWriter := zerolog.NewConsoleWriter(zerolog.ConsoleTestWriter(tb))
	isTerminal := term.IsTerminal(int(os.Stdout.Fd()))
	consoleWriter.NoColor = !isTerminal
	return zerolog.New(consoleWriter).With().Timestamp().Logger()
}

func SetGlobalLogLevelDebug() {
	SetGlobalLogLevel(zerolog.DebugLevel)
}

func SetGlobalLogLevel(level zerolog.Level) {
	if testing.Verbose() {
		zerolog.SetGlobalLevel(level)
	} else {
		zerolog.SetGlobalLevel(zerolog.Disabled)
	}
}

func TestVersion(t *testing.T) {
	is := is.New(t)
	is.True(golmdb.Version != "")
}

func TestError(t *testing.T) {
	is := is.New(t)
	is.True(golmdb.LMDBError(golmdb.KeyExist).Error() != "")
}

func createDatabase(log zerolog.Logger, batchSize uint) (client *golmdb.LMDBClient, dir string, err error) {
	dir, err = os.MkdirTemp("", "golmdb")
	if err != nil {
		return nil, "", err
	}

	client, err = golmdb.NewLMDB(log, dir, 0666, 100, 4, golmdb.NoReadAhead, batchSize)
	if err != nil {
		return nil, "", err
	}
	return client, dir, nil
}

func createDBRef(client *golmdb.LMDBClient, name string, extraFlags golmdb.DatabaseFlag) (dbRef golmdb.DBRef, err error) {
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		dbRef, err = txn.DBRef(name, golmdb.Create|extraFlags)
		return err
	})
	return dbRef, err
}

func TestOpenClose(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)
	is := is.New(t)

	client, dir, err := createDatabase(log, 16)
	is.NoErr(err)
	defer os.RemoveAll(dir)

	client.TerminateSync()
}

func TestDBRef(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)
	is := is.New(t)

	client, dir, err := createDatabase(log, 16)
	is.NoErr(err)
	defer os.RemoveAll(dir)
	defer client.TerminateSync()

	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		_, err = txn.DBRef(t.Name(), 0) // should error because no Create flag
		return err
	})
	is.True(err == golmdb.NotFound)

	dbRef1, err := createDBRef(client, t.Name(), 0) // will exist after this
	is.NoErr(err)

	err = client.View(func(txn *golmdb.ReadOnlyTxn) (err error) {
		_, err = txn.DBRef(t.Name(), 0) // should still exist
		return err
	})
	is.NoErr(err)

	dbRef2, err := createDBRef(client, t.Name()+"2", 0) // create a 2nd database

	// now check that using the same key in both databases is distinct.
	key := []byte("myFirstKey")
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		if err = txn.Put(dbRef1, key, []byte("hello"), golmdb.NoOverwrite); err != nil {
			return err
		}
		if err = txn.Put(dbRef2, key, []byte("world"), golmdb.NoOverwrite); err != nil {
			return err
		}
		return
	})
	is.NoErr(err)

	var val1, val2 []byte
	err = client.View(func(txn *golmdb.ReadOnlyTxn) (err error) {
		if val1, err = txn.Get(dbRef1, key); err != nil {
			return err
		}
		if val2, err = txn.Get(dbRef2, key); err != nil {
			return err
		}
		return
	})
	is.NoErr(err)
	is.True(bytes.Equal(val1, []byte("hello")))
	is.True(bytes.Equal(val2, []byte("world")))
}

func TestEmptyDrop(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)
	is := is.New(t)

	client, dir, err := createDatabase(log, 16)
	is.NoErr(err)
	defer os.RemoveAll(dir)
	defer client.TerminateSync()

	key := []byte(`hello`)
	value := []byte(`world`)
	err = client.Update(func(txn *golmdb.ReadWriteTxn) error {
		dbRef, err := txn.DBRef(t.Name(), golmdb.Create)
		if err != nil {
			return err
		}
		if err = txn.Put(dbRef, key, value, golmdb.NoOverwrite); err != nil {
			return err
		}
		return nil
	})
	is.NoErr(err)

	// empty
	err = client.Update(func(txn *golmdb.ReadWriteTxn) error {
		dbRef, err := txn.DBRef(t.Name(), 0) // should still be there
		if err != nil {
			return err
		}
		if vals, err := txn.Get(dbRef, key); err != nil {
			return err
		} else if !bytes.Equal(value, vals) {
			return fmt.Errorf("Fetched value did not match expectation.")
		}
		if err = txn.Empty(dbRef); err != nil {
			return err
		}
		if _, err := txn.Get(dbRef, key); err == nil {
			return fmt.Errorf("Key-value pair still exists after emptying db.")
		} else if err == golmdb.NotFound {
			return nil
		} else {
			return err
		}
	})
	is.NoErr(err)

	// drop
	err = client.Update(func(txn *golmdb.ReadWriteTxn) error {
		dbRef, err := txn.DBRef(t.Name(), 0) // should still be there
		if err != nil {
			return err
		}
		if _, err := txn.Get(dbRef, key); err == nil {
			return fmt.Errorf("Key-value pair still exists after emptying db.")
		} else if err != golmdb.NotFound {
			return err
		}
		if err = txn.Drop(dbRef); err != nil {
			return err
		}
		dbRef, err = txn.DBRef(t.Name(), 0) // should still be there
		if err == nil {
			return fmt.Errorf("Should have got error when trying to refresh dbRef after drop")
		}
		return nil
	})
	is.NoErr((err))
}

func TestWriteReadDelete(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)
	is := is.New(t)

	client, dir, err := createDatabase(log, 16)
	is.NoErr(err)
	defer os.RemoveAll(dir)
	defer client.TerminateSync()

	dbRef, err := createDBRef(client, t.Name(), 0)
	is.NoErr(err)

	key := make([]byte, 8)

	// write some key-value pairs
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		for idx := 0; idx < 64; idx++ {
			binary.BigEndian.PutUint64(key, uint64(idx))
			if err = txn.Put(dbRef, key, key, golmdb.NoOverwrite); err != nil {
				return err
			}
		}
		return
	})
	is.NoErr(err)

	// check we can read them back
	err = client.View(func(txn *golmdb.ReadOnlyTxn) (err error) {
		for idx := 0; idx < 64; idx++ {
			binary.BigEndian.PutUint64(key, uint64(idx))
			val, err := txn.Get(dbRef, key)
			if err != nil {
				return err
			} else if val == nil {
				return fmt.Errorf("For key %d, got nil value and nil error", idx)
			} else if len(val) != len(key) {
				return fmt.Errorf("For key %d, got value of length %d", idx, len(val))
			} else if uintptr(unsafe.Pointer(&key[0])) == uintptr(unsafe.Pointer(&val[0])) {
				return fmt.Errorf("For key %d, key and returned value share the same underlying array!", idx)
			} else if !bytes.Equal(key, val) {
				return fmt.Errorf("For key %d, key and value are not equal but should be", idx)
			}
		}
		return
	})
	is.NoErr(err)

	// attempt to rewrite, but this should fail due to NoOverwrite
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		idx := 3
		binary.BigEndian.PutUint64(key, uint64(idx))
		return txn.Put(dbRef, key, key, golmdb.NoOverwrite)
	})
	is.True(err == golmdb.KeyExist)

	val := make([]byte, 8)
	// rewrite them all
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		for idx := 0; idx < 64; idx++ {
			binary.BigEndian.PutUint64(key, uint64(idx))
			binary.BigEndian.PutUint64(val, uint64(idx*2))
			if err = txn.Put(dbRef, key, val, 0); err != nil {
				return err
			}
		}
		return
	})
	is.NoErr(err)

	valExpected := make([]byte, 8)
	// check we can read them back
	err = client.View(func(txn *golmdb.ReadOnlyTxn) (err error) {
		for idx := 0; idx < 64; idx++ {
			binary.BigEndian.PutUint64(key, uint64(idx))
			binary.BigEndian.PutUint64(valExpected, uint64(idx*2))
			val, err := txn.Get(dbRef, key)
			if err != nil {
				return err
			} else if val == nil {
				return fmt.Errorf("For key %d, got nil value and nil error", idx)
			} else if len(val) != len(key) {
				return fmt.Errorf("For key %d, got value of length %d", idx, len(val))
			} else if uintptr(unsafe.Pointer(&key[0])) == uintptr(unsafe.Pointer(&val[0])) {
				return fmt.Errorf("For key %d, key and returned value share the same underlying array!", idx)
			} else if !bytes.Equal(valExpected, val) {
				return fmt.Errorf("For key %d, value is not equal to the expected value", idx)
			}
		}
		return
	})
	is.NoErr(err)

	// now delete every other
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		for idx := 0; idx < 64; idx += 2 {
			binary.BigEndian.PutUint64(key, uint64(idx))
			if err = txn.Delete(dbRef, key, nil); err != nil {
				return err
			}
		}
		return
	})
	is.NoErr(err)

	// check the deletes were correct
	err = client.View(func(txn *golmdb.ReadOnlyTxn) (err error) {
		for idx := 0; idx < 64; idx++ {
			binary.BigEndian.PutUint64(key, uint64(idx))
			binary.BigEndian.PutUint64(valExpected, uint64(idx*2))
			val, err := txn.Get(dbRef, key)

			if idx%2 == 0 { // should be deleted
				if err == golmdb.NotFound {
					err = nil
					continue
				} else {
					return fmt.Errorf("For key %d, expected NotFound, but got %v error", idx, err)
				}

			} else {
				if err != nil {
					return err
				} else if val == nil {
					return fmt.Errorf("For key %d, got nil value and nil error", idx)
				} else if len(val) != len(key) {
					return fmt.Errorf("For key %d, got value of length %d", idx, len(val))
				} else if uintptr(unsafe.Pointer(&key[0])) == uintptr(unsafe.Pointer(&val[0])) {
					return fmt.Errorf("For key %d, key and returned value share the same underlying array!", idx)
				} else if !bytes.Equal(valExpected, val) {
					return fmt.Errorf("For key %d, value is not equal to the expected value", idx)
				}
			}
		}
		return
	})
	is.NoErr(err)

	// now close the database and reopen it readonly
	client.TerminateSync()
	client2, err := golmdb.NewLMDB(log, dir, 0666, 16, 4, golmdb.NoReadAhead|golmdb.ReadOnly, 16)
	is.NoErr(err)
	defer client2.TerminateSync()

	// check it's all still there.
	err = client2.View(func(txn *golmdb.ReadOnlyTxn) (err error) {
		dbRef, err := txn.DBRef(t.Name(), 0)
		if err != nil {
			return err
		}
		for idx := 0; idx < 64; idx++ {
			binary.BigEndian.PutUint64(key, uint64(idx))
			binary.BigEndian.PutUint64(valExpected, uint64(idx*2))
			val, err := txn.Get(dbRef, key)

			if idx%2 == 0 { // should be deleted
				if err == golmdb.NotFound {
					err = nil
					continue
				} else {
					return fmt.Errorf("For key %d, expected NotFound, but got %v error", idx, err)
				}

			} else {
				if err != nil {
					return err
				} else if val == nil {
					return fmt.Errorf("For key %d, got nil value and nil error", idx)
				} else if len(val) != len(key) {
					return fmt.Errorf("For key %d, got value of length %d", idx, len(val))
				} else if uintptr(unsafe.Pointer(&key[0])) == uintptr(unsafe.Pointer(&val[0])) {
					return fmt.Errorf("For key %d, key and returned value share the same underlying array!", idx)
				} else if !bytes.Equal(valExpected, val) {
					return fmt.Errorf("For key %d, value is not equal to the expected value", idx)
				}
			}
		}
		return
	})
	is.NoErr(err)

	// check any attempt to Update a readonly gets an error:
	err = client2.Update(func(txn *golmdb.ReadWriteTxn) (err error) { return nil })
	is.True(err != nil)
}

func expectCursor(fun func() (key, val []byte, err error), expectedKeyNum, expectedValNum uint64, expectedErr error) error {
	expectedKey := make([]byte, 8)
	expectedVal := make([]byte, 8)
	binary.BigEndian.PutUint64(expectedKey, expectedKeyNum)
	binary.BigEndian.PutUint64(expectedVal, expectedValNum)

	gotKey, gotVal, gotErr := fun()

	if expectedErr != gotErr {
		return fmt.Errorf("Expected error %v but got %v", expectedErr, gotErr)
	}
	if gotErr != nil {
		return nil
	}
	if (gotKey == nil) != (expectedKey == nil) {
		return fmt.Errorf("Expected key to be nil? %v, but got key isn't", expectedKey == nil)
	} else if (gotVal == nil) != (expectedVal == nil) {
		return fmt.Errorf("Expected val to be nil? %v, but got key isn't", expectedVal == nil)
	} else if !bytes.Equal(expectedKey, gotKey) {
		return errors.New("expected key and got key differ")
	} else if !bytes.Equal(expectedVal, gotVal) {
		return errors.New("expected val and got val differ")
	} else {
		return nil
	}
}

func TestCursor(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)
	is := is.New(t)

	client, dir, err := createDatabase(log, 16)
	is.NoErr(err)
	defer os.RemoveAll(dir)
	defer client.TerminateSync()

	dbRef, err := createDBRef(client, t.Name(), 0)
	is.NoErr(err)

	key := make([]byte, 8)
	val := make([]byte, 8)

	notFoundFuns := []func(*golmdb.ReadOnlyCursor) (key, val []byte, err error){
		(*golmdb.ReadOnlyCursor).First,
		(*golmdb.ReadOnlyCursor).Last,
		(*golmdb.ReadOnlyCursor).Next,
		(*golmdb.ReadOnlyCursor).Prev,
	}
	for _, fun := range notFoundFuns {
		err = client.View(func(txn *golmdb.ReadOnlyTxn) (err error) {
			cursor, err := txn.NewCursor(dbRef)
			if err != nil {
				return err
			}
			defer cursor.Close()
			key, val, err := fun(cursor)
			if err == nil {
				return errors.New("expected NotFound")
			} else if key != nil {
				return errors.New("expected nil key")
			} else if val != nil {
				return errors.New("expected nil val")
			}
			return err
		})
		is.True(err == golmdb.NotFound)
	}

	// write some key-value pairs
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		for idx := 0; idx < 64; idx++ {
			binary.BigEndian.PutUint64(key, uint64(idx))
			binary.BigEndian.PutUint64(val, uint64(63-idx))
			if err = txn.Put(dbRef, key, val, golmdb.NoOverwrite); err != nil {
				return err
			}
		}
		return
	})
	is.NoErr(err)

	// check behaviour of first / last / next / prev / current
	err = client.View(func(txn *golmdb.ReadOnlyTxn) (err error) {
		cursor, err := txn.NewCursor(dbRef)
		if err != nil {
			return err
		}
		defer cursor.Close()

		if err = expectCursor(cursor.First, 0, 63, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Current, 0, 63, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Prev, 0, 0, golmdb.NotFound); err != nil {
			return err
		}
		if err = expectCursor(cursor.Current, 0, 63, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Next, 1, 62, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Next, 2, 61, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Prev, 1, 62, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Current, 1, 62, nil); err != nil {
			return err
		}

		if err = expectCursor(cursor.Last, 63, 0, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Current, 63, 0, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Next, 0, 0, golmdb.NotFound); err != nil {
			return err
		}
		if err = expectCursor(cursor.Current, 63, 0, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Prev, 62, 1, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Prev, 61, 2, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Next, 62, 1, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Current, 62, 1, nil); err != nil {
			return err
		}
		return err
	})
	is.NoErr(err)

	// check Seek and SeekGreaterThanOrEqual
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		cursor, err := txn.NewCursor(dbRef)
		if err != nil {
			return err
		}
		defer cursor.Close()

		binary.BigEndian.PutUint64(key, 37)
		val, err = cursor.SeekExactKey(key)
		if err != nil {
			return err
		} else if len(val) != 8 {
			return errors.New("wrong length for val")
		} else if binary.BigEndian.Uint64(val) != 26 {
			return errors.New("wrong value for val")
		}

		keyOut, val, err := cursor.SeekGreaterThanOrEqualKey(key)
		if err != nil {
			return err
		} else if len(val) != 8 {
			return errors.New("wrong length for val")
		} else if len(keyOut) != 8 {
			return errors.New("wrong length for key")
		} else if binary.BigEndian.Uint64(val) != 26 {
			return errors.New("wrong value for val")
		} else if uintptr(unsafe.Pointer(&key[0])) == uintptr(unsafe.Pointer(&keyOut[0])) {
			return errors.New("key and returned key share the same underlying array!") // which means the cgo contract is being violated
		} else if !bytes.Equal(key, keyOut) {
			return errors.New("key does not equal returned key")
		}

		binary.BigEndian.PutUint64(key, 137)
		val, err = cursor.SeekExactKey(key)
		if val != nil {
			return errors.New("Expected nil val")
		} else if err != golmdb.NotFound {
			return fmt.Errorf("Expected NotFound err. Got %v", err)
		} else {
			err = nil
		}

		// should still get NotFound from SeekGreaterThanOrEqual
		keyOut, val, err = cursor.SeekGreaterThanOrEqualKey(key)
		if val != nil {
			return errors.New("Expected nil val")
		} else if keyOut != nil {
			return errors.New("Expected nil returned key")
		} else if err != golmdb.NotFound {
			return fmt.Errorf("Expected NotFound err. Got %v", err)
		} else {
			err = nil
		}

		binary.BigEndian.PutUint64(key, 18)
		_, err = cursor.SeekExactKey(key)
		if err != nil {
			return err
		} else if err = cursor.Delete(0); err != nil {
			return err
		}

		keyOut, val, err = cursor.SeekGreaterThanOrEqualKey(key)
		if err != nil {
			return err
		} else if len(val) != 8 {
			return errors.New("wrong length for val")
		} else if len(keyOut) != 8 {
			return errors.New("wrong length for key")
		} else if binary.BigEndian.Uint64(val) != 44 {
			return errors.New("wrong value for val")
		} else if uintptr(unsafe.Pointer(&key[0])) == uintptr(unsafe.Pointer(&keyOut[0])) {
			return errors.New("key and returned key share the same underlying array!") // which means the cgo contract is being violated
		} else if bytes.Equal(key, keyOut) {
			return errors.New("key should not equal returned key")
		} else if binary.BigEndian.Uint64(keyOut) != 19 {
			return errors.New("wrong value for returned key")
		}

		return err
	})
	is.NoErr(err)
}

func TestResize(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)
	is := is.New(t)

	client, dir, err := createDatabase(log, 16)
	is.NoErr(err)
	defer os.RemoveAll(dir)
	defer client.TerminateSync()

	dbRef, err := createDBRef(client, t.Name(), 0)
	is.NoErr(err)

	// it should start at 1MB, so just write 100MB to it. Do it in 2
	// stages: 100 writes of 0.5MB, and then 25 writes of 2MB.

	key := make([]byte, 8)
	size1 := 512 * 1024
	size2 := 2 * 1024 * 1024
	val := make([]byte, size1)

	for idx := 0; idx < 100; idx++ {
		binary.BigEndian.PutUint64(key, uint64(idx))
		err = client.Update(func(txn *golmdb.ReadWriteTxn) error {
			return txn.Put(dbRef, key, val, 0)
		})
		is.NoErr(err)
	}

	val = make([]byte, size2)
	for idx := 100; idx < 125; idx++ {
		binary.BigEndian.PutUint64(key, uint64(idx))
		err = client.Update(func(txn *golmdb.ReadWriteTxn) error {
			return txn.Put(dbRef, key, val, 0)
		})
		is.NoErr(err)
	}

	// now have a quick read through them all
	err = client.View(func(txn *golmdb.ReadOnlyTxn) error {
		for idx := 0; idx < 125; idx++ {
			binary.BigEndian.PutUint64(key, uint64(idx))
			val, err = txn.Get(dbRef, key)
			if err != nil {
				return err
			}
			if idx < 100 {
				if len(val) != size1 {
					return fmt.Errorf("Expected %d bytes. Got %d", size1, len(val))
				}
			} else {
				if len(val) != size2 {
					return fmt.Errorf("Expected %d bytes. Got %d", size2, len(val))
				}
			}
		}
		return nil
	})
	is.NoErr(err)
}

func TestDupDB(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)
	is := is.New(t)

	client, dir, err := createDatabase(log, 16)
	is.NoErr(err)
	defer os.RemoveAll(dir)
	defer client.TerminateSync()

	dbRef, err := createDBRef(client, t.Name(), golmdb.DupSort)
	is.NoErr(err)

	key := make([]byte, 8)
	val := make([]byte, 8)

	// write some key-value pairs
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		for idx := 0; idx < 64; idx++ {
			// for each key, we write key values
			binary.BigEndian.PutUint64(key, uint64(idx))
			for idy := 0; idy < idx; idy++ {
				binary.BigEndian.PutUint64(val, uint64(idy))
				if err = txn.Put(dbRef, key, val, golmdb.NoDupData); err != nil {
					return err
				}
				// Put it twice, just so that we know for sure that we
				// don't get duplicated key-value pairs. I.e. NoDupData is
				// somewhat misnamed - data is never duplicated!
				if err = txn.Put(dbRef, key, val, 0); err != nil {
					return err
				}
				if err = txn.Put(dbRef, key, val, golmdb.NoDupData); err == golmdb.KeyExist {
					err = nil
				} else {
					return fmt.Errorf("Expected KeyExist error, but got %v", err)
				}
			}
		}
		return
	})
	is.NoErr(err)

	// check prev and next etc
	err = client.Update(func(txn *golmdb.ReadWriteTxn) (err error) {
		cursor, err := txn.NewCursor(dbRef)
		if err != nil {
			return err
		}
		defer cursor.Close()

		if err = expectCursor(cursor.First, 1, 0, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Next, 2, 0, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Next, 2, 1, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Next, 3, 0, nil); err != nil {
			return err
		}
		if count, err := cursor.Count(); err != nil {
			return err
		} else if count != 3 {
			return fmt.Errorf("Expected 3 values. Got %d", count)
		}
		if err = expectCursor(cursor.NextInSameKey, 3, 1, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.NextInSameKey, 3, 2, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.NextInSameKey, 0, 0, golmdb.NotFound); err != nil {
			return err
		}
		if err = expectCursor(cursor.Next, 4, 0, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.NextKey, 5, 0, nil); err != nil {
			return err
		}

		if err = expectCursor(cursor.PrevInSameKey, 0, 0, golmdb.NotFound); err != nil {
			return err
		}
		if err = expectCursor(cursor.PrevKey, 4, 3, err); err != nil {
			return err
		}
		if err = expectCursor(cursor.PrevKey, 3, 2, err); err != nil {
			return err
		}
		if err = expectCursor(cursor.PrevInSameKey, 3, 1, err); err != nil {
			return err
		}
		if err = expectCursor(cursor.Prev, 3, 0, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Prev, 2, 1, nil); err != nil {
			return err
		}

		if err = expectCursor(cursor.Last, 63, 62, nil); err != nil {
			return err
		}

		binary.BigEndian.PutUint64(key, 32)
		val, err = cursor.SeekExactKey(key)
		if err != nil {
			return err
		} else if len(val) != 8 {
			return errors.New("wrong length for val")
		} else if binary.BigEndian.Uint64(val) != 0 {
			return errors.New("wrong value for val")
		}

		if err = cursor.Delete(0); err != nil { // should just remove the 0 value
			return err
		}

		val, err = cursor.SeekExactKey(key)
		if err != nil {
			return err
		} else if len(val) != 8 {
			return errors.New("wrong length for val")
		} else if binary.BigEndian.Uint64(val) != 1 { // 0 is gone, so we should be at 1
			return errors.New("wrong value for val")
		}

		if err = cursor.Delete(golmdb.NoDupData); err != nil { // delete all values
			return err
		}

		_, err = cursor.SeekExactKey(key)
		if err != golmdb.NotFound {
			return fmt.Errorf("Wrong error: expected NotFound, got %v", err)
		} else {
			err = nil
		}

		keyOut, val, err := cursor.SeekGreaterThanOrEqualKey(key)
		if err != nil {
			return err
		} else if len(val) != 8 {
			return errors.New("wrong length for val")
		} else if len(keyOut) != 8 {
			return errors.New("wrong length for key")
		} else if binary.BigEndian.Uint64(val) != 0 {
			return errors.New("wrong value for val")
		} else if uintptr(unsafe.Pointer(&key[0])) == uintptr(unsafe.Pointer(&keyOut[0])) {
			return errors.New("key and returned key share the same underlying array!") // which means the cgo contract is being violated
		} else if bytes.Equal(key, keyOut) {
			return errors.New("key should not equal returned key")
		} else if binary.BigEndian.Uint64(keyOut) != 33 {
			return errors.New("wrong value for returned key")
		}

		if err = expectCursor(cursor.Prev, 31, 30, nil); err != nil {
			return err
		}
		if err = expectCursor(cursor.Prev, 31, 29, nil); err != nil {
			return err
		}

		if err = cursor.Delete(0); err != nil {
			return err
		}

		binary.BigEndian.PutUint64(key, 31)
		val = make([]byte, 8)
		binary.BigEndian.PutUint64(val, 3)
		err = cursor.SeekExactKeyAndValue(key, val)
		if err != nil {
			return err
		}
		binary.BigEndian.PutUint64(val, 29)
		err = cursor.SeekExactKeyAndValue(key, val)
		if err != golmdb.NotFound {
			return fmt.Errorf("Wrong error: expected NotFonud, got %v", err)
		} else {
			err = nil
		}

		valOut, err := cursor.SeekGreaterThanOrEqualKeyAndValue(key, val)
		if err != nil {
			return err
		} else if len(valOut) != 8 {
			return errors.New("wrong length for val")
		} else if uintptr(unsafe.Pointer(&val[0])) == uintptr(unsafe.Pointer(&valOut[0])) {
			return errors.New("val and returned val share the same underlying array!") // which means the cgo contract is being violated
		} else if binary.BigEndian.Uint64(valOut) != 30 {
			return errors.New("wrong value for val")
		}

		// we deleted all of 32. So we should get a not found.
		binary.BigEndian.PutUint64(key, 32)
		binary.BigEndian.PutUint64(val, 0)
		_, err = cursor.SeekGreaterThanOrEqualKeyAndValue(key, val)
		if err != golmdb.NotFound {
			return fmt.Errorf("Wrong error: expected NotFonud, got %v", err)
		} else {
			err = nil
		}

		if err = expectCursor(cursor.Prev, 31, 30, nil); err != nil {
			return err
		}

		binary.BigEndian.PutUint64(key, 31)
		err = txn.Delete(dbRef, key, val)
		if err != nil {
			return err
		}

		val, err = cursor.FirstInSameKey()
		if err != nil {
			return err
		} else if len(val) != 8 {
			return errors.New("wrong length for val")
		} else if binary.BigEndian.Uint64(val) != 1 {
			return errors.New("wrong value for val")
		}

		val, err = cursor.LastInSameKey()
		if err != nil {
			return err
		} else if len(val) != 8 {
			return errors.New("wrong length for val")
		} else if binary.BigEndian.Uint64(val) != 30 {
			return errors.New("wrong value for val")
		}

		return
	})
	is.NoErr(err)
}

func TestLexicalSort(t *testing.T) {
	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)
	is := is.New(t)

	client, dir, err := createDatabase(log, 16)
	is.NoErr(err)
	defer os.RemoveAll(dir)
	defer client.TerminateSync()

	dbRef, err := createDBRef(client, t.Name(), golmdb.DupSort)
	is.NoErr(err)

	foo := []byte("foo")
	foo0 := append(foo, 0x00)
	foo1 := append(foo, 0x01)

	bar := []byte("bar")
	bar0 := append(bar, 0x00)
	bar1 := append(bar, 0x01)

	err = client.Update(func(rwtxn *golmdb.ReadWriteTxn) (err error) {
		for _, key := range [][]byte{foo, foo0, foo1, bar, bar0, bar1} {
			if err = rwtxn.Put(dbRef, key, nil, 0); err != nil {
				return err
			}
		}
		return err
	})
	is.NoErr(err)

	err = client.View(func(rotxn *golmdb.ReadOnlyTxn) (err error) {
		cursor, err := rotxn.NewCursor(dbRef)
		if err != nil {
			return err
		}
		defer cursor.Close()

		if key, _, err := cursor.First(); err != nil {
			return err
		} else if !bytes.Equal(key, bar) {
			return errors.New("Wrong first key")
		}

		if key, _, err := cursor.Next(); err != nil {
			return err
		} else if !bytes.Equal(key, bar0) {
			return errors.New("Wrong first-next key")
		}

		if key, _, err := cursor.Last(); err != nil {
			return err
		} else if !bytes.Equal(key, foo1) {
			return errors.New("Wrong last key")
		}

		if key, _, err := cursor.Prev(); err != nil {
			return err
		} else if !bytes.Equal(key, foo0) {
			return errors.New("Wrong last-prev key")
		}

		return
	})
	is.NoErr(err)

	findLastKeyWithPrefix := func(prefix []byte) (lastKey []byte, err error) {
		// We want to calculate the "next" prefix, and then go one back from there. This should work because of the lexical sorting.
		nextKeyInt := new(big.Int).SetBytes(prefix)
		nextKeyInt.Add(nextKeyInt, big.NewInt(1))
		nextKeyBytes := nextKeyInt.Bytes()

		err = client.View(func(rotxn *golmdb.ReadOnlyTxn) (err error) {
			cursor, err := rotxn.NewCursor(dbRef)
			if err != nil {
				return err
			}
			defer cursor.Close()

			// if it got longer then the next number used a new
			// column. Which means the prefix is all 0xff, so there is no
			// "next", so just go straight to the last key-value pair in
			// the database.
			gotoLast := len(nextKeyBytes) > len(foo)

			if !gotoLast {
				_, _, err := cursor.SeekGreaterThanOrEqualKey(nextKeyBytes)
				if err == golmdb.NotFound { // there is nothing after
					gotoLast = true
					err = nil
				} else if err != nil {
					return err
				} else if lastKey, _, err = cursor.Prev(); err != nil {
					return err
				}
			}
			if gotoLast {
				if lastKey, _, err = cursor.Last(); err != nil {
					return err
				}
			}
			return
		})
		if err != nil {
			return nil, err
		}
		return lastKey, nil
	}

	key, err := findLastKeyWithPrefix(foo)
	is.NoErr(err)
	is.True(bytes.Equal(key, foo1))

	key, err = findLastKeyWithPrefix(bar)
	is.NoErr(err)
	is.True(bytes.Equal(key, bar1))
}
