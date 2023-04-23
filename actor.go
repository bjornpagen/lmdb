package lmdb

/*
#include <lmdb.h>
*/
import "C"
import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"wellquite.org/actors"
	"wellquite.org/actors/mailbox"
)

type readWriteTxnMsg struct {
	actors.MsgSyncBase
	txnFun func(*ReadWriteTxn) error // input
	err    error                     // output
}

func readOnlyLMDBClient(environment *environment) *Client {
	environment.readOnly = true
	resizeRequired := uint32(0)
	return &Client{
		environment:    environment,
		resizeRequired: &resizeRequired,
	}
}

func spawnLMDBActor(manager actors.ManagerClient, log *zerolog.Logger, environment *environment, batchSize uint) (*Client, error) {
	server := &server{
		batchSize:    int(batchSize),
		environment:  environment,
		resizingLock: new(sync.RWMutex),
	}

	var err error
	var clientBase *actors.ClientBase
	if manager == nil {
		clientBase, err = actors.Spawn(*log, server, "golmdb")
	} else {
		clientBase, err = manager.Spawn(server, "golmdb")
	}
	if err != nil {
		return nil, err
	}

	return &Client{
		ClientBase:     clientBase,
		environment:    environment,
		resizingLock:   server.resizingLock,
		resizeRequired: &server.resizeRequired,
		readWriteTxnMsgPool: &sync.Pool{
			New: func() interface{} {
				return &readWriteTxnMsg{}
			},
		},
	}, nil
}

// --- Client side API ---

// A client to the whole LMDB database. The client allows you to run
// Views (read-only transactions), Updates (read-write transactions),
// and close/terminate the database. A single client is safe for any
// number of go-routines to use concurrently.
type Client struct {
	*actors.ClientBase
	environment         *environment
	resizingLock        *sync.RWMutex
	resizeRequired      *uint32
	readWriteTxnMsgPool *sync.Pool
}

var _ actors.Client = (*Client)(nil)

// Run a View: a read-only transaction. The fun will be run in the
// current go-routine. Multiple concurrent calls to View can proceed
// concurrently. If there are write transactions going on
// concurrently, they may cause MapFull excetptions. If this happens,
// all read transactions will be interrupted and aborted, and will
// automatically be restarted.
//
// As this is a read-only transaction, the transaction is aborted no
// matter what the fun returns. The error that the fun returns is
// returned from this method.
//
// Nested transactions are not supported.
func (c *Client) View(fun func(rotxn *ReadOnlyTxn) error) (err error) {
	if !c.environment.readOnly {
		c.resizingLock.RLock()
		defer c.resizingLock.RUnlock()
	}

	txn, err := c.environment.txnBegin(true, nil)
	if err != nil {
		return err
	}
	readOnlyTxn := ReadOnlyTxn{
		txn:            txn,
		resizeRequired: c.resizeRequired,
	}
	// use a defer as it'll run even on a panic
	defer C.mdb_txn_abort(txn)
	for {
		err := fun(&readOnlyTxn)
		if err == MapFull {
			// unlock and wait to relock so that the server can resize.
			c.resizingLock.RUnlock()
			c.resizingLock.RLock()
			continue
		}
		return err
	}
}

// Run an Update: a read-write transaction. The fun will not be run in
// the current go-routine.
//
// If the fun returns a nil error, then the transaction will be
// committed. If the fun returns any non-nil error then the
// transaction will be aborted. Any non-nil error returned by the fun
// is returned from this method.
//
// If the fun is run and returns a nil error, then it may still be run
// more than once. In this case, its transaction will be aborted (and
// a fresh transaction created), before it is re-run. I.e. the fun
// will never see the state of the database *after* it has already
// been run.
//
// If the fun is run and returns a non-nil error then it will not be
// re-run.
//
// Only a single Update transaction can run at a time; golmdb will
// manage this for you. An Update transaction can proceed concurrently
// with one or more View transactions.
//
// Nested transactions are not supported.
func (c *Client) Update(fun func(rwtxn *ReadWriteTxn) error) error {
	if c.environment.readOnly {
		return errors.New("cannot update: LMDB has been opened in ReadOnly mode")
	}

	msg := c.readWriteTxnMsgPool.Get().(*readWriteTxnMsg)
	msg.txnFun = fun

	if c.SendSync(msg, true) {
		err := msg.err
		c.readWriteTxnMsgPool.Put(msg)
		return err
	} else {
		c.readWriteTxnMsgPool.Put(msg)
		return errors.New("golmdb server is terminated")
	}
}

// Terminates the actor for Update transactions (if it's running), and
// then shuts down the LMDB database.
//
// You must make sure that all concurrently running transactions have
// finished before you call this method: this method will not wait for
// concurrent View transactions to finish (or prevent new ones from
// starting), and it will not wait for calls to Update to complete.
// It is your responsibility to make sure all users of the client are
// finished and shutdown before calling TerminateSync.
//
// Note that this does not call mdb_env_sync. So if you've opened the
// database with NoSync or NoMetaSync or MapAsync then you will need
// to call Sync() before TerminateSync(); the Sync in TerminateSync
// merely refers to the fact this method is synchronous - it'll only
// return once the actor has fully terminated and the LMDB database
// has been closed.
func (c *Client) TerminateSync() {
	if !c.environment.readOnly {
		c.ClientBase.TerminateSync()
	}
	c.environment.close()
}

// Manually sync the database to disk.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga85e61f05aa68b520cc6c3b981dba5037
//
// Unless you're using MapAsync or NoSync or NoMetaSync flags when
// opening the LMDB database, you do not need to worry about calling
// this. If you are using any of those flags then LMDB will not be
// syncing data to disk on every transaction commit, which raises the
// possibility of data loss or corruption in the event of a crash or
// unexpected exit. Nevertheless, those flags are sometimes useful,
// for example when rapidly loading a data set into the database. An
// explicit call to Sync is then needed to flush everything through
// onto disk.
func (c *Client) Sync(force bool) error {
	return c.environment.sync(force)
}

// Copy the entire database to a new path, optionally compacting it.
//
// See http://www.lmdb.tech/doc/group__mdb.html#ga3bf50d7793b36aaddf6b481a44e24244
//
// This can be done with the database in use: it allows you to take
// backups of the dataset without stopping anything. However, as the
// docs note, this is essentially a read-only transaction to read the
// entire database and copy it out. If that takes a long time (because
// it's a large database) and there are updates to the database going
// on at the same time, then the original database can grow in size
// due to needing to keep the old data around so that the read-only
// transaction doing the copy sees a consistent snapshot of the entire
// database.
func (c *Client) Copy(path string, compact bool) error {
	return c.environment.copy(path, compact)
}

// --- Server side ---

type server struct {
	actors.ServerBase

	batchSize      int
	batch          []*readWriteTxnMsg
	resizingLock   *sync.RWMutex
	resizeRequired uint32
	environment    *environment
	readWriteTxn   ReadWriteTxn
}

var _ actors.Server = (*server)(nil)

func (s *server) Init(log zerolog.Logger, mailboxReader *mailbox.MailboxReader, selfClient *actors.ClientBase) (err error) {
	// this is required for the writer - even though we use NoTLS
	runtime.LockOSThread()
	readWriteTxn := &s.readWriteTxn
	readWriteTxn.resizeRequired = &s.resizeRequired
	return s.ServerBase.Init(log, mailboxReader, selfClient)
}

func (s *server) HandleMsg(msg mailbox.Msg) error {
	switch msgT := msg.(type) {
	case *readWriteTxnMsg:
		s.batch = append(s.batch, msgT)
		if len(s.batch) == s.batchSize || s.MailboxReader.IsEmpty() {
			batch := s.batch
			s.batch = s.batch[:0]
			if s.Log.Trace().Enabled() {
				s.Log.Trace().Int("batch size", len(batch)).Msg("running batch")
			}
			return s.runBatch(batch)
		}
		return nil

	default:
		return s.ServerBase.HandleMsg(msg)
	}
}

func (s *server) runBatch(batch []*readWriteTxnMsg) error {
	switch batchLen := len(batch); batchLen {
	case 0:
		return nil

	case 1:
		msg := batch[0]
		for {
			txnErr, fatalErr := s.runAndCommitWriteTxnMsg(batch, nil, msg)
			if fatalErr != nil {
				markBatchProcessed(batch, fatalErr)
				return fatalErr
			}

			if txnErr == MapFull {
				// MapFull can come either from a Put, or from a Commit. We
				// need to increase the size, and then re-run the txn.
				fatalErr = s.increaseSize()
				if fatalErr == nil {
					continue
				} else {
					markBatchProcessed(batch, fatalErr)
					return fatalErr
				}
			}

			markBatchProcessed(batch, txnErr)
			return nil
		}

	default:
		for batchLen > 0 {
			outerTxn, outerErr := s.environment.txnBegin(false, nil)
			if outerErr != nil {
				// if we can't even create the txn, that's fatal to the whole system
				markBatchProcessed(batch, outerErr)
				return outerErr
			}

			for idx, msg := range batch {
				if msg == nil {
					continue
				}

				innerTxnErr, innerFatalErr := s.runAndCommitWriteTxnMsg(batch, outerTxn, msg)
				if innerFatalErr != nil {
					markBatchProcessed(batch, innerFatalErr)
					return innerFatalErr
				}

				if innerTxnErr == MapFull || innerTxnErr == TxnFull {
					outerErr = innerTxnErr
					break

				} else if innerTxnErr != nil {
					msg.err = innerTxnErr
					msg.MarkProcessed()
					batch[idx] = nil
					batchLen -= 1
				}
			}

			if outerErr == nil {
				outerErr = asError(C.mdb_txn_commit(outerTxn))
			} else {
				C.mdb_txn_abort(outerTxn)
			}

			if outerErr == MapFull {
				// MapFull can come either from a Put, or from a Commit. We
				// need to increase the size, and then re-run the entire batch.
				outerErr = s.increaseSize()
				if outerErr == nil {
					continue
				} else {
					markBatchProcessed(batch, outerErr)
					return outerErr
				}

			} else if outerErr == TxnFull {
				// they've all been aborted; we switch to attempting them
				// 1-by-1 in the hope that individually, they will not
				// overfill transactions.
				for idx := 0; idx < batchLen; idx++ {
					fatalErr := s.runBatch(batch[idx : idx+1])
					if fatalErr != nil {
						markBatchProcessed(batch[idx+1:], fatalErr)
						return fatalErr
					}
				}
				return nil
			}

			markBatchProcessed(batch, outerErr)
			return nil
		}
		return nil
	}
}

func (s *server) runAndCommitWriteTxnMsg(batch []*readWriteTxnMsg, parentTxn *C.MDB_txn, msg *readWriteTxnMsg) (txnErr, fatalErr error) {
	txn, err := s.environment.txnBegin(false, parentTxn)
	if err != nil {
		// if we can't even create the txn, that's fatal to the whole system
		return nil, err
	}

	readWriteTxn := &s.readWriteTxn
	readWriteTxn.txn = txn
	err = msg.txnFun(readWriteTxn)
	readWriteTxn.txn = nil

	if err == nil {
		err = asError(C.mdb_txn_commit(txn))
	} else {
		C.mdb_txn_abort(txn)
	}
	return err, nil
}

func markBatchProcessed(batch []*readWriteTxnMsg, err error) {
	for _, msg := range batch {
		if msg != nil {
			msg.err = err
			msg.MarkProcessed()
		}
	}
}

func (s *server) Terminated(err error, caughtPanic interface{}) {
	if s.readWriteTxn.txn != nil { // this can happen if a txn fun panics
		C.mdb_txn_abort(s.readWriteTxn.txn)
		s.readWriteTxn.txn = nil
	}
	s.ServerBase.Terminated(err, caughtPanic)
}

func (s *server) increaseSize() error {
	atomic.StoreUint32(&s.resizeRequired, 1)
	s.resizingLock.Lock()
	defer s.resizingLock.Unlock()
	defer atomic.StoreUint32(&s.resizeRequired, 0)

	currentMapSize := s.environment.mapSize
	mapSize := uint64(float64(currentMapSize) * 1.5)
	if remainder := mapSize % s.environment.pageSize; remainder != 0 {
		mapSize = (mapSize + s.environment.pageSize) - remainder
	}

	if err := s.environment.setMapSize(mapSize); err != nil {
		s.Log.Error().Uint64("current size", currentMapSize).Uint64("new size", mapSize).Err(err).Msg("increasing map size")
		return err
	}
	if s.Log.Debug().Enabled() {
		s.Log.Debug().Uint64("current size", currentMapSize).Uint64("new size", mapSize).Msg("increasing map size")
	}
	s.environment.mapSize = mapSize
	return nil
}
