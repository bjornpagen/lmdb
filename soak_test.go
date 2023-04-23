package golmdb_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	"wellquite.org/golmdb"
)

// Running this with -soak=100 takes around 100 seconds. Don't forget
// the default timeout is 10m, so you might want -timeout=0 for long
// runs.
//
// This will also need about 200MB of space in your TMPDIR
func TestSoak(t *testing.T) {
	if soak == 0 {
		t.Skip("-soak flag not provided or 0, so skipping.")
	}

	SetGlobalLogLevelDebug()
	log := NewTestLogger(t)
	is := is.New(t)

	log.Info().Int64("seed", seed).Send()

	rng := rand.New(rand.NewSource(seed))

	// create a million keys (numbers) with random values
	keys, keyValueMap := makeAllKeys(rng)
	// shuffle them
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	client, dir, err := createDatabase(log, 16)
	is.NoErr(err)
	defer os.RemoveAll(dir)
	defer client.TerminateSync()

	log.Info().Msg("Loading key-value pairs")
	dbRef, err := createDBRef(client, t.Name(), 0)
	// chuck all the key value pairs in. We could range through
	// keyValueMap but that's non-deterministic, and I'd prefer to keep
	// as much as possible deterministic.
	err = client.Update(func(rwtxn *golmdb.ReadWriteTxn) (err error) {
		for _, key := range keys {
			val := keyValueMap[key]
			if err = rwtxn.Put(dbRef, key[:], val[:], 0); err != nil {
				return err
			}
		}
		return err
	})
	is.NoErr(err)

	// we're going to spawn a number of go routines to do reading of
	// the data base. They need to report errors back to the main
	// thread.
	errLock := new(sync.Mutex)
	var errs []error

	maxViewersPerRound := 32 // must not be greater than numReaders
	totalGetCount := 0
	totalRePutCount := 0

	log.Info().Msg("Reading and modifying concurrently")

	// The "big idea" here is that for each "round", spawn a number of
	// readers. They all concurrently start View transactions, and
	// verify that some prefix of keys can all be read and match what
	// we think should be there.
	//
	// Once they've all started, LMDB should give them all a snapshot
	// of the database, so we can then go on and modify the database,
	// but the viewers should see the previous version - the snapshot.
	//
	// Once the modifications are done, we update our expectations
	// (carefully - viewers could still be running and they need to
	// continue to expect the previous set of expectations), and then
	// go on to the next "round".
	//
	// So reads and writes can happen at the same time, but writes
	// should only affect reads that _start_ _after_ the _write_ has
	// _finished_ (committed).

	start := time.Now()
	allViewersFinishedWG := new(sync.WaitGroup)

	for roundNum := uint(0); roundNum < soak; roundNum++ {
		// how many viewers to launch this round?
		viewersCount := 1 + rng.Intn(maxViewersPerRound)
		// to wait for this round's viewers to start:
		viewersStartedWG := new(sync.WaitGroup)
		viewersStartedWG.Add(viewersCount)
		allViewersFinishedWG.Add(viewersCount)

		keyValueMapSnapshot := keyValueMap
		for viewNum := 0; viewNum < viewersCount; viewNum++ {
			// each viewer can check a different prefix of keys
			toCheck := keys[0 : 1+rng.Intn(len(keys))]
			totalGetCount += len(toCheck)
			go runViewer(allViewersFinishedWG, viewersStartedWG, client, dbRef, toCheck, keyValueMapSnapshot, errLock, &errs)
		}

		// wait until all the viewers for this round have started, and
		// thus established their snapshot with LMDB.
		viewersStartedWG.Wait()

		// what are we going to modify?
		toModify := keys[0 : 1+rng.Intn(len(keys))]
		keyValueMapModified, modifiedCount, err := modify(rng, client, dbRef, keyValueMap, toModify)
		is.NoErr(err)
		// add in to keyValueMapModified everything from keyValueMap that we've not modified (i.e. was after toModify)
		for _, key := range keys[len(toModify):] {
			keyValueMapModified[key] = keyValueMap[key]
		}
		// pointer swizzle
		keyValueMap = keyValueMapModified

		totalRePutCount += modifiedCount
		log.Debug().Uint("round", roundNum).Int("viewers", viewersCount).Int("re-puts", modifiedCount).Send()
	}

	allViewersFinishedWG.Wait()
	end := time.Now()
	errLock.Lock()
	if len(errs) > 0 {
		t.Error(errs)
	}
	errLock.Unlock()

	elapsed := end.Sub(start)

	log.Info().Str("duration", elapsed.String()).Int("total gets", totalGetCount).Int("total re-puts", totalRePutCount).Send()
	log.Info().Float64("rough gets per second", float64(totalGetCount)*float64(time.Second)/float64(elapsed)).Float64("rough re-puts per second", float64(totalRePutCount)*float64(time.Second)/float64(elapsed)).Send()
}

func makeAllKeys(rng *rand.Rand) ([][8]byte, map[[8]byte][8]byte) {
	const lim = 1024 * 1024
	keys := make([][8]byte, lim)
	keyValueMap := make(map[[8]byte][8]byte, lim)
	for idx := 0; idx < lim; idx++ {
		key := [8]byte{}
		binary.BigEndian.PutUint64(key[:], uint64(idx))
		keys[idx] = key
		val := [8]byte{}
		binary.BigEndian.PutUint64(val[:], rng.Uint64())
		keyValueMap[key] = val
	}
	return keys, keyValueMap
}

func runViewer(allViewersFinishedWG, viewersStartedWG *sync.WaitGroup, client *golmdb.LMDBClient, dbRef golmdb.DBRef, toCheck [][8]byte, keyValueMapSnapshot map[[8]byte][8]byte, errLock *sync.Mutex, errs *[]error) {
	defer allViewersFinishedWG.Done()
	started := false
	err := client.View(func(rotxn *golmdb.ReadOnlyTxn) (err error) {
		if !started {
			started = true
			viewersStartedWG.Done()
			for idx, key := range toCheck {
				val, err := rotxn.Get(dbRef, key[:])
				if err != nil {
					return err
				}
				expected := keyValueMapSnapshot[key]
				if !bytes.Equal(expected[:], val) {
					return fmt.Errorf("(%d) For key %v, expected %v, got %v", idx, key, expected[:], val)
				}
			}
		}
		// nothing we can do if we're already started (i.e. the txn has
		// been automatically restarted): the snapshot we wanted is no
		// longer accessible to us.
		return err
	})
	if err != nil {
		errLock.Lock()
		*errs = append(*errs, err)
		errLock.Unlock()
	}
}

func modify(rng *rand.Rand, client *golmdb.LMDBClient, dbRef golmdb.DBRef, keyValueMap map[[8]byte][8]byte, toModify [][8]byte) (keyValueMapModified map[[8]byte][8]byte, modifiedCount int, err error) {
	// Because the txn can run multiple times (the txn could fail due
	// to out of space and so gets re-run once the database size is
	// increased), we want to form these skips outside the txn so that
	// the rng stays deterministic across multiple runs.
	//
	// Also because the txn can run multiple times, we need to be
	// careful about resetting keyValueMapModified and modifiedCount at
	// the start of each txn run.
	skips := make([]bool, len(toModify))
	for idx := range skips {
		skips[idx] = rng.Intn(3) != 0
	}
	err = client.Update(func(rwtxn *golmdb.ReadWriteTxn) (err error) {
		keyValueMapModified = make(map[[8]byte][8]byte, len(keyValueMap))
		modifiedCount = 0
		for idx, key := range toModify {
			val := keyValueMap[key]
			if skips[idx] {
				keyValueMapModified[key] = val
				continue
			}
			binary.BigEndian.PutUint64(val[:], rng.Uint64())
			keyValueMapModified[key] = val
			if err = rwtxn.Put(dbRef, key[:], val[:], 0); err != nil {
				return err
			}
			modifiedCount++
		}
		return err
	})
	return keyValueMapModified, modifiedCount, err
}
