package golmdb_test

import (
	"flag"
	"os"
	"testing"
	"time"
)

var (
	seed int64
	soak uint
)

func TestMain(m *testing.M) {
	flag.Int64Var(&seed, "seed", 0, "Set seed for Random number generator. If 0, then time.Now().UnixNano() is used")
	flag.UintVar(&soak, "soak", 0, "Number of events soak tests should perform. If 0, no soak tests are run")
	flag.Parse()

	if soak > 0 && seed == 0 {
		seed = time.Now().UnixNano()
	}

	os.Exit(m.Run())
}
