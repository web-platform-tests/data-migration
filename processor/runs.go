package processor

import (
	"cloud.google.com/go/datastore"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

// Runs is an interface for processors of TestRun entities.
type Runs interface {
	ShouldProcessRun(run *shared.TestRun) bool
	ProcessRun(tx *datastore.Transaction, key *datastore.Key, run *shared.TestRun) error
}
