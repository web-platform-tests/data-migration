package main

import (
	"strings"

	"cloud.google.com/go/datastore"

	"github.com/web-platform-tests/data-migration/processor"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

// browserNameLabeller labels a run with its browser name.
type browserNameLabeller struct{}

func (b browserNameLabeller) ShouldProcessRun(run *shared.TestRun) bool {
	if !shared.IsBrowserName(run.BrowserName) {
		return false
	}
	for _, label := range run.Labels {
		if shared.IsStableBrowserName(label) {
			return false
		}
	}
	return true
}

func (b browserNameLabeller) ProcessRun(tx *datastore.Transaction, key *datastore.Key, run *shared.TestRun) error {
	run.Labels = append(run.Labels, strings.TrimSuffix(run.BrowserName, "-experimental"))
	_, err := tx.Put(key, run)
	return err
}

func main() {
	processor.MigrateData(browserNameLabeller{})
}
