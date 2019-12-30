package main

import (
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	mapset "github.com/deckarep/golang-set"

	"github.com/web-platform-tests/data-migration/processor"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

// safari12STPLabeller ensures that Safari 12.1 runs are labelled
// 'experimental' (and not 'stable') if they ran before Safari 12.1 was
// actually released to stable.
type safari12STPLabeller struct{}

func (e safari12STPLabeller) ShouldProcessRun(run *shared.TestRun) bool {
	if run.BrowserName != "safari" {
		return false
	}
	if !strings.HasPrefix(run.BrowserVersion, "12.1") {
		return false
	}
	// We ran Safari 12.1 as STP from 2018-08-07 until 2019-01-05. It then
	// released as stable 2019-04-09. So anything after February (to pick
	// an arbitrary mid-point) is stable and should be left alone.
	if run.TimeStart.After(time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC)) {
		return false;
	}
	return true
}

func (e safari12STPLabeller) ProcessRun(tx *datastore.Transaction, key *datastore.Key, run *shared.TestRun) error {
	labels := mapset.NewSet()
	for _, label := range run.Labels {
		labels.Add(label)
	}
	labels.Remove("stable")
	labels.Add("experimental")
	run.Labels = nil
	for label := range labels.Iter() {
		run.Labels = append(run.Labels, label.(string))
	}
	_, err := tx.Put(key, run)
	return err
}

func main() {
	processor.MigrateData(safari12STPLabeller{})
}
