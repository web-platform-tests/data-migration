package main

import (
	"strings"

	"cloud.google.com/go/datastore"
	mapset "github.com/deckarep/golang-set"

	"github.com/web-platform-tests/data-migration/processor"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

type experimentalLabeller struct{}

func (e experimentalLabeller) ShouldProcessRun(run *shared.TestRun) bool {
	if !shared.IsBrowserName(run.BrowserName) {
		return false
	}
	if strings.HasSuffix(run.BrowserName, "-experimental") {
		return true
	}
	if strings.HasSuffix(run.BrowserVersion, " dev") || strings.HasSuffix(run.BrowserVersion, "a1") {
		return true
	}
	return false
}

func (e experimentalLabeller) ProcessRun(tx *datastore.Transaction, key *datastore.Key, run *shared.TestRun) error {
	labels := mapset.NewSet()
	for _, label := range run.Labels {
		labels.Add(label)
	}
	labels.Add("experimental")
	labels.Remove("stable")
	run.Labels = nil
	for label := range labels.Iter() {
		run.Labels = append(run.Labels, label.(string))
	}
	_, err := tx.Put(key, run)
	return err
}

func main() {
	processor.MigrateData(experimentalLabeller{})
}
