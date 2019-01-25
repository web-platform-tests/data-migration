package main

import (
	"strings"

	"cloud.google.com/go/datastore"

	"github.com/web-platform-tests/data-migration/processor"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

var channels = []string{"stable", "release", "beta", "dev", "nightly", "preview"}

type channelLabeller struct{}

func (e channelLabeller) ShouldProcessRun(run *shared.TestRun) bool {
	if !shared.IsBrowserName(run.BrowserName) {
		return false
	}
	labels := run.LabelsSet()
	for _, c := range channels {
		if labels.Contains(c) {
			return false
		}
	}
	return true
}

func (e channelLabeller) ProcessRun(tx *datastore.Transaction, key *datastore.Key, run *shared.TestRun) error {
	switch run.BrowserName {
	case "chrome":
		if strings.HasSuffix(run.BrowserVersion, " dev") {
			run.Labels = append(run.Labels, "dev")
		} else if strings.HasSuffix(run.BrowserVersion, " beta") {
			run.Labels = append(run.Labels, "beta")
		}
	case "firefox":
		if strings.HasSuffix(run.BrowserVersion, "a1") {
			run.Labels = append(run.Labels, "nightly")
		} else if strings.HasSuffix(run.BrowserVersion, "b1") {
			run.Labels = append(run.Labels, "beta")
		}
	case "safari":
		if strings.Contains(run.BrowserVersion, "preview") || strings.Contains(run.BrowserVersion, "Preview") {
			run.Labels = append(run.Labels, "preview")
		}
	default:
		return nil
	}
	_, err := tx.Put(key, run)
	return err
}

func main() {
	processor.MigrateData(channelLabeller{})
}
