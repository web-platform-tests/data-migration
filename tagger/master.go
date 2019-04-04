package main

import (
	"log"
	"os"
	"os/exec"
	"path"
	"strings"

	mapset "github.com/deckarep/golang-set"

	"cloud.google.com/go/datastore"

	"github.com/web-platform-tests/data-migration/processor"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

type masterLabeller struct {
	AllMasterSHAs mapset.Set
}

func isKnownUploader(labels mapset.Set) bool {
	uploaders := []string{"azure", "buildbot", "taskcluster"}
	for _, u := range uploaders {
		if labels.Contains(u) {
			return true
		}
	}
	return false
}

func (m masterLabeller) ShouldProcessRun(run *shared.TestRun) bool {
	return !run.LabelsSet().Contains("master") &&
		!run.LabelsSet().Contains("pr_base") &&
		!run.LabelsSet().Contains("pr_head") &&
		isKnownUploader(run.LabelsSet()) &&
		m.AllMasterSHAs.Contains(run.Revision)
}

func (m masterLabeller) ProcessRun(tx *datastore.Transaction, key *datastore.Key, run *shared.TestRun) error {
	run.Labels = append(run.Labels, "master")
	_, err := tx.Put(key, run)
	return err
}

func main() {
	cmd := exec.Command("git", "rev-list", "origin/master")
	dir, err := os.Getwd()
	cmd.Dir = path.Join(dir, "../../wpt")
	bytes, err := cmd.Output()
	if err != nil {
		log.Fatalf("Failed to scrape revisions: %s", err.Error())
	}
	allSHAs := mapset.NewSet()
	for _, hash := range strings.Split(string(bytes), "\n") {
		if len(hash) > 9 {
			allSHAs.Add(hash)
			allSHAs.Add(hash[:10])
		}
	}
	processor.MigrateData(masterLabeller{
		AllMasterSHAs: allSHAs,
	})
}
