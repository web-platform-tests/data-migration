package main

import (
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	mapset "github.com/deckarep/golang-set"

	"cloud.google.com/go/datastore"

	"github.com/web-platform-tests/data-migration/processor"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

type masterLabeller struct {
	AllMasterSHAs mapset.Set
}

func hasAny(set mapset.Set, known []string) bool {
	for _, i := range known {
		if set.Contains(i) {
			return true
		}
	}
	return false
}

func (m masterLabeller) ShouldProcessRun(run *shared.TestRun) bool {
	return !hasAny(run.LabelsSet(), []string{"master", "pr_base", "pr_head"}) &&
		hasAny(run.LabelsSet(), []string{"azure", "taskcluster", "buildbot"}) &&
		run.TimeEnd.Sub(run.TimeStart) > time.Minute*10 &&
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
