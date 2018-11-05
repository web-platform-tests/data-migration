package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/deckarep/golang-set"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"

	"github.com/web-platform-tests/wpt.fyi/shared"
)

var (
	projectID = flag.String("project", "wptdashboard-staging", "Google Cloud project")
	allSHAs   = mapset.NewSet()
)

type conditionUnsatisfied struct{}

func (e conditionUnsatisfied) Error() string {
	return "Condition not satisfied"
}

func condition(run *shared.TestRun) bool {
	return !run.LabelsSet().Contains("master") && allSHAs.Contains(run.Revision)
}

func operation(tx *datastore.Transaction, key *datastore.Key, run *shared.TestRun) error {
	run.Labels = append(run.Labels, "master")
	_, err := tx.Put(key, run)
	return err
}

func main() {
	flag.Parse()

	ctx := context.Background()

	dsClient, err := datastore.NewClient(ctx, *projectID)
	if err != nil {
		panic(err)
	}

	cmd := exec.Command("git", "rev-list", "origin/master")
	dir, err := os.Getwd()
	cmd.Dir = path.Join(dir, "../wpt")
	bytes, err := cmd.Output()
	for _, hash := range strings.Split(string(bytes), "\n") {
		if len(hash) > 9 {
			allSHAs.Add(hash)
			allSHAs.Add(hash[:10])
		}
	}

	query := datastore.NewQuery("TestRun").KeysOnly()

	for t := dsClient.Run(ctx, query); ; {
		key, err := t.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}
		var run shared.TestRun
		_, err = dsClient.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			err := tx.Get(key, &run)
			if err != nil {
				return err
			}
			if condition(&run) {
				return operation(tx, key, &run)
			}
			return conditionUnsatisfied{}
		})
		if err != nil {
			_, ok := err.(conditionUnsatisfied)
			if !ok {
				panic(err)
			} else {
				continue
			}
		}
		fmt.Printf("Processed TestRun %s (%s %s)\n", key.String(), run.BrowserName, run.BrowserVersion)
	}
}
