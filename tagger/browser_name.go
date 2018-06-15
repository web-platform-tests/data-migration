package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"

	"github.com/web-platform-tests/wpt.fyi/shared"
)

var (
	projectID = flag.String("project", "wptdashboard-staging", "Google Cloud project")
)

type conditionUnsatisfied struct{}

func (e conditionUnsatisfied) Error() string {
	return "Condition not satisfied"
}

func condition(run *shared.TestRun) bool {
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

func operation(tx *datastore.Transaction, key *datastore.Key, run *shared.TestRun) error {
	run.Labels = append(run.Labels, strings.TrimSuffix(run.BrowserName, "-experimental"))
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
		fmt.Printf("Proccessed TestRun %s (%s %s)\n", key.String(), run.BrowserName, run.BrowserVersion)
	}
}
