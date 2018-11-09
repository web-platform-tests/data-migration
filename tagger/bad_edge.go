package main

import (
	"context"
	"flag"
	"fmt"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"

	"github.com/web-platform-tests/wpt.fyi/shared"
)

var badRevisions = []string{
	"032cd9cc63",
	"064f51c50e",
	"6e9693d269",
	"c583bcd7eb",
	"5d4871b4b8",
	"8295368c82",
	"75b0f336c5",
	"0cc29e423f",
	"66aabf66f5",
	"71c05e7131",
	"b622fea47d",
	"f5832ccdb3",
	"09a1d43536",
	"f3e2b41349",
	"2ed748f10d",
	"d44bc3ed38",
	"168de7c332",
	"5acd3bcf66",
	"99e577f57c",
}

var (
	projectID = flag.String("project",
		"wptdashboard-staging",
		"Google Cloud project")
)

type conditionUnsatisfied struct{}

func (e conditionUnsatisfied) Error() string {
	return "Condition not satisfied"
}

func condition(run *shared.TestRun) bool {
	if run.BrowserName != "edge" {
		return false
	}
	return shared.StringSliceContains(badRevisions, run.Revision)
}

func operation(tx *datastore.Transaction, key *datastore.Key, run *shared.TestRun) error {
	run.BrowserName = "brokenedge"
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

	query := datastore.NewQuery("TestRun").
		Filter("BrowserName =", "edge").
		KeysOnly()

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
