package main

import (
	"context"
	"flag"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"google.golang.org/api/iterator"
)

var (
	projectID = flag.String("project", "wptdashboard-staging", "Google Cloud project")
)

func print(run *shared.TestRun) {
	fmt.Printf("%d %s-%s %s-%s %s\n", run.ID, run.BrowserName, run.BrowserVersion, run.OSName, run.OSVersion, run.ResultsURL)
}

func main() {
	flag.Parse()

	ctx := context.Background()

	dsClient, err := datastore.NewClient(ctx, *projectID)
	if err != nil {
		panic(err)
	}

	query := datastore.NewQuery("TestRun").Order("-RawResultsURL")

	var lastRun shared.TestRun
	var printedFirst bool

	for t := dsClient.Run(ctx, query); ; {
		key, err := t.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}

		var run shared.TestRun
		dsClient.Get(ctx, key, &run)
		run.ID = key.ID
		if run.RawResultsURL == "" {
			break
		}

		if run.RawResultsURL == lastRun.RawResultsURL {
			if !printedFirst {
				fmt.Printf("Found duplicate(s) of: ")
				printedFirst = true
				print(&lastRun)
			}
			fmt.Printf("Deleting: ")
			print(&run)
			dsClient.Delete(ctx, key)
		} else {
			printedFirst = false
		}

		lastRun = run
	}
}
