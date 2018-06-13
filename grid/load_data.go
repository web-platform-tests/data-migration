package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"

	"cloud.google.com/go/datastore"
	"github.com/web-platform-tests/results-analysis/metrics"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var projectID *string
var inputGcsBucket *string
var gcpCredentialsFile *string

func init() {
	projectID = flag.String("project_id", "wptdashboard", "Google Cloud Platform project id")
	inputGcsBucket = flag.String("input_gcs_bucket", "wptd-results", "Google Cloud Storage bucket where shareded test results are stored")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")
}

func getRuns(ctx context.Context, client *datastore.Client) ([]*datastore.Key, []shared.TestRun) {
	query := datastore.NewQuery("TestRun").Order("CreatedAt")
	keys := make([]*datastore.Key, 0)
	testRuns := make([]shared.TestRun, 0)
	it := client.Run(ctx, query)
	for {
		var testRun shared.TestRun
		key, err := it.Next(&testRun)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		keys = append(keys, key)
		testRuns = append(testRuns, testRun)
	}
	return keys, testRuns
}

type Run struct {
	ID int `json:"id"`
	shared.TestRun
}

type Test struct {
	ID      int    `json:"id"`
	Test    string `json:"test"`
	Subtest string `json:"subtest,omitempty"`
}

func main() {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, *projectID, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	nextRunID := 0
	nextTestID := 0
	testMap := make(map[string]bool)
	runSlice := make([]Run, 0)
	testSlice := make([]Test, 0)
	_, runs := getRuns(ctx, client)
	for _, run := range runs {
		runSlice = append(runSlice, Run{
			ID:      nextRunID,
			TestRun: run,
		})
		nextRunID++

		resp, err := http.Get(run.RawResultsURL)
		if err != nil {
			log.Printf("WARN: Failed to load raw results from \"%s\" for %v", run.RawResultsURL, run)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("WARN: Non-OK HTTP status code of %s from \"%s\" for %v", resp.StatusCode, run.RawResultsURL, run)
			continue
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("WARN: Failed to read contents of \"%s\" for %v", run.RawResultsURL, run)
		}
		var report metrics.TestResultsReport
		err = json.Unmarshal(data, &report)
		if err != nil {
			log.Printf("WARN: Failed to unmarshal JSON from \"%s\" for %v", run.RawResultsURL, run)
		}
		for _, res := range report.Results {
			baseName := res.Test
			if _, ok := testMap[baseName]; !ok {
				testMap[baseName] = true
				testSlice = append(testSlice, Test{
					ID:   nextTestID,
					Test: res.Test,
				})
				nextTestID++
			}
			for _, sub := range res.Subtests {
				subName := baseName + ":" + sub.Name
				if _, ok := testMap[subName]; !ok {
					testMap[subName] = true
					testSlice = append(testSlice, Test{
						ID:      nextTestID,
						Test:    res.Test,
						SubTest: &sub.Name,
					})
					nextTestID++
				}
			}
		}
	}

	log.Printf("%d runs and %d tests", len(runSlice), len(testSlice))

}
