package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

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
	query := datastore.NewQuery("TestRun").Order("CreatedAt").Limit(2)
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
	testMap := make(map[string]int)
	runSlice := make([]Run, 0)
	testSlice := make([]Test, 0)
	testsToRuns := make(map[string]map[string]metrics.CompleteTestStatus)
	runsToTests := make(map[string]map[string]metrics.CompleteTestStatus)
	_, runs := getRuns(ctx, client)
	for _, run := range runs {
		runID := strconv.Itoa(nextRunID)
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
			log.Printf("WARN: Non-OK HTTP status code of %d from \"%s\" for %v", resp.StatusCode, run.RawResultsURL, run)
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
				testMap[baseName] = nextTestID
				testSlice = append(testSlice, Test{
					ID:   nextTestID,
					Test: res.Test,
				})
				nextTestID++
			}
			status := metrics.CompleteTestStatus{
				Status: metrics.TestStatusFromString(res.Status),
			}
			testID := strconv.Itoa(testMap[baseName])
			if _, ok := runsToTests[runID]; !ok {
				runsToTests[runID] = make(map[string]metrics.CompleteTestStatus)
			}
			runsToTests[runID][testID] = status
			if _, ok := testsToRuns[testID]; !ok {
				testsToRuns[testID] = make(map[string]metrics.CompleteTestStatus)
			}
			testsToRuns[testID][runID] = status
			for _, sub := range res.Subtests {
				subName := baseName + ":" + sub.Name
				if _, ok := testMap[subName]; !ok {
					testMap[subName] = nextTestID
					testSlice = append(testSlice, Test{
						ID:      nextTestID,
						Test:    res.Test,
						Subtest: sub.Name,
					})
					nextTestID++
				}
				status := metrics.CompleteTestStatus{
					Status:    metrics.TestStatusFromString(res.Status),
					SubStatus: metrics.SubTestStatusFromString(sub.Status),
				}
				runsToTests[runID][testID] = status
				testsToRuns[testID][runID] = status
			}
		}
	}

	log.Printf("%d runs and %d tests", len(runSlice), len(testSlice))

	{
		runsJSON, err := json.Marshal(runSlice)
		if err != nil {
			log.Printf("Failed to marshal runs: %v", err)
		} else {
			err = ioutil.WriteFile("runs.json", runsJSON, 0644)
			if err != nil {
				log.Printf("Failed to save runs: %v", err)
			}
		}
	}
	runSlice = nil

	{
		testsJSON, err := json.Marshal(testSlice)
		if err != nil {
			log.Printf("Failed to marshal tests: %v", err)
		} else {
			err = ioutil.WriteFile("tests.json", testsJSON, 0644)
			if err != nil {
				log.Printf("Failed to save tests: %v", err)
			}
		}
	}
	testSlice = nil

	{
		os.Mkdir("by_run", 0700)
		for runName, data := range runsToTests {
			json, err := json.Marshal(data)
			if err != nil {
				log.Printf("Failed to marshal tests for run %s: %v", runName, err)
			} else {
				err = ioutil.WriteFile("by_run/"+runName+".json", json, 0644)
				if err != nil {
					log.Printf("Failed to save tests for run %s: %v", runName, err)
				}
			}
		}
	}
	runsToTests = nil

	{
		os.Mkdir("by_test", 0700)
		for testName, data := range testsToRuns {
			json, err := json.Marshal(data)
			if err != nil {
				log.Printf("Failed to marshal runs for test %s: %v", testName, err)
			} else {
				err = ioutil.WriteFile("by_test/"+testName+".json", json, 0644)
				if err != nil {
					log.Printf("Failed to save runs for test %s: %v", testName, err)
				}
			}
		}
	}
	testsToRuns = nil
}
