package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/spanner"
	retry "github.com/avast/retry-go"
	mapset "github.com/deckarep/golang-set"
	log "github.com/sirupsen/logrus"
	"github.com/web-platform-tests/results-analysis/metrics"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var projectID *string
var inputGcsBucket *string
var gcpCredentialsFile *string
var outputSpannerInstanceID *string
var outputSpannerDatabaseID *string
var outputSpannerTableID *string
var numConcurrentRuns *int64
var numConcurrentBatches *int64

var countStmt string

func init() {
	projectID = flag.String("project_id", "wptdashboard-staging", "Google Cloud Platform project id")
	inputGcsBucket = flag.String("input_gcs_bucket", "wptd-results", "Google Cloud Storage bucket where shareded test results are stored")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")
	outputSpannerInstanceID = flag.String("output_spanner_instance_id", "wpt-results-staging", "Output Spanner instance ID")
	outputSpannerDatabaseID = flag.String("output_spanner_database_id", "wpt-results-staging", "Output Spanner database ID")
	outputSpannerTableID = flag.String("output_spanner_table_id", "results", "Output Spanner table ID")
	numConcurrentRuns = flag.Int64("num_concurrent_runs", 16, "Number of runs to process concurrently")
	numConcurrentBatches = flag.Int64("num_concurrent_batches", 100, "Number of concurrent row batches to write per run")

	countStmt = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id = @run_id", *outputSpannerTableID)
}

var maxHeapAlloc = uint64(4.0e+10)
var monitorSleep = 2 * time.Second
var monitorsPerGC = 4

func monitor() {
	var stats runtime.MemStats
	for i := 1; ; i++ {
		if i%monitorsPerGC == 0 {
			log.Infof("Monitor: Forcing GC")
			runtime.GC()
		}

		runtime.ReadMemStats(&stats)
		if stats.HeapAlloc > maxHeapAlloc {
			log.Fatalf("Out of memory")
		} else {
			log.Infof("Monitor: %d heap-allocated bytes OK", stats.HeapAlloc)
		}
		time.Sleep(monitorSleep)
	}
}

func getLoadableRuns(ctx context.Context, client *datastore.Client) ([]*datastore.Key, []shared.TestRun) {
	query := datastore.NewQuery("TestRun").Order("-CreatedAt")
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
		testRun.ID = key.ID
		keys = append(keys, key)
		if testRun.RawResultsURL == "" {
			log.Warningf("Skipping run with no results URL: %d", testRun.ID)
		} else {
			testRuns = append(testRuns, testRun)
		}
	}
	return keys, testRuns
}

func loadRunReport(ctx context.Context, run *shared.TestRun) (*metrics.TestResultsReport, error) {
	log.Infof("Reading report from %s", run.RawResultsURL)

	resp, err := http.Get(run.RawResultsURL)
	if err != nil {
		log.Warningf("Failed to load raw results from \"%s\" for run %d", run.RawResultsURL, run.ID)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		str := fmt.Sprintf("Non-OK HTTP status code of %d from \"%s\" for %d", resp.StatusCode, run.RawResultsURL, run.ID)
		log.Warningf(str)
		return nil, errors.New(str)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warningf("Failed to read contents of \"%s\" for %v", run.RawResultsURL, run)
		return nil, err
	}
	var report metrics.TestResultsReport
	err = json.Unmarshal(data, &report)
	if err != nil {
		log.Warningf("Failed to unmarshal JSON from \"%s\" for %v", run.RawResultsURL, run)
		return nil, err
	}
	if len(report.Results) == 0 {
		str := fmt.Sprintf("Empty report from %v (%s)", run, run.RawResultsURL)
		log.Warningf(str)
		return nil, errors.New(str)
	}

	log.Infof("Read report for run ID %d", run.ID)

	return &report, nil
}

func countReportResults(report *metrics.TestResultsReport) int64 {
	count := int64(0)
	for _, r := range report.Results {
		if len(r.Subtests) == 0 {
			count++
		} else {
			set := mapset.NewSet()
			for _, s := range r.Subtests {
				if set.Contains(s.Name) {
					log.Warningf("Found test \"%s\" contains duplicate subtest name \"%s\"", r.Test, s.Name)
				} else {
					set.Add(s.Name)
				}
			}
			count += int64(set.Cardinality())
		}
	}
	return count
}

func countSpannerResults(ctx context.Context, client *spanner.Client, runID int64) (int64, error) {
	params := map[string]interface{}{
		"run_id": runID,
	}
	s := spanner.Statement{
		SQL:    countStmt,
		Params: params,
	}

	log.Infof("Spanner query: \"%s\" with %v", countStmt, params)

	itr := client.Single().WithTimestampBound(spanner.MaxStaleness(1*time.Minute)).Query(ctx, s)
	defer itr.Stop()
	var count int64
	for {
		row, err := itr.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}

		err = row.Column(0, &count)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

func numRowsToUpload(ctx context.Context, client *spanner.Client, runID int64, report *metrics.TestResultsReport) (int64, error) {
	totalRows := countReportResults(report)
	existingRows, err := countSpannerResults(ctx, client, runID)
	if err != nil {
		return 0, err
	}

	log.Infof("Run %d contains %d rows (according to GCS); %d found in Spanner", runID, totalRows, existingRows)

	return totalRows - existingRows, nil
}

func uploadReportIfIncomplete(ctx context.Context, client *spanner.Client, runID int64, report *metrics.TestResultsReport, batchSize int, numConcurrentBatches int64) error {
	var numRows int64
	var err error
	retry.Do(func() error {
		numRows, err = numRowsToUpload(ctx, client, runID, report)
		return err
	}, retry.Attempts(5))
	if err != nil {
		return err
	}
	if numRows < 0 {
		str := fmt.Sprintf("More rows in spanner than report for run %d (report row deficeit: %d); skipping run", runID, numRows)
		log.Warningf(str)
		return nil
	}
	if numRows == 0 {
		log.Infof("Skipping alredy uploaded report for run %d", runID)
		return nil
	}

	log.Infof("Queuing rows for run %d", runID)

	rows := make([]*spanner.Mutation, 0)
	for _, r := range report.Results {
		if len(r.Subtests) == 0 {
			row := map[string]interface{}{
				"run_id":  runID,
				"test":    r.Test,
				"subtest": spanner.NullString{Valid: false},
				"result":  int64(metrics.TestStatusFromString(r.Status)),
			}
			if r.Message != nil {
				row["message"] = *r.Message
			} else {
				row["message"] = spanner.NullString{Valid: false}
			}
			rows = append(rows, spanner.ReplaceMap(*outputSpannerTableID, row))
		} else {
			for _, s := range r.Subtests {
				row := map[string]interface{}{
					"run_id":  runID,
					"test":    r.Test,
					"subtest": s.Name,
					"result":  int64(metrics.SubTestStatusFromString(s.Status)),
				}
				if s.Message != nil {
					row["message"] = *s.Message
				} else {
					row["message"] = spanner.NullString{Valid: false}
				}
				rows = append(rows, spanner.ReplaceMap(*outputSpannerTableID, row))
			}
		}
	}

	log.Infof("Queued %d rows for run %d", len(rows), runID)

	log.Infof("Creating transaction for %d-row write transaction for run %d", len(rows), runID)

	log.Infof("Writing batches for %d-row run %d", len(rows), runID)

	s := semaphore.NewWeighted(numConcurrentBatches)
	writeBatch := func(m, n int) {
		defer s.Release(1)
		batch := rows[m:n]

		log.Infof("Writing batch for %d-row run %d: [%d,%d)", len(rows), runID, m, n)
		_, err := client.Apply(ctx, batch)
		if err != nil {
			log.Fatalf("Error writing batch for %d-row run %d: %v", len(rows), runID, err)
		} else {
			log.Infof("Wrote batch for %d-row run %d: [%d,%d)", len(rows), runID, m, n)
		}
	}
	var end int
	for end = batchSize; end <= len(rows); end += batchSize {
		s.Acquire(ctx, 1)
		go writeBatch(end-batchSize, end)
	}
	if end != len(rows) {
		s.Acquire(ctx, 1)
		log.Infof("Writing small batch for %d-row run %d: [%d,%d)", len(rows), runID, end-batchSize, len(rows))
		go writeBatch(end-batchSize, len(rows))
		log.Infof("Wrote small batch for %d-row run %d: [%d,%d)", len(rows), runID, end-batchSize, len(rows))
	}
	s.Acquire(ctx, numConcurrentBatches)

	log.Infof("Wrote batches for %d-row run %d", len(rows), runID)

	return nil
}

func main() {
	flag.Parse()

	go monitor()

	ctx := context.Background()

	dsClient, err := datastore.NewClient(ctx, *projectID, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("Datastore client created: %v", dsClient)

	_, runs := getLoadableRuns(ctx, dsClient)

	sSpec := fmt.Sprintf("projects/%s/instances/%s/databases/%s", *projectID, *outputSpannerInstanceID, *outputSpannerDatabaseID)
	sClient, err := spanner.NewClient(ctx, sSpec, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}

	s := semaphore.NewWeighted(*numConcurrentRuns)
	for i, run := range runs {
		s.Acquire(ctx, 1)

		log.Infof("Processing run number %d / %d (id %d)", i, len(runs), run.ID)

		r := run
		go func(i int, run *shared.TestRun) {
			defer s.Release(1)

			err := retry.Do(func() error {
				var report *metrics.TestResultsReport
				err = retry.Do(func() error {
					report, err = loadRunReport(ctx, run)
					return err
				})
				if err != nil {
					return err
				}

				return retry.Do(func() error {
					return uploadReportIfIncomplete(ctx, sClient, run.ID, report, 1000, *numConcurrentBatches)
				}, retry.Attempts(5), retry.OnRetry(func(n uint, err error) {
					log.Infof("Recreating spanner client for retry")

					sClient.Close()
					var clientErr error
					sClient, clientErr = spanner.NewClient(ctx, sSpec, option.WithCredentialsFile(*gcpCredentialsFile))
					if clientErr != nil {
						log.Fatal(clientErr)
					}
				}))
			}, retry.Attempts(5))
			if err != nil {
				log.Fatal(err)
			}

			log.Infof("Finished processing run number %d / %d (id %d)", i, len(runs), run.ID)
		}(i, &r)
	}
	s.Acquire(ctx, *numConcurrentRuns)

	log.Infof("Import complete!")
}
