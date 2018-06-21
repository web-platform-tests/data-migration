package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/datastore"
	"github.com/web-platform-tests/results-analysis/metrics"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var projectID *string
var inputGcsBucket *string
var gcpCredentialsFile *string
var outputBTInstanceID *string
var outputBTTableID *string
var outputBTFamily *string

func init() {
	projectID = flag.String("project_id", "wptdashboard", "Google Cloud Platform project id")
	inputGcsBucket = flag.String("input_gcs_bucket", "wptd-results", "Google Cloud Storage bucket where shareded test results are stored")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")
	outputBTInstanceID = flag.String("output_bt_instance_id", "wpt-results-matrix", "Output BigTable instance ID")
	outputBTTableID = flag.String("output_bt_table_id", "wpt-results", "Output BigTable table ID")
	outputBTFamily = flag.String("output_bt_family", "tests", "Output BigTable column family for test results")
}

var numConcurrentRuns = int64(100)
var maxOpsPerMutation = 100000
var maxMutationsPerBatch = 1
var maxHeapAlloc = uint64(4.5e+10)
var monitorSleep = 2 * time.Second

func monitor() {
	var stats runtime.MemStats
	for {
		runtime.ReadMemStats(&stats)
		if stats.HeapAlloc > maxHeapAlloc {
			log.Fatal("ERRO: Out of memory")
		} else {
			log.Printf("INFO: Monitor: %d heap-allocated bytes OK", stats.HeapAlloc)
		}
		time.Sleep(monitorSleep)
	}
}

func getRuns(ctx context.Context, client *datastore.Client) ([]*datastore.Key, []shared.TestRun) {
	log.SetFlags(log.LstdFlags | log.Llongfile | log.LUTC)
	flag.Parse()

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

func runID(run shared.TestRun) string {
	return run.BrowserName + "-" + run.BrowserVersion + "-" + run.OSName + "-" + run.OSVersion + "@" + run.FullRevisionHash + "#" + run.CreatedAt.UTC().Format(time.RFC3339)
}

func resultID(res *metrics.TestResults, sub *metrics.SubTest) string {
	if sub == nil {
		return res.Test
	}

	return res.Test + "#" + sub.Name
}

func resultValue(res *metrics.TestResults, sub *metrics.SubTest) []byte {
	if sub == nil {
		return []byte(res.Status)
	}

	return []byte(res.Status + "#" + sub.Status)
}

func main() {
	go monitor()

	ctx := context.Background()
	dsClient, err := datastore.NewClient(ctx, *projectID, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}

	btClient, err := bigtable.NewClient(ctx, *projectID, *outputBTInstanceID, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	tbl := btClient.Open(*outputBTTableID)
	ts := bigtable.Now()

	_, runs := getRuns(ctx, dsClient)
	sem := semaphore.NewWeighted(numConcurrentRuns)
	for _, run := range runs {
		go func(run shared.TestRun) {
			sem.Acquire(ctx, 1)
			defer sem.Release(1)

			resp, err := http.Get(run.RawResultsURL)
			if err != nil {
				log.Printf("WARN: Failed to load raw results from \"%s\" for %v", run.RawResultsURL, run)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Printf("WARN: Non-OK HTTP status code of %d from \"%s\" for %v", resp.StatusCode, run.RawResultsURL, run)
				return
			}
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("WARN: Failed to read contents of \"%s\" for %v", run.RawResultsURL, run)
				return
			}
			var report metrics.TestResultsReport
			err = json.Unmarshal(data, &report)
			if err != nil {
				log.Printf("WARN: Failed to unmarshal JSON from \"%s\" for %v", run.RawResultsURL, run)
				return
			}
			if len(report.Results) == 0 {
				log.Printf("WARN: Empty report from %s (%s)", runID(run), run.RawResultsURL)
				return
			}

			log.Printf("INFO: Gathering %d test results", len(report.Results))
			mutCount := 0
			id := runID(run)
			mut := bigtable.NewMutation()
			muts := []*bigtable.Mutation{mut}
			rows := []string{id}
			set := func(family, column string, ts bigtable.Timestamp, value []byte) {
				if mutCount == maxOpsPerMutation {
					if len(muts) == maxMutationsPerBatch {
						rs := rows[0:]
						ms := muts[0:]
						errs, err := tbl.ApplyBulk(ctx, rs, ms)
						if len(errs) > 0 {
							log.Printf("ERRO: Some writes from BigTable bulk write failed: %v", errs)
						} else if err != nil {
							log.Printf("ERRO: BigTable bulk write failed: %v", err)
						} else {
							log.Printf("INFO: BigTable bulk write success (%d mutations to row %s)", len(ms), rs[0])
						}

						mut = bigtable.NewMutation()
						muts = []*bigtable.Mutation{mut}
						rows = []string{id}
						mutCount = 0
					} else {
						mut = bigtable.NewMutation()
						muts = append(muts, mut)
						rows = append(rows, id)
						mutCount = 0
					}
				}

				mut.Set(family, column, ts, value)
				mutCount++
			}

			for _, res := range report.Results {
				if len(res.Subtests) == 0 {
					set(*outputBTFamily, resultID(res, nil), ts, resultValue(res, nil))
				} else {
					for _, sub := range res.Subtests {
						set(*outputBTFamily, resultID(res, &sub), ts, resultValue(res, &sub))
					}
				}
			}

			if mutCount > 0 {
				rs := rows[0:]
				ms := muts[0:]
				errs, err := tbl.ApplyBulk(ctx, rs, ms)
				if len(errs) > 0 {
					log.Printf("ERRO: Some writes from BigTable bulk write failed: %v", errs)
				} else if err != nil {
					log.Printf("ERRO: BigTable bulk write failed: %v", err)
				} else {
					log.Printf("INFO: BigTable bulk write success (%d mutations to row %s)", len(ms), rs[0])
				}
			}
		}(run)
	}

	sem.Acquire(ctx, numConcurrentRuns)
	log.Printf("INFO: Finished processing %d runs", len(runs))
}
