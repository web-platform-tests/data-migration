package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/spanner"
	"github.com/web-platform-tests/results-analysis/metrics"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var projectID *string
var inputGcsBucket *string
var gcpCredentialsFile *string
var outputSpannerInstanceID *string
var outputSpannerDatabaseID *string
var outputSpannerTableID *string

func init() {
	projectID = flag.String("project_id", "wptdashboard-staging", "Google Cloud Platform project id")
	inputGcsBucket = flag.String("input_gcs_bucket", "wptd-results", "Google Cloud Storage bucket where shareded test results are stored")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")
	outputSpannerInstanceID = flag.String("output_spanner_instance_id", "wpt-results-staging", "Output Spanner instance ID")
	outputSpannerDatabaseID = flag.String("output_spanner_database_id", "wpt-results-staging", "Output Spanner database ID")
	outputSpannerTableID = flag.String("output_spanner_table_id", "results", "Output Spanner table ID")
}

type Report struct {
	metrics.TestResultsReport
	RunID int64
}

type ResultRow map[string]interface{}

type buffer struct {
	s   int64
	b   []interface{}
	h   int64
	t   int64
	m   *sync.Mutex
	in  *semaphore.Weighted
	out *semaphore.Weighted
	ctx context.Context
	num int64
}

func (b *buffer) Put(v interface{}) error {
	putMutex.Lock()
	putMutex.Unlock()

	err := b.in.Acquire(b.ctx, 1)
	if err != nil {
		return err
	}

	b.m.Lock()
	b.b[b.h] = v
	b.h = (b.h + 1) % b.s
	b.num++
	b.m.Unlock()

	b.out.Release(1)

	return nil
}

func (b *buffer) Get() (interface{}, error) {
	err := b.out.Acquire(b.ctx, 1)
	if err != nil {
		return nil, err
	}

	b.m.Lock()
	v := b.b[b.t]
	b.b[b.t] = 0
	b.t = (b.t + 1) % b.s
	b.num--
	b.m.Unlock()

	b.in.Release(1)

	return v, nil
}

func NewBuffer(ctx context.Context, name string, s int64) *buffer {
	var b buffer
	b.ctx = ctx
	b.s = s

	b.b = make([]interface{}, b.s, b.s)
	b.m = &sync.Mutex{}
	b.in = semaphore.NewWeighted(b.s)
	b.out = semaphore.NewWeighted(b.s)
	b.out.Acquire(b.ctx, b.s)

	go func() {
		for {
			b.m.Lock()
			log.Infof("Buffer %s: %d / %d", name, b.num, b.s)
			b.m.Unlock()

			time.Sleep(5 * time.Second)
		}
	}()

	return &b
}

var maxHeapAlloc = uint64(4.0e+10)
var monitorSleep = 2 * time.Second
var monitorsPerGC = 4
var putMutex = &sync.Mutex{}
var putBlocked = false

func monitor() {
	var stats runtime.MemStats
	for i := 1; ; i++ {
		if i%monitorsPerGC == 0 {
			log.Infof("Monitor: Forcing GC")
			runtime.GC()
		}

		runtime.ReadMemStats(&stats)
		if stats.HeapAlloc > maxHeapAlloc {
			log.Errorf("Out of memory")
			if !putBlocked {
				putBlocked = true
				log.Infof("Locking buffer Put()")
				putMutex.Lock()
				log.Infof("Buffer Put() locked")
			}
		} else {
			log.Infof("Monitor: %d heap-allocated bytes OK", stats.HeapAlloc)
			if putBlocked {
				putBlocked = false
				log.Infof("Unlocking buffer Put()")
				putMutex.Unlock()
				log.Infof("Buffer Put() unlocked")
			}
		}
		time.Sleep(monitorSleep)
	}
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
		testRun.ID = key.ID
		keys = append(keys, key)
		testRuns = append(testRuns, testRun)
	}
	return keys, testRuns
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

	sSpec := fmt.Sprintf("projects/%s/instances/%s/databases/%s", *projectID, *outputSpannerInstanceID, *outputSpannerDatabaseID)
	sClient, err := spanner.NewClient(ctx, sSpec, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("Spanner client created: %v", sClient)

	_, runs := getRuns(ctx, dsClient)

	reportBuf := NewBuffer(ctx, "report", 10)
	go func() {
		for _, run := range runs {
			func() {
				log.Infof("Reading report from %s", run.RawResultsURL)

				resp, err := http.Get(run.RawResultsURL)
				if err != nil {
					log.Warningf("Failed to load raw results from \"%s\" for %v", run.RawResultsURL, run)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					log.Warningf("Non-OK HTTP status code of %d from \"%s\" for %v", resp.StatusCode, run.RawResultsURL, run)
					return
				}
				data, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					log.Warningf("Failed to read contents of \"%s\" for %v", run.RawResultsURL, run)
					return
				}
				var report metrics.TestResultsReport
				err = json.Unmarshal(data, &report)
				if err != nil {
					log.Warningf("Failed to unmarshal JSON from \"%s\" for %v", run.RawResultsURL, run)
					return
				}
				if len(report.Results) == 0 {
					log.Warningf("Empty report from %v (%s)", run, run.RawResultsURL)
					return
				}

				err = reportBuf.Put(&Report{
					report,
					run.ID,
				})
				if err != nil {
					log.Fatal(err)
				}

				log.Infof("Put report for run ID %d", run.ID)
			}()
		}

		reportBuf.Put(io.EOF)
		log.Infof("Run processing complete")
	}()

	rowBuf := NewBuffer(ctx, "row", 4000)
	go func() {
		for {
			iReport, err := reportBuf.Get()
			if err != nil {
				log.Fatal(err)
			}
			report, ok := iReport.(*Report)
			if !ok {
				if iReport.(error) != io.EOF {
					log.Fatalf("Expected *metrics.TestResultsReport or io.EOF but got %v", iReport)
				} else {
					log.Infof("Report processing complete")
					break
				}
			}

			log.Infof("Got report for run ID %d", report.RunID)

			func() {
				log.Infof("Queuing rows for run ID %d", report.RunID)

				rowCount := 0
				for _, r := range report.Results {
					if len(r.Subtests) == 0 {
						row := ResultRow{
							"run_id":  report.RunID,
							"test":    r.Test,
							"subtest": spanner.NullString{Valid: false},
							"result":  int64(metrics.TestStatusFromString(r.Status)),
						}
						if r.Message != nil {
							row["message"] = *r.Message
						} else {
							row["message"] = spanner.NullString{Valid: false}
						}
						err := rowBuf.Put(&row)
						if err != nil {
							log.Fatal(err)
						}

						rowCount++
					} else {
						for _, s := range r.Subtests {
							row := ResultRow{
								"run_id":  report.RunID,
								"test":    r.Test,
								"subtest": s.Name,
								"result":  int64(metrics.SubTestStatusFromString(s.Status)),
							}
							if s.Message != nil {
								row["message"] = *s.Message
							} else {
								row["message"] = spanner.NullString{Valid: false}
							}
							err := rowBuf.Put(&row)
							if err != nil {
								log.Fatal(err)
							}

							rowCount++
						}
					}
				}

				log.Infof("Queued %d rows for run ID %d", rowCount, report.RunID)
			}()
		}
	}()

	batchBuf := NewBuffer(ctx, "batch", 4000)
	go func() {
		done := false
		for !done {
			log.Infof("Gathering up to %d mutations for batch spanner write", 1000)

			muts := make([]*spanner.Mutation, 0, 1000)
			for i := 0; !done && i < 1000; i++ {
				iRow, err := rowBuf.Get()
				if err != nil {
					log.Fatal(err)
				}
				row, ok := iRow.(*ResultRow)
				if !ok {
					if iRow.(error) != io.EOF {
						log.Fatalf("Expected *ResultRow or io.EOF but got %v", iRow)
					} else {
						log.Infof("Row processing complete")
						done = true
						break
					}
				}

				mut := spanner.ReplaceMap(*outputSpannerTableID, *row)
				muts = append(muts, mut)
			}

			if len(muts) > 0 {
				log.Infof("Putting %d-mutation batch", len(muts))

				err := batchBuf.Put(&muts)
				if err != nil {
					log.Fatal(err)
				}

				log.Infof("Put %d-mutation batch", len(muts))
			}
		}
	}()

	func() {
		ws := semaphore.NewWeighted(4000)
		for {
			ws.Acquire(ctx, 1)

			iBatch, err := batchBuf.Get()
			if err != nil {
				log.Fatal(err)
			}
			batch, ok := iBatch.(*[]*spanner.Mutation)
			if !ok {
				if iBatch.(error) != io.EOF {
					log.Fatalf("Expected *[]*spanner.Mutation or io.EOF but got %v", iBatch)
				} else {
					log.Infof("Batch processing complete")
					break
				}
			}

			log.Infof("Got %d-mutation batch", len(*batch))

			go func() {
				_, err = sClient.Apply(ctx, *batch)
				if err != nil {
					log.Fatal(err)
				}

				log.Infof("Wrote %d-mutation batch to spanner", len(*batch))

				ws.Release(1)
			}()
		}
	}()

	log.Infof("Done writing data to spanner")
}
