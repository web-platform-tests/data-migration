package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/spanner"
	retry "github.com/avast/retry-go"
	farm "github.com/dgryski/go-farm"
	log "github.com/sirupsen/logrus"
	"github.com/web-platform-tests/results-analysis/metrics"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func ToNullString(s *string) spanner.NullString {
	if s != nil && *s != "" {
		return spanner.NullString{
			StringVal: *s,
			Valid:     true,
		}
	}

	return spanner.NullString{
		Valid: false,
	}
}

type Mutation interface {
	ToMutation(*spanner.Client, string) *spanner.Mutation
}

type Test struct {
	TestID      int64
	TestName    string
	SubtestName spanner.NullString
}

func NewTest(name string, sub *string) *Test {
	var id int64
	if sub != nil && *sub != "" {
		id = int64(farm.Fingerprint64([]byte(name + "\x00" + *sub)))
	} else {
		id = int64(farm.Fingerprint64([]byte(name)))
	}
	return &Test{
		TestID:      id,
		TestName:    name,
		SubtestName: ToNullString(sub),
	}
}

type Run struct {
	RunID           int64
	BrowserName     string
	BrowserVersion  string
	OSName          string
	OSVersion       string
	WPTRevisionHash []byte
	ResultsURL      spanner.NullString
	CreatedAt       time.Time
	TimeStart       time.Time
	TimeEnd         time.Time
	RawResultsURL   spanner.NullString
	Labels          []string
}

func NewRun(r *shared.TestRun) (*Run, error) {
	hash, err := hex.DecodeString(r.FullRevisionHash)
	if err != nil {
		return nil, err
	}
	return &Run{
		RunID:           r.ID,
		BrowserName:     r.BrowserName,
		BrowserVersion:  r.BrowserVersion,
		OSName:          r.OSName,
		OSVersion:       r.OSVersion,
		WPTRevisionHash: hash,
		ResultsURL:      ToNullString(&r.ResultsURL),
		CreatedAt:       r.CreatedAt,
		TimeStart:       r.TimeStart,
		TimeEnd:         r.TimeEnd,
		RawResultsURL:   ToNullString(&r.RawResultsURL),
		Labels:          r.Labels,
	}, nil
}

type Result struct {
	ResultID    int64
	Name        string
	Description spanner.NullString
}

func NewResult(name string, desc *string) *Result {
	id := shared.TestStatusValueFromString(name)
	return &Result{
		ResultID:    id,
		Name:        name,
		Description: ToNullString(desc),
	}
}

type TestRun struct {
	TestID int64
	RunID  int64
}

type TestResult struct {
	TestID   int64
	ResultID int64
}

type RunResult struct {
	RunID    int64
	ResultID int64
}

type TestRunResult struct {
	TestID   int64
	RunID    int64
	ResultID int64
	Message  spanner.NullString
}

type Structs struct {
	Tests          map[int64]*Test
	Runs           map[int64]*Run
	Results        map[int64]*Result
	TestRuns       map[int64]map[int64]*TestRun
	TestResults    map[int64]map[int64]*TestResult
	RunResults     map[int64]map[int64]*RunResult
	TestRunResults map[int64]map[int64]map[int64]*TestRunResult
}

func NewStructs() *Structs {
	return &Structs{
		make(map[int64]*Test),
		make(map[int64]*Run),
		make(map[int64]*Result),
		make(map[int64]map[int64]*TestRun),
		make(map[int64]map[int64]*TestResult),
		make(map[int64]map[int64]*RunResult),
		make(map[int64]map[int64]map[int64]*TestRunResult),
	}
}

func (s *Structs) AddTest(t *Test) {
	s.Tests[t.TestID] = t
}

func (s *Structs) AddRun(r *Run) {
	s.Runs[r.RunID] = r
}

func (s *Structs) AddResult(r *Result) {
	s.Results[r.ResultID] = r
}

func (s *Structs) AddTestRun(t *Test, r *Run) {
	if _, ok := s.TestRuns[t.TestID]; !ok {
		s.TestRuns[t.TestID] = make(map[int64]*TestRun)
	}
	s.TestRuns[t.TestID][r.RunID] = &TestRun{
		TestID: t.TestID,
		RunID:  r.RunID,
	}
}

func (s *Structs) AddTestResult(t *Test, r *Result) {
	if _, ok := s.TestResults[t.TestID]; !ok {
		s.TestResults[t.TestID] = make(map[int64]*TestResult)
	}
	s.TestResults[t.TestID][r.ResultID] = &TestResult{
		TestID:   t.TestID,
		ResultID: r.ResultID,
	}
}

func (s *Structs) AddRunResult(run *Run, res *Result) {
	if _, ok := s.RunResults[run.RunID]; !ok {
		s.RunResults[run.RunID] = make(map[int64]*RunResult)
	}
	s.RunResults[run.RunID][res.ResultID] = &RunResult{
		RunID:    run.RunID,
		ResultID: res.ResultID,
	}
}

func (s *Structs) AddTestRunResult(t *Test, run *Run, res *Result, message *string) {
	msg := ToNullString(message)
	if _, ok := s.TestRunResults[t.TestID]; !ok {
		s.TestRunResults[t.TestID] = make(map[int64]map[int64]*TestRunResult)
	}
	if _, ok := s.TestRunResults[t.TestID][run.RunID]; !ok {
		s.TestRunResults[t.TestID][run.RunID] = make(map[int64]*TestRunResult)
	}
	s.TestRunResults[t.TestID][run.RunID][res.ResultID] = &TestRunResult{
		TestID:   t.TestID,
		RunID:    run.RunID,
		ResultID: res.ResultID,
		Message:  msg,
	}
}

func (s *Structs) ToMutations() ([]*spanner.Mutation, []*spanner.Mutation, []*spanner.Mutation, error) {
	m1s := make([]*spanner.Mutation, 0, len(s.Tests)+len(s.Runs)+len(s.Results))
	m2s := make([]*spanner.Mutation, 0, len(s.TestRuns)+len(s.TestResults)+len(s.RunResults))
	m3s := make([]*spanner.Mutation, 0, len(s.TestRunResults))
	for _, t := range s.Tests {
		m, err := spanner.InsertOrUpdateStruct("Tests", t)
		if err != nil {
			return nil, nil, nil, err
		}
		m1s = append(m1s, m)
	}
	for _, r := range s.Runs {
		m, err := spanner.InsertOrUpdateStruct("Runs", r)
		if err != nil {
			return nil, nil, nil, err
		}
		m1s = append(m1s, m)
	}
	for _, r := range s.Results {
		m, err := spanner.InsertOrUpdateStruct("Results", r)
		if err != nil {
			return nil, nil, nil, err
		}
		m1s = append(m1s, m)
	}
	for _, m1 := range s.TestRuns {
		for _, tr := range m1 {
			m, err := spanner.InsertOrUpdateStruct("TestRuns", tr)
			if err != nil {
				return nil, nil, nil, err
			}
			m2s = append(m2s, m)
			m, err = spanner.InsertOrUpdateStruct("RunTests", tr)
			if err != nil {
				return nil, nil, nil, err
			}
			m2s = append(m2s, m)
		}
	}
	for _, m1 := range s.TestResults {
		for _, tr := range m1 {
			m, err := spanner.InsertOrUpdateStruct("TestResults", tr)
			if err != nil {
				return nil, nil, nil, err
			}
			m2s = append(m2s, m)
			m, err = spanner.InsertOrUpdateStruct("ResultTests", tr)
			if err != nil {
				return nil, nil, nil, err
			}
			m2s = append(m2s, m)
		}
	}
	for _, m1 := range s.RunResults {
		for _, rr := range m1 {
			m, err := spanner.InsertOrUpdateStruct("RunResults", rr)
			if err != nil {
				return nil, nil, nil, err
			}
			m2s = append(m2s, m)
			m, err = spanner.InsertOrUpdateStruct("ResultRuns", rr)
			if err != nil {
				return nil, nil, nil, err
			}
			m2s = append(m2s, m)
		}
	}
	for _, m1 := range s.TestRunResults {
		for _, m2 := range m1 {
			for _, trr := range m2 {
				m, err := spanner.InsertOrUpdateStruct("TestRunResults", trr)
				if err != nil {
					return nil, nil, nil, err
				}
				m3s = append(m3s, m)
				m, err = spanner.InsertOrUpdateStruct("TestResultRuns", trr)
				if err != nil {
					return nil, nil, nil, err
				}
				m3s = append(m3s, m)
				m, err = spanner.InsertOrUpdateStruct("RunTestResults", trr)
				if err != nil {
					return nil, nil, nil, err
				}
				m3s = append(m3s, m)
				m, err = spanner.InsertOrUpdateStruct("RunResultTests", trr)
				if err != nil {
					return nil, nil, nil, err
				}
				m3s = append(m3s, m)
				m, err = spanner.InsertOrUpdateStruct("ResultTestRuns", trr)
				if err != nil {
					return nil, nil, nil, err
				}
				m3s = append(m3s, m)
				m, err = spanner.InsertOrUpdateStruct("ResultRunTests", trr)
				if err != nil {
					return nil, nil, nil, err
				}
				m3s = append(m3s, m)
			}
		}
	}

	return m1s, m2s, m3s, nil
}

var (
	projectID          = flag.String("project_id", "wptdashboard-staging", "Google Cloud Platform project id")
	inputGcsBucket     = flag.String("input_gcs_bucket", "wptd-results", "Google Cloud Storage bucket where shareded test results are stored")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "spanner_client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")
)

const (
	outputSpannerInstanceID = "wpt-results"
	outputSpannerDatabaseID = "results-bulgu"
	numConcurrentRuns       = 2
	numConcurrentBatches    = 50
)

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
	return keys[0:8], testRuns[0:8]
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

func uploadReport(ctx context.Context, client *spanner.Client, run *shared.TestRun, report *metrics.TestResultsReport, batchSize int, numConcurrentBatches int64) error {
	var err error

	log.Infof("Preparing data for run %d", run.ID)

	ss := NewStructs()

	r, err := NewRun(run)
	if err != nil {
		return err
	}
	ss.AddRun(r)

	for _, result := range report.Results {
		if len(result.Subtests) == 0 {
			t := NewTest(result.Test, nil)
			ss.AddTest(t)
			res := NewResult(result.Status, nil)
			ss.AddResult(res)
			ss.AddTestRun(t, r)
			ss.AddTestResult(t, res)
			ss.AddRunResult(r, res)
			ss.AddTestRunResult(t, r, res, result.Message)
		} else {
			for _, s := range result.Subtests {
				t := NewTest(result.Test, &s.Name)
				ss.AddTest(t)
				res := NewResult(s.Status, nil)
				ss.AddResult(res)
				ss.AddTestRun(t, r)
				ss.AddTestResult(t, res)
				ss.AddRunResult(r, res)
				ss.AddTestRunResult(t, r, res, s.Message)
			}
		}
	}

	log.Infof("Generating row-based mutations for run %d", run.ID)
	r1s, r2s, r3s, err := ss.ToMutations()
	if err != nil {
		return err
	}
	numRows := len(r2s) + len(r2s) + len(r3s)
	log.Infof("Generated %d rows for run %d", numRows, run.ID)

	log.Infof("Writing batches for %d-row run %d", numRows, run.ID)

	s := semaphore.NewWeighted(numConcurrentBatches)
	writeBatch := func(batchSync *semaphore.Weighted, rowGroupSync *sync.WaitGroup, rows []*spanner.Mutation, m, n int) {
		defer rowGroupSync.Done()
		defer batchSync.Release(1)
		batch := rows[m:n]

		err := retry.Do(func() error {
			log.Infof("Writing batch for %d-row run %d: [%d,%d)", len(rows), run.ID, m, n)

			newCtx, cancel := context.WithTimeout(ctx, time.Second*60)
			defer cancel()

			_, err := client.Apply(newCtx, batch)
			if err != nil {
				log.Errorf("Error writing batch for %d-row run %d: %v", len(rows), run.ID, err)
				return err
			}

			log.Infof("Wrote batch for %d-row run %d: [%d,%d)", len(rows), run.ID, m, n)
			return nil
		}, retry.Attempts(5), retry.OnRetry(func(n uint, err error) {
			log.Warningf("Retrying failed batch batch for %d-row run %d: [%d,%d): %v", len(rows), run.ID, m, n, err)
		}))
		if err != nil {
			log.Fatal(err)
		}
	}
	writeRows := func(rows []*spanner.Mutation) *sync.WaitGroup {
		var wg sync.WaitGroup
		var end int
		for end = batchSize; end <= len(rows); end += batchSize {
			wg.Add(1)
			s.Acquire(ctx, 1)
			go writeBatch(s, &wg, rows[0:], end-batchSize, end)
		}
		if end != len(rows) {
			wg.Add(1)
			s.Acquire(ctx, 1)
			log.Infof("Writing small batch for %d-row run %d: [%d,%d)", len(rows), run.ID, end-batchSize, len(rows))
			go writeBatch(s, &wg, rows[0:], end-batchSize, len(rows))
			log.Infof("Wrote small batch for %d-row run %d: [%d,%d)", len(rows), run.ID, end-batchSize, len(rows))
		}
		return &wg
	}

	log.Infof("Writing %d layer-1 rows run %d", len(r1s), run.ID)
	writeRows(r1s).Wait()

	log.Infof("Writing %d layer-2 rows run %d", len(r2s), run.ID)
	writeRows(r2s).Wait()

	log.Infof("Writing %d layer-3 rows run %d", len(r3s), run.ID)
	writeRows(r3s).Wait()

	log.Infof("Wrote batches for %d-row run %d", numRows, run.ID)

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

	sSpec := fmt.Sprintf("projects/%s/instances/%s/databases/%s", *projectID, outputSpannerInstanceID, outputSpannerDatabaseID)
	sClient, err := spanner.NewClient(ctx, sSpec, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}

	s := semaphore.NewWeighted(numConcurrentRuns)
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
					return uploadReport(ctx, sClient, run, report, 1000, numConcurrentBatches)
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
	s.Acquire(ctx, numConcurrentRuns)

	log.Infof("Import complete!")
}
