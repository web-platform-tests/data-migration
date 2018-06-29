package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"runtime"
	"sort"
	"time"

	"cloud.google.com/go/datastore"
	r "github.com/web-platform-tests/data-migration/grid/reflect"
	"github.com/web-platform-tests/data-migration/grid/split"
	"github.com/web-platform-tests/data-migration/grid/split/results"
	"github.com/web-platform-tests/data-migration/grid/split/runs"
	"github.com/web-platform-tests/data-migration/grid/split/tests"
	"github.com/web-platform-tests/data-migration/grid/store"
	"github.com/web-platform-tests/results-analysis/metrics"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var rs runs.Runs
var tns tests.TestNames
var ress results.Results
var s store.Store
var trs []shared.TestRun
var trks []*datastore.Key
var maxHeapSize *uint64
var monitorSleep = 2 * time.Second

var projectID *string
var inputGcsBucket *string
var gcpCredentialsFile *string

func init() {
	projectID = flag.String("project_id", "wptdashboard", "Google Cloud Platform project id")
	inputGcsBucket = flag.String("input_gcs_bucket", "wptd-results", "Google Cloud Storage bucket where shareded test results are stored")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")
	maxHeapSize = flag.Uint64("max_heap_size", uint64(45e+9), "Maximum heap size before abort")
}

type bySubTestName []metrics.SubTest

func (s bySubTestName) Len() int {
	return len(s)
}

func (s bySubTestName) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s bySubTestName) Less(i, j int) bool {
	return s[i].Name < s[j].Name
}

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile | log.LUTC)
	flag.Parse()

	go monitor()

	//tns = tests.NewFuzzySearchTestNames()
	//tns = tests.NewBleveTestNames()
	tns = tests.NewSTTestNames()
	rs = runs.NewRunSlice()
	ress = results.NewResultsMap()
	s = store.NewTriStore(tns, rs, ress)

	loadTestRuns()

	for _, tr := range trs {
		resp, err := http.Get(tr.RawResultsURL)
		if err != nil {
			log.Printf("WARN: Failed to load raw results from \"%s\" for %v", tr.RawResultsURL, tr)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("WARN: Non-OK HTTP status code of %d from \"%s\" for %v", resp.StatusCode, tr.RawResultsURL, tr)
			continue
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("WARN: Failed to read contents of \"%s\" for %v", tr.RawResultsURL, tr)
		}
		var report metrics.TestResultsReport
		err = json.Unmarshal(data, &report)
		if err != nil {
			log.Printf("WARN: Failed to unmarshal JSON from \"%s\" for %v", tr.RawResultsURL, tr)
			continue
		}

		rs.Put(runs.Run(tr))

		tBatch := make(tests.Names, 0, len(report.Results))
		resBatch := make([]results.KeyValue, 0, len(report.Results))
		for _, res := range report.Results {
			tName := res.Test
			tBatch = append(tBatch, tName)
			ress := results.KeyValue{
				results.Key{
					split.RunKey(tr.ID),
					split.TestKey(tests.NewTest(tests.Name(tName)).ID()),
				},
				results.Value(make([]split.TestStatus, 0, 1)),
			}
			if res.Status != "OK" {
				ress.Value = append(ress.Value, split.TestStatus(metrics.TestStatusFromString(res.Status)))
			}

			subs := bySubTestName(res.Subtests[0:])
			sort.Sort(subs)
			for _, sub := range subs {
				ress.Value = append(ress.Value, split.TestStatus(metrics.SubTestStatusFromString(sub.Status)))
			}

			if len(ress.Value) > 0 {
				resBatch = append(resBatch, ress)
			}
		}

		tns.PutBatch(tBatch)
		ress.PutBatch(resBatch)

		runTestQueries()
	}
}

func monitor() {
	var stats runtime.MemStats
	for {
		runtime.ReadMemStats(&stats)
		if stats.HeapAlloc > *maxHeapSize {
			log.Fatal("ERRO: Out of memory")
		}
		// } else {
		// 	log.Printf("INFO: Monitor: %d heap-allocated bytes OK", stats.HeapAlloc)
		// }
		time.Sleep(monitorSleep)
	}
}

func loadTestRuns() {
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, *projectID, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	query := datastore.NewQuery("TestRun").Order("-CreatedAt")
	trks = make([]*datastore.Key, 0)
	trs = make([]shared.TestRun, 0)
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
		trks = append(trks, key)
		trs = append(trs, testRun)
	}

	for i := range trs {
		trs[i].ID = trks[i].ID
	}
}

func runTestQueries() {
	q := store.Query{
		TestQuery: &tests.Query{
			Term: "/FileAPI/url/",
		},
		RunQuery: &runs.Query{
			Predicate: r.EQ(
				r.Property{
					PropertyName: "BrowserName",
				},
				r.Constant(reflect.ValueOf("chrome")),
			),
		},
		ResultQuery: &results.Query{
			Predicate: r.UnaryEagerArg{
				Op:  r.ANY(r.EQ(r.Constant(reflect.ValueOf(uint8(metrics.TestStatusFromString("PASS")))), nil)),
				Arg: r.INDEX(r.Constant(reflect.ValueOf(0))),
			},
		},
	}

	start := time.Now()
	res, err := s.Find(q)
	log.Printf("INFO: Query time: %v", time.Now().Sub(start))
	if err != nil {
		log.Printf("ERRO: %v", err)
		return
	}
	log.Printf("INFO: Query yielded %d tests, %d runs, and %d slices of results", len(res.Tests), len(res.Runs), len(res.Results))
	for _, t := range res.Tests {
		log.Printf("INFO: Matched test name: %s", t.Name())
	}
	for _, r := range res.Results {
		for _, t := range res.Tests {
			if t.ID() == tests.ID(r.Test) {
				log.Printf("INFO: Matched result test name: %s", t.Name())
			}
		}
	}
}
