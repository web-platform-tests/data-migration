package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/datastore"
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

var projectID *string
var inputGcsBucket *string
var gcpCredentialsFile *string

func init() {
	projectID = flag.String("project_id", "wptdashboard", "Google Cloud Platform project id")
	inputGcsBucket = flag.String("input_gcs_bucket", "wptd-results", "Google Cloud Storage bucket where shareded test results are stored")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")
}

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile | log.LUTC)
	flag.Parse()

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
			if res.Status != "OK" {
				tName := res.Test
				tBatch = append(tBatch, tName)
				resBatch = append(resBatch, results.KeyValue{
					results.Key{
						split.RunKey(tr.ID),
						split.TestKey(tests.NewTest(tests.Name(tName)).ID()),
					},
					results.Value(uint8(metrics.TestStatusFromString(res.Status))),
				})
			}
			for _, sub := range res.Subtests {
				sName := res.Test + ":" + sub.Name
				tBatch = append(tBatch, sName)
				resBatch = append(resBatch, results.KeyValue{
					results.Key{
						split.RunKey(tr.ID),
						split.TestKey(tests.NewTest(tests.Name(sName)).ID()),
					},
					results.Value(uint8(metrics.SubTestStatusFromString(sub.Status))),
				})
			}
		}

		tns.PutBatch(tBatch)
		ress.PutBatch(resBatch)

		runTestQueries()
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
	tq := tests.Query("/2dcontext/building-paths/")
	q := store.Query{
		TestQuery: &tq,
		/*RunQuery: &runs.Query{
			Predicate: r.EQ(
				r.Property{
					PropertyName: "BrowserName",
				},
				r.Constant(reflect.ValueOf("chrome")),
			),
		},
		ResultQuery: &results.Query{
			Predicate: r.EQ(
				r.INDEX(r.Constant(reflect.ValueOf(0))),
				r.Constant(reflect.ValueOf(uint8(metrics.TestStatusFromString("PASS")))),
			),
		},*/
	}

	start := time.Now()
	res, err := s.Find(q)
	log.Printf("INFO: Query time: %v", time.Now().Sub(start))
	if err != nil {
		log.Printf("ERRO: %v", err)
		return
	}
	log.Printf("INFO: Query yielded %d test across %d runs", len(res.Tests), len(res.Runs))
}
