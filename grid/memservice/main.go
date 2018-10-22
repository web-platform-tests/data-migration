package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/api/option"

	"cloud.google.com/go/datastore"
	log "github.com/sirupsen/logrus"
	"github.com/web-platform-tests/data-migration/grid/mem"
	"github.com/web-platform-tests/data-migration/grid/memparser"
	"github.com/web-platform-tests/results-analysis/metrics"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

var (
	port               = flag.Int("port", 8080, "Port to listen on")
	projectID          = flag.String("project_id", "wptdashboard-staging", "Google Cloud Platform project id")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")

	tests   = mem.NewTests()
	results = mem.NewResults()
)

func dsClient(ctx context.Context) (*datastore.Client, error) {
	if *gcpCredentialsFile != "" {
		return datastore.NewClient(ctx, *projectID, option.WithCredentialsFile(*gcpCredentialsFile))
	}
	return datastore.NewClient(ctx, *projectID)
}

func loadRun(run *shared.TestRun) error {
	resp, err := http.Get(run.RawResultsURL)
	if err != nil {
		return fmt.Errorf("Failed to fetch %s", run.RawResultsURL)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bad status from %s: %d", run.RawResultsURL, resp.StatusCode)
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Failed to read response body from %s", run.RawResultsURL)
	}
	var report metrics.TestResultsReport
	err = json.Unmarshal(data, &report)
	if err != nil {
		return fmt.Errorf("Failed to unmarshal response body from %s", run.RawResultsURL)
	}
	if len(report.Results) == 0 {
		return fmt.Errorf("Report at %s contains no results", run.RawResultsURL)
	}

	for _, r := range report.Results {
		t, err := tests.Add(r.Test, nil)
		if err != nil {
			return err
		}
		s := shared.TestStatusValueFromString(r.Status)
		results.Add(mem.RunID(run.ID), mem.ResultID(s), t)
		if len(r.Subtests) != 0 {
			for _, r2 := range r.Subtests {
				t, err := tests.Add(r.Test, &r2.Name)
				if err != nil {
					return err
				}
				s := shared.TestStatusValueFromString(r2.Status)
				results.Add(mem.RunID(run.ID), mem.ResultID(s), t)
			}
		}
	}

	return nil
}

func loadRunHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.URL == nil {
		http.Error(w, "Missing URL", http.StatusInternalServerError)
		return
	}
	runIDStr := r.URL.Query()["run_id"]
	if len(runIDStr) == 0 {
		http.Error(w, "Missing run_id query parameter", http.StatusBadRequest)
		return
	}
	if len(runIDStr) > 1 {
		http.Error(w, "Too many run_id query parameters", http.StatusBadRequest)
		return
	}
	runID, err := strconv.ParseInt(runIDStr[0], 10, 64)
	if err != nil {
		http.Error(w, "Malformed run_id query parameter", http.StatusBadRequest)
		return
	}

	ds, err := dsClient(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var run shared.TestRun
	err = ds.Get(ctx, &datastore.Key{
		Kind: "TestRun",
		ID:   runID,
	}, &run)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	run.ID = runID

	log.Infof("Loading run %v", run)

	err = loadRun(&run)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte("Run loaded"))
}

func qHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL == nil {
		http.Error(w, "Missing URL", http.StatusInternalServerError)
		return
	}
	// runIDStr := r.URL.Query()["run_id"]
	// if len(runIDStr) == 0 {
	// 	http.Error(w, "Missing run_id query parameter", http.StatusBadRequest)
	// 	return
	// }
	// if len(runIDStr) > 1 {
	// 	http.Error(w, "Too many run_id query parameters", http.StatusBadRequest)
	// 	return
	// }
	// runIDs := make([]int64, 0, len(runIDStr))
	// for _, idStr := range runIDStr {
	// 	id, err := strconv.ParseInt(idStr, 10, 64)
	// 	if err != nil {
	// 		http.Error(w, "Malformed run_id query parameter", http.StatusBadRequest)
	// 		return
	// 	}
	// 	runIDs = append(runIDs, id)
	// }

	qStrs := r.URL.Query()["q"]
	if len(qStrs) == 0 {
		http.Error(w, "Missing q query parameter", http.StatusBadRequest)
		return
	}
	if len(qStrs) > 1 {
		http.Error(w, "Too many q query parameters", http.StatusBadRequest)
		return
	}
	qStr := qStrs[0]

	qable, err := memparser.Parse(qStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof("Processing query: %v", qable)

	t0 := time.Now()
	c := qable.RunAll(tests, results)
	res := make([]mem.TestID, 0)
	for {
		v := <-c
		if v == tests.EOF() {
			break
		}
		res = append(res, v)
	}

	log.Infof("Query %v processed in %v", qable, time.Now().Sub(t0))

	data, err := json.Marshal(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func init() {
	flag.Parse()
}

func main() {
	http.HandleFunc("/load-run", loadRunHandler)
	http.HandleFunc("/q", qHandler)
	log.Infof("Listening on port %d", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
