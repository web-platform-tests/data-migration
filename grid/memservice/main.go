package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/web-platform-tests/wpt.fyi/api/query"

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
	numShards          = flag.Int("num_shards", runtime.NumCPU(), "Number of shards for spliting data for concurrent processing")
	numInitialRuns     = flag.Int("num_initial_runs", 50, "Number of latest runs to automatically load on startup")

	idx          *mem.TestResultIndex
	maxHeapAlloc = uint64(4.5e+10)
	monitorSleep = 2 * time.Second
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

	newIdx, err := idx.WithRunResults(mem.RunResults{
		RunID:   mem.RunID(run.ID),
		Results: report.Results,
	})
	if err != nil {
		return err
	}

	idx = newIdx
	return nil
}

func loadRunHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.URL == nil {
		http.Error(w, "Missing URL", http.StatusInternalServerError)
		return
	}
	reqQuery := r.URL.Query()
	runIDStr := reqQuery["run_id"]
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
	runIDStr := r.URL.Query()["run_id"]
	if len(runIDStr) == 0 {
		http.Error(w, "Missing run_id query parameter", http.StatusBadRequest)
		return
	}
	runIDs := make([]mem.RunID, 0, len(runIDStr))
	for _, idStr := range runIDStr {
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "Malformed run_id query parameter", http.StatusBadRequest)
			return
		}
		runIDs = append(runIDs, mem.RunID(id))
	}

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

	fable, err := memparser.Parse(qStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof("Processing query: %v", fable)
	t0 := time.Now()
	ts := idx.Query(fable.ToFilter())
	log.Infof("Query %v processed in %v", fable, time.Now().Sub(t0))

	log.Infof("Preparing results for %d tests", len(ts))
	t0 = time.Now()
	var wg sync.WaitGroup
	var names map[mem.TestID]string
	var results map[mem.TestID][]mem.ResultID
	var searchResults []query.SearchResult
	wg.Add(2)
	go func() {
		defer wg.Done()
		names = idx.GetNames(ts)
	}()
	go func() {
		defer wg.Done()
		results = idx.GetResults(runIDs, ts)
	}()
	wg.Wait()

	passes := make(map[string][]int)
	totals := make(map[string][]int)
	for _, t := range ts {
		name := names[t]
		ress := results[t]
		if passes[name] == nil {
			passes[name] = make([]int, len(results[t]))
		}
		if totals[name] == nil {
			totals[name] = make([]int, len(results[t]))
		}
		for i, res := range ress {
			if res == mem.ResultID(shared.TestStatusOK) || res == mem.ResultID(shared.TestStatusPass) {
				passes[name][i]++
			}
			if res != mem.ResultID(shared.TestStatusUnknown) {
				totals[name][i]++
			}
		}
	}
	searchResults = make([]query.SearchResult, 0, len(totals))
	for name, ttls := range totals {
		ps := passes[name]
		statuses := make([]query.LegacySearchRunResult, 0, len(ttls))
		for i := range ttls {
			statuses = append(statuses, query.LegacySearchRunResult{
				Passes: ps[i],
				Total:  ttls[i],
			})
		}
		searchResults = append(searchResults, query.SearchResult{
			Test:         name,
			LegacyStatus: statuses,
		})
	}
	sort.Sort(query.ByName(searchResults))
	log.Infof("Prepared results for %d tests in %v", len(ts), time.Now().Sub(t0))

	t0 = time.Now()
	log.Infof("Sending results for %d tests", len(ts))
	data, err := json.Marshal(query.SearchResponse{
		Results: searchResults,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(data)
	log.Infof("Sent results for %d tests in %v", len(ts), time.Now().Sub(t0))
}

func loadInitialRuns() {
	if numInitialRuns == nil || *numInitialRuns <= 0 {
		log.Infof("No initial runs")
		return
	}

	log.Infof("Loading initial runs")

	ctx := context.Background()
	ds, err := dsClient(ctx)
	if err != nil {
		log.Warningf("Error loading initial runs: %v", err)
		return
	}

	var runs []shared.TestRun
	keys, err := ds.GetAll(ctx, datastore.NewQuery("TestRun").Order("-CreatedAt").Limit(*numInitialRuns), &runs)
	if err != nil {
		log.Warningf("Error loading initial runs: %v", err)
		return
	}

	// TODO: This strategy performs copy-and-add for every run; consider loading
	// in larger batches.
	for i := range runs {
		runs[i].ID = keys[i].ID
		if err := loadRun(&runs[i]); err != nil {
			log.Warningf("Continuing despite error loading initial run: %v", err)
		} else {
			log.Infof("Loaded initial run %d", runs[i].ID)
		}
	}

	log.Infof("Initial runs loaded")
}

func monitor() {
	var stats runtime.MemStats
	for {
		runtime.ReadMemStats(&stats)
		if stats.HeapAlloc > maxHeapAlloc {
			log.Fatal("Out of memory")
		} else {
			log.Infof("Monitor: %d heap-allocated bytes OK", stats.HeapAlloc)
		}
		time.Sleep(monitorSleep)
	}
}

func init() {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})
	flag.Parse()
	idx = mem.NewIndex(runtime.NumCPU())
}

func main() {
	go monitor()
	log.Infof("Running with %d shards", runtime.NumCPU())
	http.HandleFunc("/load-run", loadRunHandler)
	http.HandleFunc("/q", qHandler)

	go loadInitialRuns()

	log.Infof("Listening on port %d", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
