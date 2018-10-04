package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	log "github.com/sirupsen/logrus"
	"github.com/web-platform-tests/results-analysis/metrics"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	projectID          = "wptdashboard-staging"
	gcpCredentialsFile = "spanner_client-secret.json"
	instanceID         = "wpt-results-staging"
	databaseID         = "wpt-results-staging"
	tableID            = "results"
)

type TestQuery struct {
	ID      string
	Queries []spanner.Statement
}

var (
	resQueries = map[string][]spanner.Statement{
		"RES01": []spanner.Statement{
			spanner.Statement{
				"SELECT test, subtest, result, message FROM results@{FORCE_INDEX=results_by_run_id} WHERE run_id = @run_id",
				map[string]interface{}{},
			},
		},
		"RES02": []spanner.Statement{
			spanner.Statement{
				"SELECT test, subtest, result, message FROM results@{FORCE_INDEX=results_by_run_id} WHERE run_id = @run_id AND (LOWER(test) LIKE @pattern OR LOWER(subtest) LIKE @pattern)",
				map[string]interface{}{
					"pattern": "%webaudio%",
				},
			},
		},
		"RES03": []spanner.Statement{
			spanner.Statement{
				"SELECT test, subtest, result, message FROM results@{FORCE_INDEX=results_by_run_id_result} WHERE run_id = @run_id AND result != @result",
				map[string]interface{}{
					// NOTE: TEST_[result] and SUB_TEST_[result] enums from metrics
					// package do not always line up! Probably need to map them to a
					// sainer enum in Spanner. For now, use TIMEOUT, which does line up
					// for test queries.
					"result": int64(metrics.TestStatusFromString("TEST_TIMEOUT")),
				},
			},
		},
		"RES04": []spanner.Statement{
			spanner.Statement{
				"SELECT test, subtest, result, message FROM results@{FORCE_INDEX=results_by_run_id_result} WHERE run_id = @run_id AND result = @result",
				map[string]interface{}{
					// NOTE: TEST_[result] and SUB_TEST_[result] enums from metrics
					// package do not always line up! Probably need to map them to a
					// sainer enum in Spanner. For now, use test-OK / subtest-PASS as
					// PASS.
					"result": int64(metrics.TestStatusFromString("TEST_OK")),
				},
			},
			spanner.Statement{
				"SELECT test, subtest, result, message FROM results@{FORCE_INDEX=results_by_run_id_result} WHERE run_id = @run_id AND result != @result",
				map[string]interface{}{
					// NOTE: TEST_[result] and SUB_TEST_[result] enums from metrics
					// package do not always line up! Probably need to map them to a
					// sainer enum in Spanner. For now, use test-OK / subtest-PASS as
					// PASS.
					"result": int64(metrics.TestStatusFromString("TEST_OK")),
				},
			},
		},
		"RES05": []spanner.Statement{
			spanner.Statement{
				"SELECT test, subtest, result, message FROM results@{FORCE_INDEX=results_by_run_id_result} WHERE run_id = @run_id AND (LOWER(test) LIKE @pattern OR LOWER(subtest) LIKE @pattern) AND result != @result",
				map[string]interface{}{
					"pattern": "%cssom%",
					// NOTE: TEST_[result] and SUB_TEST_[result] enums from metrics
					// package do not always line up! Probably need to map them to a
					// sainer enum in Spanner. For now, use TIMEOUT, which does line up
					// for test queries.
					"result": int64(metrics.TestStatusFromString("TEST_TIMEOUT")),
				},
			},
		},
		"RES06": []spanner.Statement{
			spanner.Statement{
				"SELECT test, subtest, result, message FROM results@{FORCE_INDEX=results_by_run_id} WHERE run_id = @run_id AND (LOWER(test) LIKE @pattern1 OR LOWER(subtest) LIKE @pattern1 OR LOWER(test) LIKE @pattern2 OR LOWER(subtest) LIKE @pattern2)",
				map[string]interface{}{
					"pattern1": "%webaudio%",
					"pattern2": "%webusb%",
				},
			},
		},
	}
	runIDs = map[string][]int64{
		// Firefox stable vs. experimental.
		"RUN01": []int64{
			4898202331381760,
			5485518036926464,
		},
		// Four hash-aligned runs.
		"RUN02": []int64{
			4810756193255424,
			6254197527805952,
			6329942967058432,
			6305762435399680,
		},
		// Time series of Chrome experimental.
		"RUN03": []int64{
			5182433402028030,
			5972722551095290,
			5185128728887290,
			5128297620963320,
			6305762435399680,
			5156518425001980,
			5106730979557370,
			6284018525929470,
			5104455385088000,
			5171435668504570,
			5111633147854840,
			5350060170674170,
			5143230215618560,
			4786352927277050,
			5198007087661050,
			5134834091425790,
			6194485100806140,
			5067827903987710,
			5184600951226360,
			5093232903979000,
			5163674259947520,
			6192792346820600,
			5488665442648060,
			6227504809377790,
			5119259734704120,
			5394310111428600,
			4790038143434750,
			4831360158007290,
			6302168621514750,
			5166363882553340,
			4874009518800890,
			5112835134717950,
			5151910646513660,
			5109548981420030,
			4910885906677760,
			6197543218184190,
			5071643311341560,
			4794958867333120,
			6246242543730680,
			5145245394141180,
		},
	}
	testQueries = make(map[string][]spanner.Statement)
)

func init() {
	for resKey, resQs := range resQueries {
		for runKey, ids := range runIDs {
			key := runKey + resKey
			qs := make([]spanner.Statement, 0, len(ids))
			for i, id := range ids {
				q := resQs[i%len(resQs)]
				params := make(map[string]interface{})
				for k, v := range q.Params {
					params[k] = v
				}
				params["run_id"] = id
				q.Params = params
				qs = append(qs, q)
			}
			testQueries[key] = qs
		}
	}
}

func main() {
	ctx := context.Background()

	spec := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := spanner.NewClient(ctx, spec, option.WithCredentialsFile(gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}

	testIDs := make([]string, 0, len(testQueries))
	for id := range testQueries {
		testIDs = append(testIDs, id)
	}
	sort.Strings(testIDs)
	log.Infof("Running spanner tests on %v", testIDs)

	for i := 0; i < 6; i++ {
		for id, stmts := range testQueries {
			n, t, err := RunTestQuery(ctx, client, stmts)
			if err != nil {
				log.Fatal(err)
			} else {
				log.Infof("Executed query %s: %d rows in %dms (%v)", id, n, int64(t/time.Millisecond), t)
			}
		}
	}
}

func RunTestQuery(ctx context.Context, client *spanner.Client, stmts []spanner.Statement) (n int, t time.Duration, err error) {
	var tx *spanner.ReadOnlyTransaction
	if len(stmts) == 1 {
		tx = client.Single()
	} else {
		tx = client.ReadOnlyTransaction()
	}
	defer tx.Close()

	sm := &sync.Mutex{}
	var wg sync.WaitGroup
	start := time.Now()
	for _, s := range stmts {
		wg.Add(1)
		go func(ctx context.Context, client *spanner.Client, tx *spanner.ReadOnlyTransaction, s spanner.Statement) {
			defer wg.Done()

			i := 0
			it := tx.Query(ctx, s)
			for _, itErr := it.Next(); itErr != iterator.Done; _, itErr = it.Next() {
				if itErr != nil {
					err = itErr
					break
				}
				i++
			}
			log.Infof("Rows: %d ; Sub-query: %v", i, s)

			sm.Lock()
			n += i
			sm.Unlock()
		}(ctx, client, tx, s)
	}
	wg.Wait()
	end := time.Now()
	return n, end.Sub(start), err
}
