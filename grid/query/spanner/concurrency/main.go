package main

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	log "github.com/sirupsen/logrus"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	projectID          = "wptdashboard-staging"
	instanceID         = "wpt-results"
	databaseID         = "results-apep"
	resultsTableID     = "results"
	gcpCredentialsFile = "spanner_client-secret.json"

	runID = 4651974658097152

	maxResults = 1000000
)

var (
	baseStmt string
	params   map[string]interface{}
)

func init() {
	baseStmt = fmt.Sprintf("SELECT test_id, result FROM %s WHERE run_id = @run_id AND result != %d ORDER BY test_id, run_id", resultsTableID, shared.TestStatusPass)
	params = map[string]interface{}{
		"run_id": 4651974658097152,
	}
}

func limitBatch(numBatches, batchSize, n int) string {
	return fmt.Sprintf(baseStmt+" LIMIT %d OFFSET %d", batchSize, n*batchSize)
}

func main() {
	for batchSize := 1000; batchSize <= 4000; batchSize += 200 {
		numBatches := int(math.Ceil(float64(maxResults) / float64(batchSize)))
		log.Infof("Trying %d %d-size batches", numBatches, batchSize)
		runQuery(numBatches, batchSize)
	}
}

func runQuery(numBatches, batchSize int) {
	ctx := context.Background()
	spec := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	client, err := spanner.NewClient(ctx, spec, option.WithCredentialsFile(gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	start := time.Now()
	n := 0
	nm := &sync.Mutex{}
	var wg sync.WaitGroup
	for i := 0; i < numBatches; i++ {
		wg.Add(1)
		stmt := limitBatch(numBatches, batchSize, i)
		go func(stmt string, params map[string]interface{}) {
			defer wg.Done()

			tx := client.Single()
			defer tx.Close()

			j := 0
			it := tx.Query(ctx, spanner.Statement{
				SQL:    stmt,
				Params: params,
			})
			for _, itErr := it.Next(); itErr != iterator.Done; _, itErr = it.Next() {
				if itErr != nil {
					log.Errorf("Error processing rows: %v", itErr)
					break
				}
				j++
			}

			nm.Lock()
			n += j
			nm.Unlock()

			log.Infof("Finish %v %v", stmt, params)
		}(stmt, params)
	}
	wg.Wait()
	t := time.Now().Sub(start).Nanoseconds()
	log.Infof("%d,%d,%d,%d", numBatches, batchSize, n, t)
}
