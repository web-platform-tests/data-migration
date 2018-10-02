package main

import (
	"context"

	"cloud.google.com/go/bigtable"
	log "github.com/sirupsen/logrus"
	"github.com/web-platform-tests/data-migration/grid/query/bigtable/shared"
	"google.golang.org/api/option"
)

const (
	projectID          = "wptdashboard"
	gcpCredentialsFile = "bt_client-secret.json"
	outputBTInstanceID = "wpt-results-matrix"
	outputBTTableID    = "wpt-results-per-test"
	outputBTFamily     = "tests"
)

var (
	testQueries = []shared.TestQuery{
		shared.TestQuery{
			ID: "RUN01RES01",
			RowSet: bigtable.RowRangeList{
				bigtable.PrefixRange("de3ae39cb59880a8245431e7f09817a2a4dad1a3#firefox-60.0.2-linux-4.4@2018-06-16T02:30:43Z$"),
				bigtable.PrefixRange("de3ae39cb59880a8245431e7f09817a2a4dad1a3#firefox-experimental-62.0a1-linux-4.4@2018-06-16T01:48:04Z$"),
			},
			IterFunc: func(bigtable.Row) bool { return true },
			// Opts:     []bigtable.ReadOption{bigtable.LimitRows(10)},
		},
	}
)

func main() {
	ctx := context.Background()

	btClient, err := bigtable.NewClient(ctx, projectID, outputBTInstanceID, option.WithCredentialsFile(gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	tbl := btClient.Open(outputBTTableID)

	for _, q := range testQueries {
		ts, err := shared.RunTestQuery(ctx, tbl, q, 1)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Infof("Times for %s: %v", q.ID, ts)
		}
	}
}
