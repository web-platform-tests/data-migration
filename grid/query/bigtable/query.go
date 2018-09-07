package main

import (
	"context"
	"time"

	"cloud.google.com/go/bigtable"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

const (
	projectID          = "wptdashboard"
	gcpCredentialsFile = "client-secret.json"
	outputBTInstanceID = "wpt-results-matrix"
	outputBTTableID    = "wpt-results-per-test"
	outputBTFamily     = "tests"
)

func main() {
	ctx := context.Background()

	btClient, err := bigtable.NewClient(ctx, projectID, outputBTInstanceID, option.WithCredentialsFile(gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	tbl := btClient.Open(outputBTTableID)

	var rows []bigtable.Row
	for i := 0; i < 20; i++ {
		rows, err = testLoadRuns(
			"One complete run",
			tbl,
			ctx,
			bigtable.PrefixRange("de6ce4a47fe10bc7a86947ca9ff7dbc48c2d4648#chrome-62.0-linux-3.16@2017-09-30T14:26:23Z$"),
			func(r bigtable.Row) bool { return true },
		)
		rows, err = testLoadRuns(
			"Some data; any data",
			tbl,
			ctx,
			bigtable.PrefixRange(""),
			func(r bigtable.Row) bool { return true },
			bigtable.LimitRows(100),
		)

		if err != nil {
			log.Fatal(err)
		}
	}

	if len(rows) > 0 {

		for i := 0; i < 5 && i < len(rows); i++ {
			for _, item := range rows[i][outputBTFamily] {
				log.Printf("%dth row:\n  Row: %s\n  Col: %s\n  Value: %s\n\n", i, item.Row, item.Column, string(item.Value))
			}
		}

		for i := len(rows) - 6; i >= 5 && i < len(rows); i++ {
			for _, item := range rows[i][outputBTFamily] {
				log.Printf("%dth row:\n  Row: %s\n  Col: %s\n  Value: %s\n\n", i, item.Row, item.Column, string(item.Value))
			}
		}
	}
}

func testLoadRuns(name string, tbl *bigtable.Table, ctx context.Context, rowSet bigtable.RowSet, f func(bigtable.Row) bool, opts ...bigtable.ReadOption) ([]bigtable.Row, error) {
	start := time.Now()
	rows := make([]bigtable.Row, 0)
	err := tbl.ReadRows(ctx, rowSet, func(r bigtable.Row) bool {
		rows = append(rows, r)
		return f(r)
	}, opts...)
	if err != nil {
		log.Error(err)
	}
	end := time.Now()
	log.Printf("%s: query time: %v ; number of rows: %d", name, end.Sub(start), len(rows))

	return rows, err
}
