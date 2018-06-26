package main

import (
	"context"
	"flag"
	"log"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
)

var projectID *string
var gcpCredentialsFile *string
var outputBTInstanceID *string
var outputBTTableID *string
var outputBTFamily *string

func init() {
	projectID = flag.String("project_id", "wptdashboard", "Google Cloud Platform project id")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")
	outputBTInstanceID = flag.String("output_bt_instance_id", "wpt-results-matrix", "Output BigTable instance ID")
	outputBTTableID = flag.String("output_bt_table_id", "wpt-results-per-test", "Output BigTable table ID")
	outputBTFamily = flag.String("output_bt_family", "tests", "Output BigTable column family for test results")
}

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile | log.LUTC)
	flag.Parse()

	ctx := context.Background()

	btClient, err := bigtable.NewClient(ctx, *projectID, *outputBTInstanceID, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	tbl := btClient.Open(*outputBTTableID)

	start := time.Now()
	rows := make([]bigtable.Row, 0)
	err = tbl.ReadRows(
		ctx,
		bigtable.PrefixRange("de6ce4a47fe10bc7a86947ca9ff7dbc48c2d4648#chrome-62.0-linux-3.16@2017-09-30T14:26:23Z$"),
		func(r bigtable.Row) bool {
			rows = append(rows, r)
			return true
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	end := time.Now()
	log.Printf("Query time: %v ; number of rows: %d", end.Sub(start), len(rows))
	if len(rows) > 0 {
		for _, item := range rows[0][*outputBTFamily] {
			log.Printf("First row:\n  Row: %s\n  Col: %s\n  Value: %s\n\n", item.Row, item.Column, string(item.Value))
		}
		last := make([]string, 0)
		for _, item := range rows[len(rows)-1][*outputBTFamily] {
			log.Printf("Last row:\n  Row: %s\n  Col: %s\n  Value: %s\n\n", item.Row, item.Column, string(item.Value))
			last = append(last, string(item.Value))
		}
	}
}
