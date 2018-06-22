package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"

	"github.com/web-platform-tests/results-analysis/metrics"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

var projectID *string
var gcsBucket *string

const gcsPrefix string = "https://storage.googleapis.com/"

func init() {
	projectID = flag.String("project", "wptdashboard-staging", "Google Cloud Platform project ID")
	gcsBucket = flag.String("bucket", "wptd-results-staging", "Only process reports in this bucket")
}

func process(ctx context.Context, ds *datastore.Client, gcs *storage.Client, key *datastore.Key) error {
	var testRun shared.TestRun
	if err := ds.Get(ctx, key, &testRun); err != nil {
		return err
	}

	if testRun.RawResultsURL == "" {
		log.Printf("TestRun %d doesn't have a raw report, skipping.", key.ID)
		return nil
	}
	if !strings.HasPrefix(testRun.RawResultsURL, gcsPrefix+*gcsBucket+"/") {
		log.Printf("TestRun %d 's raw report is not in %s, skipping.", key.ID, *gcsBucket)
		return nil
	}
	if !strings.HasSuffix(testRun.RawResultsURL, "report.json") {
		log.Printf("Unrecognized report URL %s.", testRun.RawResultsURL)
		return nil
	}

	bucket := gcs.Bucket(*gcsBucket)
	reportPath := strings.TrimPrefix(testRun.RawResultsURL, gcsPrefix+*gcsBucket+"/")
	reportFile := bucket.Object(reportPath)
	logPath := strings.TrimSuffix(reportPath, "report.json") + "migration.log"
	logFile := bucket.Object(logPath)

	_, err := logFile.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		log.Printf("TestRun %d doesn't have a migrated raw report, skipping.", key.ID)
		return nil
	}
	if err != nil {
		return err
	}

	reader, err := reportFile.NewReader(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()

	var report metrics.TestResultsReport
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(&report); err != nil {
		return err
	}

	report.RunInfo.ProductAtRevision = testRun.ProductAtRevision
	report.RunInfo.BrowserName = strings.TrimSuffix(report.RunInfo.BrowserName, "-experimental")
	output, _ := json.Marshal(report.RunInfo)
	log.Println(string(output))

	// Take this chance to use the new dash-separated URL schema.
	newReportPath := strings.Replace(reportPath, "_", "-", -1)
	newReportFile := bucket.Object(newReportPath)
	writer := newReportFile.NewWriter(ctx)
	log.Printf("Writing to %s", newReportPath)
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(report); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	newReportURL := gcsPrefix + *gcsBucket + "/" + newReportPath
	log.Printf("New report URL: %s", newReportURL)

	testRun.RawResultsURL = newReportURL
	if _, err := ds.Put(ctx, key, testRun); err != nil {
		return err
	}

	// To be safe, we don't delete the old reports right away.
	// Instead, print a list of commands to run later.
	fmt.Printf("gsutil rm -r gs://%s/%s\n", *gcsBucket, strings.TrimSuffix(reportPath, "report.json"))

	return nil
}

func main() {
	flag.Parse()

	ctx := context.Background()
	ds, err := datastore.NewClient(ctx, *projectID)
	if err != nil {
		panic(err)
	}
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	query := datastore.NewQuery("TestRun").KeysOnly()
	keys, err := ds.GetAll(ctx, query, nil)
	if err != nil {
		panic(err)
	}
	for i, key := range keys {
		log.Printf("[%d/%d] Processing TestRun %d...", i+1, len(keys), key.ID)
		err := process(ctx, ds, gcs, key)
		if err != nil {
			log.Printf("ERROR cannot process TestRun %d: %v", key.ID, err)
		}
	}
}
