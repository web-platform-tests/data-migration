package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	gcs "cloud.google.com/go/storage"
	"github.com/web-platform-tests/results-analysis/metrics"
	wptStorage "github.com/web-platform-tests/results-analysis/metrics/storage"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	billy "gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

var wptGitPath *string
var wptDataPath *string
var projectID *string
var inputGcsBucket *string
var outputGcsBucket *string
var wptdHost *string
var gcpCredentialsFile *string

func init() {
	_, srcFilePath, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal(errors.New("Failed to get golang source file path"))
	}
	defaultGitDir := filepath.Clean(path.Dir(srcFilePath) + "/../../.wpt")
	defaultDataDir := filepath.Clean(path.Dir(srcFilePath) + "/../../.cache/migration")
	wptGitPath = flag.String("wpt_git_path", defaultGitDir, "Path to WPT checkout")
	wptDataPath = flag.String("wpt_data_path", defaultDataDir, "Path to data directory for local data from Google Cloud Storage")
	projectID = flag.String("project_id", "wptdashboard", "Google Cloud Platform project id")
	inputGcsBucket = flag.String("input_gcs_bucket", "wptd", "Google Cloud Storage bucket where shareded test results are stored")
	outputGcsBucket = flag.String("output_gcs_bucket", "wptd-results", "Google Cloud Storage bucket where unified test results are stored")
	wptdHost = flag.String("wptd_host", "wpt.fyi", "Hostname of endpoint that serves WPT Dashboard data API")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "client-secret.json", "Path to credentials file for authenticating against Google Cloud Platform services")
}

func getRuns(ctx wptStorage.GCSDatastoreContext) ([]*datastore.Key, []shared.TestRun) {
	query := datastore.NewQuery("TestRun").Order("-CreatedAt")
	keys := make([]*datastore.Key, 0)
	testRuns := make([]shared.TestRun, 0)
	it := ctx.Client.Run(ctx.Context, query)
	for {
		var testRun shared.TestRun
		key, err := it.Next(&testRun)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		keys = append(keys, key)
		testRuns = append(testRuns, testRun)
	}
	return keys, testRuns
}

func getGit(s storage.Storer, fs billy.Filesystem, o *git.CloneOptions) *git.Repository {
	repo, err := git.Open(s, fs)
	if err == git.ErrRepositoryNotExists {
		repo, err = git.Clone(s, fs, o)
		if err != nil {
			log.Fatal(err)
		}
		return nil
	}
	if err != nil {
		log.Fatal(err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		log.Fatal(err)
	}

	for {
		err = wt.Pull(&git.PullOptions{})

		if err == io.EOF {
			log.Println("EOF during git pull; retrying...")
			continue
		} else if err != git.NoErrAlreadyUpToDate && err != nil {
			log.Fatal(err)
		} else {
			break
		}
	}
	return repo
}

func getHashForRun(run shared.TestRun) (string, error) {
	cmd := exec.Command("git", "rev-parse", run.Revision)
	cmd.Dir = *wptGitPath
	bytes, err := cmd.Output()
	if err != nil {
		return "", err
	}
	str := string(bytes)
	return strings.Trim(str, " \t\r\n\v"), nil
}

func getRunsAndSetupGit(ctx wptStorage.GCSDatastoreContext) ([]*datastore.Key, []shared.TestRun) {
	var wg sync.WaitGroup
	var keys []*datastore.Key
	var runs []shared.TestRun
	wg.Add(2)
	go func() {
		defer wg.Done()
		keys, runs = getRuns(ctx)
	}()
	go func() {
		defer wg.Done()
		fs := osfs.New(*wptGitPath)
		store, err := filesystem.NewStorage(osfs.New(*wptGitPath + "/.git"))
		if err != nil {
			log.Fatal(err)
		}
		getGit(store, fs, &git.CloneOptions{
			URL: "https://github.com/w3c/web-platform-tests.git",
		})
	}()
	wg.Wait()

	return keys, runs
}

// Currently dead code, but may be used later to batch update datastore entities missing new fields.
func getHashes(runs []shared.TestRun) (map[string]string, map[string]error) {
	errs := make(map[string]error)
	hashes := make(map[string]string)
	var wg sync.WaitGroup
	wg.Add(len(runs))
	for i, run := range runs {
		go func(i int, run shared.TestRun) {
			defer wg.Done()
			h := run.Revision
			if _, ok := hashes[h]; ok {
				return
			}
			hashes[h], errs[h] = getHashForRun(run)
		}(i, run)
	}
	wg.Wait()

	return hashes, errs
}

func writeJSON(ctx context.Context, bucket *gcs.BucketHandle, path string, data interface{}) error {
	obj := bucket.Object(path)
	if err := func() error {
		objWriter := obj.NewWriter(ctx)
		gzWriter := gzip.NewWriter(objWriter)
		encoder := json.NewEncoder(gzWriter)

		objWriter.ContentType = "application/json"
		objWriter.ContentEncoding = "gzip"

		if err := encoder.Encode(data); err != nil {
			objWriter.CloseWithError(err)
			return err
		}

		if err := gzWriter.Close(); err != nil {
			return err
		}
		return objWriter.Close()
	}(); err != nil {
		return err
	}

	return nil
}

func streamData(ctx context.Context, bucket *gcs.BucketHandle, path string, reader io.Reader) error {
	obj := bucket.Object(path)
	if err := func() error {
		objWriter := obj.NewWriter(ctx)
		gzWriter := gzip.NewWriter(objWriter)
		scanner := bufio.NewScanner(reader)

		objWriter.ContentType = "text/plain"
		objWriter.ContentEncoding = "gzip"

		for scanner.Scan() {
			if _, err := gzWriter.Write([]byte(scanner.Text() + "\n")); err != nil {
				objWriter.CloseWithError(err)
				return err
			}
		}
		if err := scanner.Err(); err != nil {
			objWriter.CloseWithError(err)
			return err
		}

		if err := gzWriter.Close(); err != nil {
			return err
		}
		return objWriter.Close()
	}(); err != nil {
		return err
	}

	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Printf("Loading and storing WPT checkout in %s", *wptGitPath)
	log.Printf("Caching WPT data in %s", *wptDataPath)
	err := os.MkdirAll(*wptDataPath, 0755)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	datastoreClient, err := datastore.NewClient(ctx, *projectID, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}

	storageClient, err := gcs.NewClient(ctx, option.WithCredentialsFile(*gcpCredentialsFile))
	if err != nil {
		log.Fatal(err)
	}
	inputBucket := storageClient.Bucket(*inputGcsBucket)

	remoteCtx := wptStorage.GCSDatastoreContext{
		ctx,
		wptStorage.Bucket{
			*inputGcsBucket,
			inputBucket,
		},
		datastoreClient,
	}

	// Forever: Reload wpt revisions and runs; skip handled runs; handle one run;
	// repeat.
	for {
		log.Printf("Loading runs from Datastore and initializing local web-platform-tests checkout")
		datastoreKeys, testRuns := getRunsAndSetupGit(remoteCtx)
		outputBucket := storageClient.Bucket(*outputGcsBucket)

		for i, testRun := range testRuns {
			datastoreKey := datastoreKeys[i]
			hash, err := getHashForRun(testRun)
			if err != nil {
				log.Printf("Skipping run for unknown revision: %v", testRun)
				continue
			}
			bucketDir := fmt.Sprintf("%s/%s_%s_%s_%s", hash, testRun.BrowserName, testRun.BrowserVersion, testRun.OSName, testRun.OSVersion)
			remoteLogPath := bucketDir + "/migration.log"
			remoteReportPath := bucketDir + "/report.json"
			rawResultsURL := fmt.Sprintf("https://storage.googleapis.com/%s/%s", *outputGcsBucket, remoteReportPath)

			// Check RawResultsURL as indicator that run was already handled.
			if testRun.RawResultsURL != "" {
				// Update FullRevisionHash in Datastore.
				if testRun.FullRevisionHash != hash {
					testRun.FullRevisionHash = hash
					log.Printf("Updating datastore TestRun key=%v FullRevisionHash=%s", datastoreKey, testRun.FullRevisionHash)
					_, err := datastoreClient.Put(ctx, datastoreKey, &testRun)
					if err != nil {
						log.Fatal(err)
					}
				}
				log.Printf("Skipping revision: Found log file for revision: %v", testRun)
				continue
			}

			// Create local log file for this run.
			localLogFileName := fmt.Sprintf("%s_%s_%s_%s_%s_migration.log", hash, testRun.BrowserName, testRun.BrowserVersion, testRun.OSName, testRun.OSVersion)
			log.Printf("Opening local run-specific log file %s", localLogFileName)
			logFile, err := os.OpenFile(localLogFileName, os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				log.Fatal(err)
			}

			// Download sharded run, consolidate it, upload consolidated run.
			{
				defer logFile.Close()
				log.Printf("Downloading, consolidating, and uploading %v", testRun)
				log.Printf("Logging to %s", localLogFileName)
				log.SetOutput(logFile)

				log.Printf("Loading results from %s for %v", remoteCtx.Bucket.Name, testRun)
				runResults := wptStorage.LoadTestRunResults(&remoteCtx, []shared.TestRun{testRun}, nil, false)
				log.Printf("Consolidating metrics for %v", testRun)
				results := make([]*metrics.TestResults, 0, len(runResults))
				for _, rr := range runResults {
					results = append(results, rr.Res)
				}
				report := metrics.TestResultsReport{results}

				log.Printf("Writing consolidated results to %s/%s", *outputGcsBucket, remoteReportPath)
				if err = writeJSON(ctx, outputBucket, remoteReportPath, report); err != nil {
					log.Printf("Error writing %s to Google Cloud Storage: %v\n", remoteReportPath, err)
					log.SetOutput(os.Stdout)
					log.Fatal(err)
				}

				log.SetOutput(os.Stdout)
			}

			// Re-open local log file for streaming to GCS.
			log.Printf("Opening %s for reading", localLogFileName)
			logFile, err = os.OpenFile(localLogFileName, os.O_RDONLY, 0666)
			if err != nil {
				log.Fatal(err)
			}

			// Stream log file to GCS.
			{
				defer logFile.Close()
				log.Printf("Streaming %s to GCS object: %s", localLogFileName, remoteLogPath)
				if err := streamData(ctx, outputBucket, remoteLogPath, logFile); err != nil {
					log.Printf("Error streaming log to Google Cloud Storage: %v\n", err)
				}
				if err := logFile.Close(); err != nil {
					log.Fatal(err)
				}
			}

			// Update TestRun in Datastore.
			if testRun.FullRevisionHash != hash || testRun.RawResultsURL != rawResultsURL {
				testRun.FullRevisionHash = hash
				testRun.RawResultsURL = rawResultsURL
				log.Printf("Updating datastore TestRun key=%v FullRevisionHash=%s RawResultsURL=%s", datastoreKey, testRun.FullRevisionHash, testRun.RawResultsURL)
				_, err := datastoreClient.Put(ctx, datastoreKey, &testRun)
				if err != nil {
					log.Fatal(err)
				}
			}

			// Wait a minute to avoid being throttled by GCS.
			time.Sleep(time.Minute)

			// Jump to outer loop to reload latest revisions and test runs that may
			// have landed in the meantime.
			break
		}
	}
}
