package processor

import (
	"context"
	"flag"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/web-platform-tests/wpt.fyi/shared"
	"google.golang.org/api/iterator"
)

var (
	dryRun    = flag.Bool("dry-run", false, "Only print out runs that would be affected")
	projectID = flag.String("project", "wptdashboard-staging", "Google Cloud project")
)

// ConditionUnsatisfied is a non-fatal error when a run does not need to be processed.
type ConditionUnsatisfied struct{}

func (e ConditionUnsatisfied) Error() string {
	return "Condition not satisfied"
}

func ProcessRun(ctx context.Context, runsProcessor Runs, dsClient *datastore.Client, key *datastore.Key) {
	var run shared.TestRun
	_, err := dsClient.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		err := tx.Get(key, &run)
		if err != nil {
			return err
		}
		if runsProcessor.ShouldProcessRun(&run) {
			if *dryRun {
				return nil
			}
			return runsProcessor.ProcessRun(tx, key, &run)
		}
		return ConditionUnsatisfied{}
	})
	if err != nil {
		_, ok := err.(ConditionUnsatisfied)
		if !ok {
			panic(err)
		} else {
			return
		}
	}
	fmt.Printf("Processed TestRun %s (%s %s)\n", key.String(), run.BrowserName, run.BrowserVersion)
}

// MigrateData handles all the loading and transactions across the full
// datastore. It should be called from a main(), e.g.
//
// func main() {
//   p := experimentalLabeller{}
//   processor.MigrateData(p)
// }
func MigrateData(runsProcessor Runs) {
	flag.Parse()
	if *dryRun {
		fmt.Println("Dry running; data will NOT be modified...")
	}

	ctx := context.Background()

	dsClient, err := datastore.NewClient(ctx, *projectID)
	if err != nil {
		panic(err)
	}

	query := datastore.NewQuery("TestRun").Order("-TimeStart").KeysOnly()

	for t := dsClient.Run(ctx, query); ; {
		key, err := t.Next(nil)
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}

		go ProcessRun(ctx, runsProcessor, dsClient, key)
	}
}
