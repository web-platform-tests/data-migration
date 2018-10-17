package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/spanner"
	farm "github.com/dgryski/go-farm"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	projectID  = "wptdashboard-staging"
	instanceID = "wpt-results-staging"
	databaseID = "baseline"
	tableID    = "numbers"
)

var (
	port               = flag.Int("port", 8080, "Port to listen on")
	gcpCredentialsFile = flag.String("gcp_credentials_file", "", "Credentails file for GCP authentication")
)

func run(ctx context.Context) (string, error) {
	var (
		client *spanner.Client
		err    error
	)
	spec := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	if gcpCredentialsFile != nil && *gcpCredentialsFile != "" {
		client, err = spanner.NewClient(ctx, spec, option.WithCredentialsFile(*gcpCredentialsFile))
	} else {
		client, err = spanner.NewClient(ctx, spec)
	}
	if err != nil {
		log.Error(err)
		return err.Error(), err
	}
	defer client.Close()

	tx := client.Single()
	defer tx.Close()

	q := spanner.Statement{
		SQL: fmt.Sprintf("SELECT int_id FROM %s WHERE int_id = @num", tableID),
		Params: map[string]interface{}{
			"num": int64(farm.Fingerprint64([]byte("0"))),
		},
	}
	start := time.Now()
	it := tx.Query(ctx, q)
	for _, itErr := it.Next(); itErr != iterator.Done; _, itErr = it.Next() {
		if itErr != nil {
			log.Error(itErr)
			return err.Error(), itErr
		}
	}
	t := time.Now().Sub(start)
	str := fmt.Sprintf("Query: %v; time %v", q, t)
	log.Infof(str)

	return str, nil
}

func handleReq(w http.ResponseWriter, r *http.Request) {
	str, err := run(r.Context())
	if err != nil {
		http.Error(w, str, http.StatusInternalServerError)
		return
	}
	w.Write([]byte(str))
}

func init() {
	flag.Parse()
}

func main() {
	http.HandleFunc("/_ah/liveness_check", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("alive"))
	})
	http.HandleFunc("/_ah/readiness_check", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ready"))
	})
	http.HandleFunc("/q", handleReq)

	log.Infof("Listening on port %d", *port)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
