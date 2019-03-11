package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"

	"golang.org/x/oauth2/google"

	"github.com/web-platform-tests/wpt.fyi/shared"
	"google.golang.org/appengine/remote_api"
	"google.golang.org/appengine/search"
)

var (
	projectID = flag.String("project", "wptdashboard-staging", "Google Cloud project")
	creds     = flag.String("creds", "", "AppEngine credentials file path")
)

type TestContent struct {
	Content search.HTML
}

func main() {
	flag.Parse()
	ctx := context.Background()

	hc, err := google.DefaultClient(ctx,
		"https://www.googleapis.com/auth/appengine.apis",
		"https://www.googleapis.com/auth/cloud-platform",
		"https://www.googleapis.com/auth/cloud_search",
		"https://www.googleapis.com/auth/userinfo.email",
	)
	if err != nil {
		log.Fatal(err)
	}

	remoteCtx, err := remote_api.NewRemoteContext("wptdashboard-staging.appspot.com", hc)
	if err != nil {
		log.Fatal(err)
	}

	index, err := search.Open("test-content")
	if err != nil {
		panic(err)
	}

	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))

	type manifest struct {
		Items map[string]map[string][][]json.RawMessage `json:"items"`
	}

	m := new(manifest)
	err = shared.FetchJSON("https://wpt.fyi/api/manifest", &m)
	if err != nil {
		panic(err)
	}

	for _, files := range m.Items {
		for filePath := range files {
			if done.Contains("/" + filePath) {
				continue
			}
			go func(filePath string) {
				file, err := os.Open(path.Join(dir, "../../wpt/", filePath))
				if err != nil {
					log.Printf("Failed to open %s: %s", filePath, err.Error())
					return
				}
				defer file.Close()

				data, _ := ioutil.ReadAll(file)
				_, err = index.Put(remoteCtx, "/"+filePath, &TestContent{search.HTML(data)})
				if err != nil {
					log.Printf("ERROR: %s", err.Error())
				} else {
					log.Printf("/%s : %v bytes", filePath, len(data))
				}
			}(filePath)
		}
	}
}
