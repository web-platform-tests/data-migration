// Copyright 2018 The WPT Dashboard Project. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/web-platform-tests/data-migration/grid"
	"github.com/web-platform-tests/results-analysis/metrics"
)

var testsPath *string
var runsPath *string
var byRunPath *string

type RunTestData map[int32]map[int32]metrics.CompleteTestStatus
type TestRunData map[int32]map[int32]metrics.CompleteTestStatus

type StringIndex interface {
	Lookup(string) []int32
}

/*
type fmindex struct {
	data  []byte
	index *fmi.FMIndex
}

func (i fmindex) Lookup(str string) []int32 {
	res, err := i.index.Locate([]byte(str), 0)
	if err != nil {
		log.Printf("WARN: Error occurred during FM-Index lookup for \"%s\"", str)
		return nil
	}

	ret := make([]int32, 0)
	for _, idx := range res {
		for i.data[idx] != '\x00' && idx < len(i.data) {
			idx++
		}

		if idx >= len(i.data)-5 {
			log.Printf("WARN: Malformed string for FM-index: Could not locate ID")
			return nil
		}

		idx++
		var id int32
		binary.Read(bytes.NewBuffer(i.data[idx:idx+4]), binary.LittleEndian, &id)
		ret = append(ret, id)
	}

	return ret
}
*/

type node struct {
	children map[string]*node
	data     []int32
}

const maxResults = 10000000

func (n *node) all() []int32 {
	var data []int32
	data = nil
	for _, c := range n.children {
		if c == nil {
			continue
		}
		if data == nil {
			data = make([]int32, 0, len(n.data))
		}
		data = append(data, c.all()...)
		if len(data) >= maxResults {
			return data[0:maxResults]
		}
	}
	if n.data != nil {
		if data == nil {
			data = make([]int32, 0, len(n.data))
		}
		data = append(data, n.data...)
	}
	return data[0:maxResults]
}

func (n *node) lookup(parts []string) []int32 {
	if n.children == nil && len(parts) > 0 {
		return nil
	} else if len(parts) == 0 {
		return n.all()
	}

	part := parts[0]
	var data []int32
	for key, next := range n.children {
		if strings.Contains(key, part) {
			res := next.lookup(parts[1:])
			if res != nil {
				if data == nil {
					data = make([]int32, 0, len(res))
				}
				data = append(data, res...)
				if len(data) >= maxResults {
					return data[0:maxResults]
				}
			}
		}
	}

	if n.data != nil {
		data = append(data, n.data...)
	}
	return data[0:maxResults]
}

func (n *node) Lookup(str string) []int32 {
	return n.lookup(strings.Split(str, "/")[1:])
}

type API interface {
	Runs() []grid.Run
	Tests() []grid.Test
	RunTestIndex() RunTestData
	TestIndex() StringIndex
}

type apiData struct {
	runs         []grid.Run
	tests        []grid.Test
	runTestIndex RunTestData
	testIndex    *node
}

func (a *apiData) Runs() []grid.Run {
	return a.runs
}

func (a *apiData) Tests() []grid.Test {
	return a.tests
}

func (a *apiData) RunTestIndex() RunTestData {
	return a.runTestIndex
}

func (a *apiData) TestIndex() StringIndex {
	return a.testIndex
}

var a API

/*
func bufCheck(i int, j int, err error) {
	if err != nil {
		log.Fatal(err)
	}
	if i != j {
		log.Fatal(errors.New("Failed to write all bytes"))
	}
}
*/

func loadAPI() {
	var newAPI apiData
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		bytes, err := ioutil.ReadFile(*runsPath)
		if err != nil {
			log.Fatal(err)
		}
		json.Unmarshal(bytes, &newAPI.runs)
		log.Printf("INFO: Unmarshaled %d runs", len(newAPI.runs))
	}()
	go func() {
		defer wg.Done()
		data, err := ioutil.ReadFile(*testsPath)
		if err != nil {
			log.Fatal(err)
		}
		json.Unmarshal(data, &newAPI.tests)
		log.Printf("INFO: Unmarshaled %d tests", len(newAPI.tests))

		/*
			var buf bytes.Buffer
			for _, t := range newAPI.tests {
				i, err := buf.WriteString(t.Test)
				bufCheck(len(t.Test), i, err)
				if t.Subtest != "" {
					buf.WriteRune(':')
					buf.WriteString(t.Subtest)
				}
				buf.WriteRune('\x00')
				var indicator byte
				indicator = 0
				var mask int32
				mask = 0xFF000000
				id := t.ID
				for i := 0; i < 4; i++ {
					if t.ID&mask == mask {
						indicator |= 0x01
						id &= ^mask
					}
					indicator <<= 1
					mask >>= 8
				}
				indicator >>= 1
				i, err = buf.Write([]byte{indicator})
				bufCheck(1, i, err)

				err = binary.Write(&buf, binary.LittleEndian, id)
				if err != nil {
					log.Fatal(err)
				}
			}
			var index fmindex
			index.data = buf.Bytes()
			index.index = fmi.NewFMIndex()
			index.index.EndSymbol = '\xFF'
			_, err = index.index.TransformForLocate(index.data)
			if err != nil {
				log.Fatal(err)
			}
		*/

		var root node
		for _, t := range newAPI.tests {
			var id string
			if t.Subtest == "" {
				id = t.Test
			} else {
				id = t.Test + ":" + t.Subtest
			}

			n := &root
			parts := strings.Split(id, "/")[1:]
			for _, part := range parts {
				if n.children == nil {
					n.children = make(map[string]*node)
				}
				if _, ok := n.children[part]; !ok {
					n.children[part] = &node{}
				}
				n = n.children[part]
			}
			n.data = append(n.data, t.ID)
		}
		newAPI.testIndex = &root
		log.Printf("INFO: Constructed index for test names")
	}()
	go func() {
		defer wg.Done()
		files, err := ioutil.ReadDir(*byRunPath)
		if err != nil {
			log.Fatal(err)
		}

		var testRunData TestRunData
		var mutex = &sync.Mutex{}
		testRunData = make(map[int32]map[int32]metrics.CompleteTestStatus)
		var wg2 sync.WaitGroup
		wg2.Add(len(files))
		for _, f := range files {
			path := *byRunPath + "/" + f.Name()
			go func(path string) {
				defer wg2.Done()
				id64, err := strconv.ParseInt(path[strings.LastIndex(path, "/")+1:strings.LastIndex(path, ".")], 10, 32)
				if err != nil {
					log.Fatal(err)
				}
				id := int32(id64)
				bytes, err := ioutil.ReadFile(path)
				if err != nil {
					log.Fatal(err)
				}
				m := make(map[int32]metrics.CompleteTestStatus)
				json.Unmarshal(bytes, &m)

				{
					defer mutex.Unlock()
					mutex.Lock()
					testRunData[id] = m
				}
			}(path)
		}
		wg2.Wait()

		newAPI.runTestIndex = make(map[int32]map[int32]metrics.CompleteTestStatus)
		for testID, runData := range testRunData {
			for runID, data := range runData {
				if _, ok := newAPI.runTestIndex[runID]; !ok {
					newAPI.runTestIndex[runID] = make(map[int32]metrics.CompleteTestStatus)
				}
				newAPI.runTestIndex[runID][testID] = data
			}
		}
	}()
	wg.Wait()
	if a != nil {
		a = &newAPI
		log.Printf("INFO: Triggering GC")
		runtime.GC()
		log.Printf("INFO: GC complete")
	} else {
		a = &newAPI
	}
	log.Printf("INFO: API initialized")
}

func regSplit(text string, delimeter string) []string {
	reg := regexp.MustCompile(delimeter)
	indexes := reg.FindAllStringIndex(text, -1)
	laststart := 0
	result := make([]string, len(indexes)+1)
	for i, element := range indexes {
		result[i] = text[laststart:element[0]]
		laststart = element[1]
	}
	result[len(indexes)] = text[laststart:len(text)]
	return result
}

func runsFilterAnd(x func(grid.Run, *[]grid.Run) bool, y func(grid.Run, *[]grid.Run) bool) func(grid.Run, *[]grid.Run) bool {
	return func(r grid.Run, s *[]grid.Run) bool {
		return x(r, s) && y(r, s)
	}
}

func runsFilterTrue(grid.Run, *[]grid.Run) bool {
	return true
}

var runType = reflect.TypeOf(grid.Run{})

func runsFilterStringProperty(name, substr string) func(grid.Run, *[]grid.Run) bool {
	f, ok := runType.FieldByName(name)
	if !ok {
		log.Fatalf("Failed to lookup Run struct field: \"%s\"", name)
	}

	return func(r grid.Run, s *[]grid.Run) bool {
		v := reflect.ValueOf(r)
		str := v.FieldByName(name).Interface().(string)
		return strings.Contains(str, substr)
	}
}

func runsFilterAnyStringSliceProperty(name, substr string) func(grid.Run, *[]grid.Run) bool {
	f, ok := runType.FieldByName(name)
	if !ok {
		log.Fatalf("Failed to lookup Run struct field: \"%s\"", name)
	}

	return func(r grid.Run, s *[]grid.Run) bool {
		v := reflect.ValueOf(r)
		strs := v.FieldByName(name).Interface().([]string)
		for _, str := range strs {
			if strings.Contains(str, substr) {
				return true
			}
		}
		return false
	}
}

func runsFilterLimit(limit int) func(grid.Run, *[]grid.Run) bool {
	return func(r grid.Run, s *[]grid.Run) bool {
		return s != nil && len(*s) < limit
	}
}

var runsStringProps = map[string]string{
	"browser_name":    "BrowserName",
	"browser_version": "BrowserVersion",
	"os_name":         "OSName",
	"os_version":      "OSVersion",
}

var runsStringSliceProps = map[string]string{
	"labels": "Labels",
}

func runsHandler(w http.ResponseWriter, r *http.Request) {
	if a == nil {
		http.Error(w, "API not yet initialized", http.StatusServiceUnavailable)
		return
	}

	q := r.URL.Query()
	hash := q.Get("hash")

	filter := runsFilterTrue
	for qKey, propName := range runsStringProps {
		v := q.Get(qKey)
		if v != "" {
			filter = runsFilterAnd(runsFilterStringProperty(propName, v), filter)
		}
	}
	for qKey, propName := range runsStringSliceProps {
		v := q.Get(qKey)
		if v != "" {
			filter = runsFilterAnd(runsFilterAnyStringSliceProperty(propName, v), filter)
		}
	}
	limit := q.Get("limit")
	if limit != "" {
		lim, err := strconv.Atoi(limit)
		if err != nil {
			log.Printf("WARN: Invalid limit: %s", limit)
		} else {
			filter = runsFilterAnd(runsFilterLimit(lim), filter)
		}
	}

	allRuns := a.Runs()

	res := allRuns[:0]
	for _, run := range allRuns {
		if filter(run, &res) {
			res = append(res, run)
		}
	}

	// TODO: JSONify res; perhaps just IDs?
}

func qHandler(w http.ResponseWriter, r *http.Request) {
	if a == nil {
		http.Error(w, "API not yet initialized", http.StatusServiceUnavailable)
		return
	}

	/*
		q := r.URL.Query()
		tq := q.Get("tq")
		rq := q.Get("rq")
		if tq != "" {
			tests = a.TestIndex().Lookup(tq)
		}

		if rq != "" {
			allRuns := a.Runs()
			allRuns[0].BrowserName
		}
	*/

	w.Write([]byte(fmt.Sprintf("%d runs and %d tests", len(a.Runs()), len(a.Tests()))))
}

func init() {
	testsPath = flag.String("tests-path", "tests.json", "Path to tests JSON file")
	runsPath = flag.String("runs-path", "runs.json", "Path to runs JSON file")
	byRunPath = flag.String("by-run-path", "by_run", "Path to directory containing tests sharded by run")

	log.SetFlags(log.LstdFlags | log.Llongfile | log.LUTC)
	flag.Parse()

	a = nil
	go func() {
		for true {
			log.Printf("INFO: (Re)loading API")
			loadAPI()
			log.Printf("INFO: Sleeping")
			time.Sleep(time.Minute)
		}
	}()
}

func main() {
	http.HandleFunc("/", qHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}
