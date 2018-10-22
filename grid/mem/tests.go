package mem

import (
	"fmt"
	"strings"

	farm "github.com/dgryski/go-farm"
)

type TestID uint64

type Tests struct {
	Tests map[TestID]string
}

const (
	initialQueryResultCap = 10
	testEOF               = TestID(0)
)

func NewTests() *Tests {
	return &Tests{Tests: make(map[TestID]string)}
}

func (ts *Tests) Add(name string, subPtr *string) (TestID, error) {
	id, str, err := computeID(name, subPtr)
	if err != nil {
		return id, err
	}

	ts.Tests[id] = str

	return id, nil
}

func (ts *Tests) QueryChan(q string, in chan TestID) chan TestID {
	res := make(chan TestID)
	go func() {
		for {
			id := <-in
			if id == testEOF {
				break
			}
			if str, ok := ts.Tests[id]; ok && strings.Contains(str, q) {
				res <- id
			}
		}
		res <- testEOF
	}()
	return res
}

func (ts *Tests) QueryAll(q string) chan TestID {
	res := make(chan TestID)
	go func() {
		for id, str := range ts.Tests {
			if strings.Contains(str, q) {
				res <- id
			}
		}
		res <- testEOF
	}()
	return res
}

func (ts *Tests) Lookup(name string, subPtr *string) (TestID, error) {
	id, _, err := computeID(name, subPtr)
	return id, err
}

func computeID(name string, subPtr *string) (TestID, string, error) {
	var id TestID
	var str string
	if subPtr != nil && *subPtr != "" {
		str = name + "\x00" + *subPtr
	} else {
		str = name
	}
	id = TestID(farm.Fingerprint64([]byte(str)))

	if id == testEOF {
		return id, str, fmt.Errorf("Invalid TestID computed from name=%v, sub=%v", name, subPtr)
	}

	return id, str, nil
}

func (ts *Tests) EOF() TestID {
	return testEOF
}
