package mem

import (
	"strings"

	farm "github.com/dgryski/go-farm"
)

type TestID uint64

type Tests struct {
	Tests map[TestID]string
}

func NewTests() *Tests {
	return &Tests{Tests: make(map[TestID]string)}
}

func (ts *Tests) Copy() *Tests {
	nu := &Tests{}
	m := make(map[TestID]string)
	for a, b := range ts.Tests {
		m[a] = b
	}
	nu.Tests = m
	return nu
}

func (ts *Tests) Add(id TestID, str string) {
	ts.Tests[id] = str
}

func (ts *Tests) GetName(id TestID) string {
	return strings.Split(ts.Tests[id], "\x00")[0]
}

func TestFilter(q string) Filter {
	return func(ts *Tests, rs *Results, t TestID) bool {
		str, ok := ts.Tests[t]
		if !ok {
			return false
		}
		return strings.Contains(str, q)
	}
}

func computeID(name string, subPtr *string) (TestID, string, error) {
	var str string
	if subPtr != nil && *subPtr != "" {
		str = name + "\x00" + *subPtr
	} else {
		str = name
	}
	return TestID(farm.Fingerprint64([]byte(str))), str, nil
}
