package mem

import (
	"fmt"

	farm "github.com/dgryski/go-farm"
)

type TestID struct {
	TestID uint64
	SubID  uint64
}

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
	return ts.Tests[id]
}

func TestFilter(q string) UnboundFilter {
	return NewTestNameFilter(q)
}

func computeID(name string, subPtr *string) (TestID, error) {
	var s uint64
	t := farm.Fingerprint64([]byte(name))
	if subPtr != nil && *subPtr != "" {
		s := farm.Fingerprint64([]byte(*subPtr))
		if s == 0 {
			return TestID{}, fmt.Errorf(`Subtest ID for string "%s" is 0`, *subPtr)
		}
	}
	return TestID{t, s}, nil
}
