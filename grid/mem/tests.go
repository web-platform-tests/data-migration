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

// type TestIndex struct {
// 	Shards []*Tests
// }

const (
	testEOF = TestID(0)
)

// func NewTestIndex(n int) *TestIndex {
// 	tss := make([]*Tests, n)
// 	for i := range tss {
// 		tss[i] = NewTests()
// 	}
// 	return &TestIndex{tss}
// }

func NewTests() *Tests {
	return &Tests{Tests: make(map[TestID]string)}
}

// func (ti *TestIndex) Add(name string, subPtr *string) (TestID, error) {
// 	id, str, err := computeID(name, subPtr)
// 	if err != nil {
// 		return id, err
// 	}
// 	si := uint64(id) % uint64(len(ti.Shards))
// 	ti.Shards[si].Add(id, str)
// 	return id, nil
// }

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
