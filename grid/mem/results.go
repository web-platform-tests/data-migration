package mem

import (
	"github.com/web-platform-tests/wpt.fyi/shared"
)

type RunID int64
type ResultID int64

type Results struct {
	ByRunTest map[RunID]map[TestID]ResultID
}

// type ResultIndex struct {
// 	Shards []*Results
// }

const (
	initialResultsCap = 10
	resultEOF         = ResultID(0)
)

// func NewResultIndex(n int) *ResultIndex {
// 	rss := make([]*Results, n)
// 	for i := range rss {
// 		rss[i] = NewResults()
// 	}
// 	return &ResultIndex{rss}
// }

func NewResults() *Results {
	return &Results{ByRunTest: make(map[RunID]map[TestID]ResultID)}
}

// func (ri *ResultIndex) Add(ru RunID, re ResultID, t TestID) error {
// 	si := uint64(t) % uint64(len(ri.Shards))
// 	ri.Shards[si].Add(ru, re, t)
// 	return nil
// }

func (rs *Results) Add(ru RunID, re ResultID, t TestID) {
	if _, ok := rs.ByRunTest[ru]; !ok {
		rs.ByRunTest[ru] = make(map[TestID]ResultID)
	}
	rs.ByRunTest[ru][t] = re
}

func (rs *Results) GetResult(ru RunID, t TestID) ResultID {
	byTest, ok := rs.ByRunTest[ru]
	if !ok {
		return ResultID(shared.TestStatusUnknown)
	}
	re, ok := byTest[t]
	if !ok {
		return ResultID(shared.TestStatusUnknown)
	}
	return re
}

func ResultFilter(ru RunID, re ResultID) Filter {
	return func(ts *Tests, rs *Results, t TestID) bool {
		byTest, ok := rs.ByRunTest[ru]
		if !ok {
			return false
		}
		result, ok := byTest[t]
		return ok && result == re
	}
}
