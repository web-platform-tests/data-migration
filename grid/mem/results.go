package mem

import (
	"github.com/web-platform-tests/wpt.fyi/shared"
)

type RunID int64
type ResultID int64

type Results struct {
	ByRunTest map[RunID]map[TestID]ResultID
}

func NewResults() *Results {
	return &Results{ByRunTest: make(map[RunID]map[TestID]ResultID)}
}

func (rs *Results) Copy() *Results {
	nu := &Results{}
	m1 := make(map[RunID]map[TestID]ResultID)
	for a, b := range rs.ByRunTest {
		m2 := make(map[TestID]ResultID)
		for c, d := range b {
			m2[c] = d
		}
		m1[a] = m2
	}
	nu.ByRunTest = m1
	return nu
}

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

func ResultFilter(ru RunID, re ResultID) UnboundFilter {
	return NewResultEQFilter(ru, re)
}
