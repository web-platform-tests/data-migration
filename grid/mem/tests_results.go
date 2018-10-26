package mem

import (
	"github.com/web-platform-tests/results-analysis/metrics"
	"github.com/web-platform-tests/wpt.fyi/shared"

	mapset "github.com/deckarep/golang-set"
)

type TestsResults struct {
	Tests   *Tests
	Results *Results
}

type TestResultIndex struct {
	Shards []*TestsResults
	Runs   mapset.Set
}

type NamedTest struct {
	Test TestID
	Name string
}

type TestResult struct {
	Test   TestID
	Result ResultID
}

type RunResults struct {
	RunID
	Results []*metrics.TestResults
}

func (tr *TestsResults) Execute(f Filter) []TestID {
	res := make([]TestID, 0)
	for t := range tr.Tests.Tests {
		if f(tr.Tests, tr.Results, t) {
			res = append(res, t)
		}
	}
	return res
}

func NewIndex(n int) *TestResultIndex {
	tr := make([]*TestsResults, n)
	for i := range tr {
		tr[i] = &TestsResults{
			Tests:   NewTests(),
			Results: NewResults(),
		}
	}
	return &TestResultIndex{tr, mapset.NewSet()}
}

func (i *TestResultIndex) Copy() *TestResultIndex {
	nu := &TestResultIndex{}
	nu.Runs = i.Runs.Clone()
	ss := make([]*TestsResults, len(i.Shards))
	for j := range ss {
		ss[j] = &TestsResults{
			Tests:   i.Shards[j].Tests.Copy(),
			Results: i.Shards[j].Results.Copy(),
		}
	}
	nu.Shards = ss
	return nu
}

func (i *TestResultIndex) WithRunResults(rrs ...RunResults) (*TestResultIndex, error) {
	nu := i.Copy()
	for _, rr := range rrs {
		nu.Runs.Add(rr.RunID)
		for _, r := range rr.Results {
			err := nu.add(r.Test, nil, rr.RunID, ResultID(shared.TestStatusValueFromString(r.Status)))
			if err != nil {
				return nil, err
			}
			for _, r2 := range r.Subtests {
				err := nu.add(r.Test, &r2.Name, rr.RunID, ResultID(shared.TestStatusValueFromString(r2.Status)))
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return nu, nil
}

func (i *TestResultIndex) add(name string, subPtr *string, ru RunID, re ResultID) error {
	id, str, err := computeID(name, subPtr)
	if err != nil {
		return err
	}
	tr := i.Shards[i.getShardIdx(id)]
	tr.Tests.Add(id, str)
	tr.Results.Add(ru, re, id)
	return nil
}

func (i *TestResultIndex) HasRun(ru RunID) bool {
	return i.Runs.Contains(ru)
}

func (i *TestResultIndex) Query(f Filter) []TestID {
	c := make(chan []TestID, len(i.Shards))
	for i, tr := range i.Shards {
		go func(n int, tr *TestsResults) {
			c <- tr.Execute(f)
		}(i, tr)
	}
	res := make([]TestID, 0)
	for n := 0; n < len(i.Shards); n++ {
		res = append(res, <-c...)
	}
	return res
}

func (i *TestResultIndex) GetName(id TestID) string {
	return i.Shards[i.getShardIdx(id)].Tests.GetName(id)
}

func (i *TestResultIndex) GetResult(ru RunID, t TestID) ResultID {
	return i.Shards[i.getShardIdx(t)].Results.GetResult(ru, t)
}

func (i *TestResultIndex) GetNames(ids []TestID) map[TestID]string {
	tss := make([][]TestID, 0, len(i.Shards))
	for range i.Shards {
		tss = append(tss, make([]TestID, 0))
	}
	for _, id := range ids {
		si := i.getShardIdx(id)
		tss[si] = append(tss[si], id)
	}
	c := make(chan []NamedTest, len(tss))
	for j, ts := range tss {
		go func(j int, ts []TestID) {
			testsIdx := i.Shards[j].Tests
			ns := make([]NamedTest, 0, len(ts))
			for _, t := range ts {
				ns = append(ns, NamedTest{
					Test: t,
					Name: testsIdx.GetName(t),
				})
			}
			c <- ns
		}(j, ts)
	}

	res := make(map[TestID]string, 0)
	for j := 0; j < len(tss); j++ {
		ns := <-c
		for _, n := range ns {
			res[n.Test] = n.Name
		}
	}
	return res
}

func (i *TestResultIndex) GetResults(rus []RunID, ids []TestID) map[TestID][]ResultID {
	tss := make([][]TestID, 0, len(i.Shards))
	for range i.Shards {
		tss = append(tss, make([]TestID, 0))
	}
	for _, id := range ids {
		si := i.getShardIdx(id)
		tss[si] = append(tss[si], id)
	}
	ress := make(chan []TestResult, len(i.Shards))
	for j, ts := range tss {
		go func(j int, ts []TestID) {
			res := make([]TestResult, 0, len(ts)*len(rus))
			resultsIdx := i.Shards[j].Results
			for _, t := range ts {
				for _, ru := range rus {
					res = append(res, TestResult{
						Test:   t,
						Result: resultsIdx.GetResult(ru, t),
					})
				}
			}
			ress <- res
		}(j, ts)
	}
	res := make(map[TestID][]ResultID)
	for j := 0; j < len(i.Shards); j++ {
		trs := <-ress
		var t TestID
		var rs []ResultID
		for k, tr := range trs {
			if (k % len(rus)) == 0 {
				if rs != nil {
					res[t] = rs
				}
				t = tr.Test
				rs = make([]ResultID, 0, len(rus))
			}
			rs = append(rs, tr.Result)
		}
		if len(rs) > 0 {
			res[t] = rs
		}
	}
	return res
}

func (i *TestResultIndex) getShardIdx(id TestID) int {
	return int(uint64(id) % uint64(len(i.Shards)))
}
