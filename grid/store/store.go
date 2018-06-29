package store

import (
	"sync"

	"github.com/web-platform-tests/data-migration/grid/split"

	"github.com/web-platform-tests/data-migration/grid/split/results"
	"github.com/web-platform-tests/data-migration/grid/split/runs"
	"github.com/web-platform-tests/data-migration/grid/split/tests"
)

type Query struct {
	TestQuery   *tests.Query
	RunQuery    *runs.Query
	ResultQuery *results.Query
}

type Result struct {
	Runs    []runs.Run
	Tests   []tests.RankedTest
	Results []results.TestResults
}

type Store interface {
	Find(Query) (Result, error)
}

type TriStore struct {
	tests.TestNames
	runs.Runs
	results.Results
}

func (s TriStore) Find(q Query) (Result, error) {
	var err error
	var ts tests.RankedTests
	var rs []runs.Run
	var wg sync.WaitGroup
	if q.TestQuery != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts, err = s.TestNames.Find(*q.TestQuery)
		}()
	} else {
		ts = s.TestNames.GetAll()
	}
	if q.RunQuery != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rs, err = s.Runs.Find(*q.RunQuery)
		}()
	} else {
		rs = s.Runs.GetAll()
	}
	wg.Wait()

	if err != nil {
		return Result{}, err
	}

	tks := make([]split.TestKey, 0, len(ts))
	for _, t := range ts {
		tks = append(tks, split.TestKey(t.ID()))
	}
	rks := make([]split.RunKey, 0, len(rs))
	for _, r := range rs {
		rks = append(rks, split.RunKey(r.ID))
	}

	var ress []results.TestResults
	if len(tks) > 0 && len(rks) > 0 {
		if q.ResultQuery != nil {
			ress, err = s.Results.Find(rks, tks, *q.ResultQuery)
		} else {
			ress, err = s.Results.GetAll(rks, tks)
		}
	} else {
		if q.ResultQuery != nil {
			ress, err = s.Results.Find(rks, tks, *q.ResultQuery)
		} else {
			ress, err = s.Results.GetAll(rks, tks)
		}
	}

	if err != nil {
		return Result{}, err
	}

	return Result{rs, ts, ress}, nil
}

func NewTriStore(tn tests.TestNames, ru runs.Runs, re results.Results) Store {
	return &TriStore{tn, ru, re}
}
