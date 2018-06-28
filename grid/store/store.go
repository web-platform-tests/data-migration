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
	Tests   []tests.Test
	Results [][]results.Value
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
	var ts tests.Tests
	var rs []runs.Run
	var wg sync.WaitGroup
	if q.TestQuery != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ts = s.TestNames.Find(*q.TestQuery)
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

	ress := make([][]results.Value, 0, len(ts))
	for _, t := range ts {
		res := make([]results.Value, 0, len(rs))
		for _, tr := range rs {
			res = append(res, s.Results.Get(results.Key{split.RunKey(tr.ID), split.TestKey(t.ID())}))
		}
		ress = append(ress, res)
	}

	// TODO(markdittmer): Run results query.

	return Result{rs, ts, ress}, nil
}

func NewTriStore(tn tests.TestNames, ru runs.Runs, re results.Results) Store {
	return &TriStore{tn, ru, re}
}
