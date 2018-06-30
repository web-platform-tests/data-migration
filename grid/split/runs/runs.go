package runs

import (
	"fmt"
	"reflect"

	r "github.com/web-platform-tests/data-migration/grid/reflect"
	"github.com/web-platform-tests/data-migration/grid/split"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

type Query split.Query

type Run shared.TestRun

type Runs interface {
	Put(Run)
	PutBatch([]shared.TestRun)
	Find(Query) ([]Run, error)
	GetAll() []Run
}

type RunsSlice struct {
	runs []Run
	c    chan Run
}

func (rs *RunsSlice) Put(tr Run) {
	rs.c <- tr
}

func (rs *RunsSlice) PutBatch(runs []shared.TestRun) {
	for _, tr := range runs {
		rs.Put(Run(tr))
	}
}

func (rs *RunsSlice) Find(q Query) ([]Run, error) {
	var ok bool
	var err error
	var v reflect.Value
	skip := uint(0)
	limit := int(^uint(0) >> 1)

	if q.Skip != nil {
		v, err = q.Skip.F(reflect.ValueOf(rs.runs))
		if err != nil {
			return nil, err
		}
		skip, ok = v.Interface().(uint)
		if !ok {
			return nil, fmt.Errorf("Expected skip functor to return uint but got %v", v.Type())
		}
	}
	if q.Limit != nil {
		v, err = q.Limit.F(reflect.ValueOf(rs.runs))
		if err != nil {
			return nil, err
		}
		limit, ok = v.Interface().(int)
		if !ok {
			return nil, fmt.Errorf("Expected limit functor to return uint but got %v", v.Type())
		}
	}

	res := make([]Run, 0, len(rs.runs))
	for _, r := range rs.runs {
		if q.Predicate != nil {
			bv, err := q.Predicate.F(reflect.ValueOf(r))
			if err != nil {
				continue
			}
			b, ok := bv.Interface().(bool)
			if !ok {
				continue
			}
			if b {
				res = append(res, r)
			}
		} else {
			res = append(res, r)
		}
	}
	if q.Order != nil {
		v, err = r.FunctorSort(q.Order, reflect.ValueOf(res))
		if err != nil {
			return nil, err
		}
		res, ok = v.Interface().([]Run)
		if !ok {
			return nil, fmt.Errorf("Expected order to return []TestRun but got %v", v.Type())
		}
	}

	if q.Filter != nil {
		v, err = q.Filter.F(reflect.ValueOf(res))
		if err != nil {
			return nil, err
		}
		res, ok = v.Interface().([]Run)
		if !ok {
			return nil, fmt.Errorf("Expected filter to return []TestRun but got %v", v.Type())
		}
	}

	if limit < len(res) {
		if skip > 0 {
			res = res[skip:limit]
		} else {
			res = res[:limit]
		}
	} else if skip > 0 {
		res = res[skip:]
	}

	return res, nil
}

func (rs *RunsSlice) GetAll() []Run {
	return rs.runs
}

func NewRunSlice() Runs {
	ret := &RunsSlice{
		runs: make([]Run, 0, 0),
		c:    make(chan Run),
	}
	go func() {
		for r := range ret.c {
			ret.runs = append(ret.runs, r)
		}
	}()
	return ret
}
