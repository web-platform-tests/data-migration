package results

import (
	"fmt"
	"reflect"

	r "github.com/web-platform-tests/data-migration/grid/reflect"
	"github.com/web-platform-tests/data-migration/grid/split"
)

type Key struct {
	split.RunKey
	split.TestKey
}

type Value []split.TestStatus

type Query split.Query

type KeyValue struct {
	Key
	Value
}

type TestResults struct {
	Test    split.TestKey `json:"test"`
	Results []Value       `json:"results"`
}

type Results interface {
	Get(Key) Value
	GetBatch([]Key) []Value
	Put(KeyValue)
	PutBatch([]KeyValue)
	Find([]split.RunKey, []split.TestKey, Query) ([]TestResults, error)
	GetAll([]split.RunKey, []split.TestKey) ([]TestResults, error)
}

type ResultsMap struct {
	m map[Key]Value
	c chan KeyValue
}

func (m *ResultsMap) Get(k Key) Value {
	return m.m[k]
}

func (m *ResultsMap) GetBatch(ks []Key) []Value {
	vs := make([]Value, 0, len(ks))
	for _, k := range ks {
		vs = append(vs, m.Get(k))
	}
	return vs
}

func (m *ResultsMap) Put(kv KeyValue) {
	m.c <- kv
}

func (m *ResultsMap) PutBatch(kvs []KeyValue) {
	for _, kv := range kvs {
		m.Put(kv)
	}
}

func (m *ResultsMap) Find(rs []split.RunKey, ts []split.TestKey, q Query) ([]TestResults, error) {
	var ok bool
	var err error
	var v reflect.Value
	skip := uint(0)
	limit := int(^uint(0) >> 1)

	if q.Skip != nil {
		v, err = q.Skip.F(reflect.ValueOf(m))
		if err != nil {
			return nil, err
		}
		skip, ok = v.Interface().(uint)
		if !ok {
			return nil, fmt.Errorf("Expected skip functor to return uint but got %v", v.Type())
		}
	}
	if q.Limit != nil {
		v, err = q.Limit.F(reflect.ValueOf(m))
		if err != nil {
			return nil, err
		}
		limit, ok = v.Interface().(int)
		if !ok {
			return nil, fmt.Errorf("Expected limit functor to return uint but got %v", v.Type())
		}
	}

	ress := make([]TestResults, 0, len(ts))
	for _, t := range ts {
		res := TestResults{t, make([]Value, 0, len(rs))}
		for _, r := range rs {
			res.Results = append(res.Results, m.m[Key{r, t}])
		}

		if len(ress) >= limit {
			break
		}

		if q.Predicate != nil {
			bv, err := q.Predicate.F(reflect.ValueOf(res.Results))
			if err != nil {
				continue
			}
			b, ok := bv.Interface().(bool)
			if !ok {
				continue
			}
			if b {
				ress = append(ress, res)
			}
		} else {
			ress = append(ress, res)
		}
	}

	if q.Order != nil {
		v, err = r.FunctorSort(q.Order, reflect.ValueOf(ress))
		if err != nil {
			return nil, err
		}
		ress, ok = v.Interface().([]TestResults)
		if !ok {
			return nil, fmt.Errorf("Expected order to return []TestResults but got %v", v.Type())
		}
	}

	if q.Filter != nil {
		v, err = q.Filter.F(reflect.ValueOf(ress))
		if err != nil {
			return nil, err
		}
		ress, ok = v.Interface().([]TestResults)
		if !ok {
			return nil, fmt.Errorf("Expected filter to return []TestResults but got %v", v.Type())
		}
	}

	if limit < len(ress) {
		if skip > 0 {
			ress = ress[skip:limit]
		} else {
			ress = ress[:limit]
		}
	} else if skip > 0 {
		ress = ress[skip:]
	}

	return ress, nil
}

func (m *ResultsMap) GetAll(rs []split.RunKey, ts []split.TestKey) ([]TestResults, error) {
	ress := make([]TestResults, 0, len(ts))
	for _, t := range ts {
		res := TestResults{t, make([]Value, 0, len(rs))}
		for _, r := range rs {
			res.Results = append(res.Results, m.m[Key{r, t}])
		}
		ress = append(ress, res)
	}

	return ress, nil
}

func NewResultsMap() *ResultsMap {
	rm := &ResultsMap{
		m: make(map[Key]Value),
		c: make(chan KeyValue),
	}

	go func() {
		for kv := range rm.c {
			rm.m[kv.Key] = kv.Value
		}
	}()

	return rm
}
