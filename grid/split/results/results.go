package results

import "github.com/web-platform-tests/data-migration/grid/split"

type Key struct {
	split.RunKey
	split.TestKey
}

type Value split.TestStatus

type KeyValue struct {
	Key
	Value
}

type Results interface {
	Get(Key) Value
	GetBatch([]Key) []Value
	Put(KeyValue)
	PutBatch([]KeyValue)
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
