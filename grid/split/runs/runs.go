package runs

import "github.com/web-platform-tests/wpt.fyi/shared"

type Query interface{}

type Runs interface {
	Put(shared.TestRun)
	PutBatch([]shared.TestRun)
	Find(Query) []shared.TestRun
}

type RunsSlice struct {
	runs []shared.TestRun
	c    chan shared.TestRun
}

func (rs *RunsSlice) Put(tr shared.TestRun) {
	rs.c <- tr
}

func (rs *RunsSlice) PutBatch(runs []shared.TestRun) {
	for _, tr := range runs {
		rs.Put(tr)
	}
}

func (rs *RunsSlice) Find(Query) []shared.TestRun {
	// TODO(markdittmer): Implement.
	return rs.runs[0:5]
}
