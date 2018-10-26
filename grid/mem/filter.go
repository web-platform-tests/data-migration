package mem

import "strings"

type Filter interface {
	Exec(TestID) bool
}

type UnboundFilter interface {
	Bind(*TestsResults) Filter
}

type UnboundTestNameFilter struct {
	Q string
}

type TestNameFilter struct {
	UnboundTestNameFilter
	ts *Tests
}

func (f *UnboundTestNameFilter) Bind(tr *TestsResults) Filter {
	return &TestNameFilter{*f, tr.Tests}
}

func (f *TestNameFilter) Exec(t TestID) bool {
	return strings.Contains(f.ts.Tests[t], f.Q)
}

func NewTestNameFilter(q string) *UnboundTestNameFilter {
	return &UnboundTestNameFilter{q}
}

type UnboundResultEQFilter struct {
	RunID
	ResultID
}

type ResultEQFilter struct {
	UnboundResultEQFilter
	m map[TestID]ResultID
}

func (f *UnboundResultEQFilter) Bind(tr *TestsResults) Filter {
	return &ResultEQFilter{*f, tr.Results.ByRunTest[f.RunID]}
}

func (f *ResultEQFilter) Exec(t TestID) bool {
	return f.m[t] == f.ResultID
}

func NewResultEQFilter(ru RunID, re ResultID) *UnboundResultEQFilter {
	return &UnboundResultEQFilter{ru, re}
}

type UnboundAnd struct {
	fs []UnboundFilter
}

func (f *UnboundAnd) Bind(tr *TestsResults) Filter {
	fs := make([]Filter, 0, len(f.fs))
	for _, f := range f.fs {
		fs = append(fs, f.Bind(tr))
	}
	return &And{fs}
}

type And struct {
	fs []Filter
}

func (f *And) Exec(t TestID) bool {
	for _, fp := range f.fs {
		if !fp.Exec(t) {
			return false
		}
	}
	return true
}

func NewAnd(ufs ...UnboundFilter) *UnboundAnd {
	return &UnboundAnd{ufs}
}

type Or struct {
	fs []Filter
}

type UnboundOr struct {
	fs []UnboundFilter
}

func (f *UnboundOr) Bind(tr *TestsResults) Filter {
	fs := make([]Filter, 0, len(f.fs))
	for _, f := range f.fs {
		fs = append(fs, f.Bind(tr))
	}
	return &Or{fs}
}

func (f *Or) Exec(t TestID) bool {
	for _, fp := range f.fs {
		if fp.Exec(t) {
			return true
		}
	}
	return false
}

func NewOr(ufs ...UnboundFilter) *UnboundOr {
	return &UnboundOr{ufs}
}

type UnboundNot struct {
	f UnboundFilter
}

func (f *UnboundNot) Bind(tr *TestsResults) Filter {
	return &Not{f.Bind(tr)}
}

type Not struct {
	f Filter
}

func (f *Not) Exec(t TestID) bool {
	return !f.f.Exec(t)
}

func NewNot(uf UnboundFilter) *UnboundNot {
	return &UnboundNot{uf}
}

// // type Filter func(*Tests, *Results, TestID) bool

// func And(fs ...Filter) Filter {
// 	return struct{ Exec func(t TestID) bool }{
// 		Exec: func(t TestID) bool {
// 			for _, f := range fs {
// 				if !f.Exec(t) {
// 					return false
// 				}
// 			}
// 			return true
// 		},
// 	}
// }

// func Or(fs ...Filter) Filter {
// 	return func(ts *Tests, rs *Results, t TestID) bool {
// 		for _, f := range fs {
// 			if f(ts, rs, t) {
// 				return true
// 			}
// 		}
// 		return false
// 	}
// }

// func Not(f Filter) Filter {
// 	return func(ts *Tests, rs *Results, t TestID) bool {
// 		return !f(ts, rs, t)
// 	}
// }
