package tests

import (
	"sort"

	"github.com/lithammer/fuzzysearch/fuzzy"
)

type Query string
type Name string
type Names []string

type TestNames interface {
	Put(Name)
	PutBatch(Names)
	Find(Query) Names
}

type FuzzySearchTestNames struct {
	ns Names
	c  chan Name
}

func (ns *FuzzySearchTestNames) Put(n Name) {
	ns.c <- n
}

func (ns *FuzzySearchTestNames) PutBatch(names Names) {
	for _, n := range names {
		ns.Put(Name(n))
	}
}

func (ns *FuzzySearchTestNames) Find(q Query) Names {
	matches := fuzzy.RankFind(string(q), []string(ns.ns))
	sort.Sort(matches)
	results := make(Names, 0, len(matches))
	for _, match := range matches {
		results = append(results, match.Target)
	}
	return results
}
