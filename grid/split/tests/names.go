package tests

import (
	"bytes"
	"crypto/sha256"
	"log"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/blevesearch/bleve"

	"github.com/blevesearch/bleve/mapping"

	"github.com/web-platform-tests/data-migration/grid/split"

	"github.com/lithammer/fuzzysearch/fuzzy"
)

type Query string
type ID split.TestKey
type IDs []ID
type Name string
type Names []string

type Test interface {
	Name() Name
	ID() ID
}

type Tests []Test

type TestData struct {
	TestName Name `json:"name"`
	TestID   *ID  `json:"id"`
}

func (td *TestData) Name() Name {
	return td.TestName
}

func (td *TestData) ID() ID {
	if td.TestID == nil {
		id := ID(sha256.Sum256([]byte(td.TestName)))
		td.TestID = &id
	}
	return *td.TestID
}

func NewTest(n Name) Test {
	return &TestData{n, nil}
}

type TestNames interface {
	Put(Name)
	PutBatch(Names)
	Find(Query) Tests
	GetAll() Tests
}

type FuzzySearchTestNames struct {
	ns   Names
	nmap map[Name]bool
	c    chan Name
}

func (ns *FuzzySearchTestNames) Put(n Name) {
	ns.c <- n
}

func (ns *FuzzySearchTestNames) PutBatch(names Names) {
	for _, n := range names {
		ns.Put(Name(n))
	}
}

func (ns *FuzzySearchTestNames) Find(q Query) Tests {
	matches := fuzzy.RankFind(string(q), []string(ns.ns))
	sort.Sort(matches)
	results := make(Tests, 0, len(matches))
	for _, match := range matches {
		results = append(results, &TestData{Name(match.Target), nil})
	}
	return results
}

func (ns *FuzzySearchTestNames) GetAll() Tests {
	results := make(Tests, 0, len(ns.ns))
	for _, n := range ns.ns {
		results = append(results, &TestData{Name(n), nil})
	}
	return results
}

func NewFuzzySearchTestNames() TestNames {
	ret := &FuzzySearchTestNames{
		ns:   make(Names, 0, 0),
		nmap: make(map[Name]bool),
		c:    make(chan Name),
	}
	go func() {
		for n := range ret.c {
			if _, ok := ret.nmap[n]; !ok {
				ret.nmap[n] = true
				ret.ns = append(ret.ns, string(n))
			}
		}
	}()
	return ret
}

type BleveTestNames struct {
	m    mapping.IndexMapping
	i    bleve.Index
	nmap map[Name]bool
	c    chan Name
}

func (btn *BleveTestNames) Put(n Name) {
	btn.c <- n
}

func (btn *BleveTestNames) PutBatch(ns Names) {
	for _, n := range ns {
		btn.Put(Name(n))
	}
}

func (btn *BleveTestNames) Find(q Query) Tests {
	bres, err := btn.i.Search(bleve.NewSearchRequest(bleve.NewMatchQuery(string(q))))
	if err != nil {
		log.Printf("WARN: Bleve search request error: %v", err)
	}
	res := make(Tests, 0, len(bres.Hits))
	for _, h := range bres.Hits {
		res = append(res, NewTest(Name(h.ID)))
	}
	return res
}

func (btn *BleveTestNames) GetAll() Tests {
	res := make(Tests, 0, len(btn.nmap))
	for n := range btn.nmap {
		res = append(res, NewTest(n))
	}
	return res
}

func NewBleveTestNames() TestNames {
	m := bleve.NewIndexMapping()
	i, err := bleve.NewMemOnly(m)
	if err != nil {
		log.Printf("WARN: Bleve instantiation error: %v", err)
	}
	ret := &BleveTestNames{
		m:    m,
		i:    i,
		nmap: make(map[Name]bool),
		c:    make(chan Name),
	}
	go func() {
		for n := range ret.c {
			if _, ok := ret.nmap[n]; !ok {
				ret.nmap[n] = true
				err := ret.i.Index(string(n), n)
				if err != nil {
					log.Printf("WARN: Bleve store error: %v", err)
				}
			}
		}
	}()
	return ret
}

type RawTestNames struct {
	b    *bytes.Buffer
	nmap map[Name]bool
	c    chan Name
}

func (rtn *RawTestNames) Put(n Name) {
	rtn.c <- n

}

func (rtn *RawTestNames) PutBatch(ns Names) {
	for _, n := range ns {
		rtn.Put(Name(n))
	}
}

func (rtn *RawTestNames) Find(q Query) Tests {
	var buf bytes.Buffer
	buf.WriteRune(utf8.MaxRune)
	for _, c := range q {
		buf.WriteString("[^")
		buf.WriteRune(utf8.MaxRune)
		buf.WriteString("]*")
		buf.WriteRune(c)
	}
	buf.WriteString("[^")
	buf.WriteRune(utf8.MaxRune)
	buf.WriteString("]*")
	buf.WriteRune(utf8.MaxRune)
	re := regexp.MustCompile(buf.String())
	bss := re.FindAll(rtn.b.Bytes(), -1)
	res := make(Tests, 0, len(bss))
	for _, bs := range bss {
		res = append(res, NewTest(Name(bs)))
	}
	return res
}

func (rtn *RawTestNames) GetAll() Tests {
	res := make(Tests, 0, len(rtn.nmap))
	for n := range rtn.nmap {
		res = append(res, NewTest(n))
	}
	return res
}

func NewRawTestNames() TestNames {
	ret := &RawTestNames{
		b:    bytes.NewBufferString(string([]rune{utf8.MaxRune})),
		nmap: make(map[Name]bool),
		c:    make(chan Name),
	}
	go func() {
		for n := range ret.c {
			if _, ok := ret.nmap[n]; !ok {
				ret.nmap[n] = true
				ret.b.WriteString(string(n))
				ret.b.WriteRune(utf8.MaxRune)
			}
		}
	}()
	return ret
}

type STNode struct {
	cs map[rune]*STNode
	r  rune
}

type STTest struct {
	st *STNode
	TestData
}

func (t STTest) match(q string) bool {
	if len(q) == 0 {
		return true
	}
	st := t.st
	head := make(map[rune]*STNode)
	for k, v := range st.cs {
		head[k] = v
	}
	for _, r := range q {
		n := head[r]
		if n == nil {
			return false
		}
		head[n.r] = n
	}
	return true
}

type STTestNames struct {
	ts   []STTest
	nmap map[Name]bool
	c    chan Name
}

func (sttn *STTestNames) Put(n Name) {
	sttn.c <- n
}

func (sttn *STTestNames) PutBatch(ns Names) {
	log.Printf("INFO: Queueing %d tests", len(ns))
	for _, n := range ns {
		sttn.Put(Name(n))
	}
	log.Printf("INFO: Done queueing %d tests", len(ns))
}

func (sttn *STTestNames) Find(q Query) Tests {
	res := make(Tests, 0, len(sttn.ts))
	for _, t := range sttn.ts {
		if t.match(string(q)) {
			res = append(res, &t)
		}
	}
	return res
}

func (sttn *STTestNames) GetAll() Tests {
	res := make(Tests, 0, len(sttn.ts))
	for _, t := range sttn.ts {
		res = append(res, &t)
	}
	return res
}

func NewSTTestNames() TestNames {
	ret := &STTestNames{
		ts:   make([]STTest, 0),
		nmap: make(map[Name]bool),
		c:    make(chan Name),
	}
	go func() {
		count := 0
		for n := range ret.c {
			// Do not index subtest names.
			idx := strings.Index(string(n), ":")
			if idx >= 0 {
				n = n[:idx]
			}
			if _, ok := ret.nmap[n]; !ok {
				if count%10000 == 0 {
					log.Printf("INFO: Indexing test %d", count)
				}
				ret.nmap[n] = true
				st := &STNode{make(map[rune]*STNode), rune(0)}
				head := make(map[rune]*STNode)
				head[st.r] = st
				for _, r := range n {
					node := head[r]
					if node == nil {
						node = &STNode{make(map[rune]*STNode), r}
						for _, headNode := range head {
							headNode.cs[r] = node
						}
						head[r] = node
					}
				}
				ret.ts = append(ret.ts, STTest{
					st,
					TestData{n, nil},
				})
				count++
			}
		}
	}()
	return ret
}

/*
rtn.b.WriteRune(utf8.MaxRune)
	rtn.b.WriteString(string(n))
*/
