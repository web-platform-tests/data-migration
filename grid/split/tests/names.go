package tests

import (
	"crypto/sha256"
	"fmt"
	"log"
	"reflect"
	"strings"

	r "github.com/web-platform-tests/data-migration/grid/reflect"
	"github.com/web-platform-tests/data-migration/grid/split"
)

type Query struct {
	Term string
	split.Query
}

type ID split.TestKey
type IDs []ID
type Name string
type Names []string
type Rank int

type Test interface {
	Name() Name
	ID() ID
}

type Tests []Test

type RankedTest interface {
	Name() Name
	ID() ID
	Rank() Rank
}

type RankedTests []RankedTest

func (s RankedTests) Len() int {
	return len(s)
}

func (s RankedTests) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s RankedTests) Less(i, j int) bool {
	return s[i].Rank() < s[j].Rank()
}

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

type RankedTestData struct {
	TestRank Rank
	TestData
}

func (td *RankedTestData) Rank() Rank {
	return td.TestRank
}

func NewTest(n Name) Test {
	return &TestData{n, nil}
}

func NewRankedTest(n Name, r Rank) RankedTest {
	return &RankedTestData{r, TestData{n, nil}}
}

type TestNames interface {
	Put(Name)
	PutBatch(Names)
	Find(Query) (RankedTests, error)
	GetAll() RankedTests
}

/*
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

func (ns *FuzzySearchTestNames) Find(q Query) RankedTests {
	matches := fuzzy.RankFind(string(q.Term), []string(ns.ns))
	sort.Sort(matches)
	results := make(RankedTests, 0, len(matches))
	for _, match := range matches {
		results = append(results, NewRankedTest(Name(match.Target), Rank(match.Distance)))
	}
	return results
}

func (ns *FuzzySearchTestNames) GetAll() RankedTests {
	results := make(RankedTests, 0, len(ns.ns))
	for _, n := range ns.ns {
		results = append(results, NewRankedTest(Name(n), 0))
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

func (btn *BleveTestNames) Find(q Query) RankedTests {
	bres, err := btn.i.Search(bleve.NewSearchRequest(bleve.NewMatchQuery(string(q))))
	if err != nil {
		log.Printf("WARN: Bleve search request error: %v", err)
	}
	res := make(RankedTests, 0, len(bres.Hits))
	for _, h := range bres.Hits {
		res = append(res, NewRankedTest(Name(h.ID), Rank(h.HitNumber)))
	}
	return res
}

func (btn *BleveTestNames) GetAll() RankedTests {
	res := make(RankedTests, 0, len(btn.nmap))
	for n := range btn.nmap {
		res = append(res, NewRankedTest(n, 0))
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

func (rtn *RawTestNames) Find(q Query) RankedTests {
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
	res := make(RankedTests, 0, len(bss))
	for _, bs := range bss {
		res = append(res, NewRankedTest(Name(bs), 0))
	}
	return res
}

func (rtn *RawTestNames) GetAll() RankedTests {
	res := make(RankedTests, 0, len(rtn.nmap))
	for n := range rtn.nmap {
		res = append(res, NewRankedTest(n, 0))
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
*/

type STNode struct {
	cs map[rune]*STNode
	i  int16
}

type STTest struct {
	st *STNode
	TestData
}

func (t STTest) match(q string) (int, int) {
	if len(q) == 0 {
		return 0, 0
	}
	st := t.st
	rank := 0
	for i, r := range q {
		st = st.cs[r]
		if st == nil {
			return i, rank
		}
		rank += int(st.i) - i
	}
	return len(q), rank
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

func (sttn *STTestNames) Find(q Query) (RankedTests, error) {
	term := q.Term
	res := make(RankedTests, 0, len(sttn.ts))

	var ok bool
	var err error
	var v reflect.Value
	skip := uint(0)
	limit := int(^uint(0) >> 1)

	if q.Skip != nil {
		v, err = q.Skip.F(reflect.ValueOf(sttn.ts))
		if err != nil {
			return nil, err
		}
		skip, ok = v.Interface().(uint)
		if !ok {
			return nil, fmt.Errorf("Expected skip functor to return uint but got %v", v.Type())
		}
	}
	if q.Limit != nil {
		v, err = q.Limit.F(reflect.ValueOf(sttn.ts))
		if err != nil {
			return nil, err
		}
		limit, ok = v.Interface().(int)
		if !ok {
			return nil, fmt.Errorf("Expected limit functor to return uint but got %v", v.Type())
		}
	}

	for _, stt := range sttn.ts {
		var num, rank int
		if strings.HasPrefix(string(stt.TestName), "/2dcontext/building-paths/") {
			num, rank = stt.match(term)
		} else {
			num, rank = stt.match(term)
		}

		if num != len(term) {
			continue
		}
		t := NewRankedTest(stt.TestName, Rank(rank))

		if len(res) >= limit {
			break
		}

		if q.Predicate != nil {
			bv, err := q.Predicate.F(reflect.ValueOf(t))
			if err != nil {
				continue
			}
			b, ok := bv.Interface().(bool)
			if !ok {
				continue
			}
			if b {
				res = append(res, t)
			}
		} else {
			res = append(res, t)
		}
	}

	if q.Order != nil {
		v, err = r.FunctorSort(q.Order, reflect.ValueOf(res))
		if err != nil {
			return nil, err
		}
		res, ok = v.Interface().(RankedTests)
		if !ok {
			return nil, fmt.Errorf("Expected order to return RankedTests but got %v", v.Type())
		}
	}

	if q.Filter != nil {
		v, err = q.Filter.F(reflect.ValueOf(res))
		if err != nil {
			return nil, err
		}
		res, ok = v.Interface().(RankedTests)
		if !ok {
			return nil, fmt.Errorf("Expected filter to return RankedTests but got %v", v.Type())
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

func (sttn *STTestNames) GetAll() RankedTests {
	res := make(RankedTests, 0, len(sttn.ts))
	for _, t := range sttn.ts {
		res = append(res, NewRankedTest(t.TestName, 0))
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
			if _, ok := ret.nmap[n]; !ok {
				if count%10000 == 0 {
					log.Printf("INFO: Indexing test %d", count)
				}
				ret.nmap[n] = true
				st := &STNode{make(map[rune]*STNode), 0}
				nodes := make([]*STNode, 0, 1)
				nodes = append(nodes, st)
				for i, r := range n {
					node := &STNode{make(map[rune]*STNode), int16(i)}
					for _, prev := range nodes {
						if prev.cs[r] == nil {
							prev.cs[r] = node
						}
					}
					nodes = append(nodes, node)
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
