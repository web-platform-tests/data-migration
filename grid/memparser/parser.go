package memparser

import (
	"errors"
	"strconv"

	"github.com/web-platform-tests/data-migration/grid/mem"

	parsec "github.com/prataprc/goparsec"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

type Queryable interface {
	Run(*mem.Tests, *mem.Results) chan mem.TestID
}

type NameFragment struct {
	Name string
}

func (nf *NameFragment) Run(ts *mem.Tests, rs *mem.Results) chan mem.TestID {
	return ts.QueryChan(nf.Name)
}

type ResultOp struct {
	Name string
}

type ResultFragment struct {
	RunID    mem.RunID
	Op       ResultOp
	ResultID mem.ResultID
}

func (rf *ResultFragment) Run(ts *mem.Tests, rs *mem.Results) chan mem.TestID {
	if rf.Op.Name == "EQ" {
		return rs.QueryChan(rf.RunID, rf.ResultID)
	}
	// TODO: More operators and/or error handling.
	return nil
}

type And struct {
	Parts []Queryable
}

func (a *And) Run(ts *mem.Tests, rs *mem.Results) chan mem.TestID {
	var c chan mem.TestID
	for _, p := range a.Parts {
		if c == nil {
			c = p.Run(ts, rs)
			continue
		}
		c = mem.AndChan(c, p.Run(ts, rs))
	}
	return c
}

var (
	ws        = parsec.TokenExact(`[ \t\r\n\v]+`, "WHITESPACE")
	nameTok   = parsec.Token(`[a-zA-Z/._][0-9a-zA-Z/._-]*`, "NAME")
	nameExpr  = parsec.And(nameExprF, nameTok)
	id        = parsec.Int()
	pass      = parsec.Token(`PASS`, "PASS")
	ok        = parsec.Token(`OK`, "OK")
	errStatus = parsec.Token(`ERROR`, "ERROR")
	timeout   = parsec.Token(`TIMEOUT`, "TIMEOUT")
	notRun    = parsec.Token(`NOT_RUN`, "NOT_RUN")
	fail      = parsec.Token(`FAIL`, "FAIL")
	crash     = parsec.Token(`CRASH`, "CRASH")
	unknown   = parsec.Token(`UNKNOWN`, "UNKNOWN")
	status    = parsec.OrdChoice(statusF, pass, ok, errStatus, timeout, notRun, fail, unknown)
	eq        = parsec.Token("=", "EQ")
	//neq        = parsec.Token("!=", "NEQ")
	statusOp   = parsec.OrdChoice(first, eq /*, neq*/)
	statusExpr = parsec.And(statusExprF, id, statusOp, status)
	q          = parsec.Many(qF, parsec.OrdChoice(first, nameExpr, statusExpr), ws)

	first = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		return pns[0]
	}
	statusF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		return shared.TestStatusValueFromString(pns[0].(*parsec.Terminal).GetValue())
	}
	statusExprF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		// TODO: Handle error.
		id, _ := strconv.ParseInt(pns[0].(*parsec.Terminal).GetValue(), 10, 64)
		op := pns[1].(*parsec.Terminal).GetName()
		status := pns[2].(int64)
		return &ResultFragment{mem.RunID(id), ResultOp{op}, mem.ResultID(status)}
	}
	nameExprF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		name := pns[0].(*parsec.Terminal).GetValue()
		return parsec.ParsecNode(&NameFragment{name})
	}
	qF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		if len(pns) == 0 {
			return nil
		}
		if len(pns) == 1 {
			return parsec.ParsecNode(pns[0])
		}

		qs := make([]Queryable, 0)
		for _, pn := range pns {
			qs = append(qs, pn.(Queryable))
		}
		return &And{qs}
	}
)

func Parse(query string) (Queryable, error) {
	pn, s := q(parsec.NewScanner([]byte(query)))
	if !s.Endof() {
		return nil, errors.New("Parse did not consume all input")
	}
	qable, ok := pn.(Queryable)
	if !ok {
		return nil, errors.New("Parser returned unexpected type of result")
	}
	return qable, nil
}
