package memparser

import (
	"errors"
	"strconv"

	"github.com/web-platform-tests/data-migration/grid/mem"

	parsec "github.com/prataprc/goparsec"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

type Queryable interface {
	RunAll(*mem.Tests, *mem.Results) chan mem.TestID
	RunChan(*mem.Tests, *mem.Results, chan mem.TestID) chan mem.TestID
}

type NameFragment struct {
	Name string
}

type ResultOp struct {
	Name string
}

type ResultFragment struct {
	RunID    mem.RunID
	Op       ResultOp
	ResultID mem.ResultID
}

type And struct {
	Parts []Filterable
}

type Or struct {
	Parts []Filterable
}

var (
	ws         = parsec.TokenExact(`[ \t\r\n\v]+`, "WHITESPACE")
	and        = parsec.Token(`([aA][nN][dD]|[&])`, "AND")
	or         = parsec.Token(`([oO][rR]|[|])`, "OR")
	nameTok    = parsec.Token(`[a-zA-Z/._][0-9a-zA-Z/._-]*`, "NAME")
	nameExpr   = parsec.And(nameExprF, nameTok)
	id         = parsec.Int()
	pass       = parsec.Token(`PASS`, "PASS")
	ok         = parsec.Token(`OK`, "OK")
	errStatus  = parsec.Token(`ERROR`, "ERROR")
	timeout    = parsec.Token(`TIMEOUT`, "TIMEOUT")
	notRun     = parsec.Token(`NOT_RUN`, "NOT_RUN")
	fail       = parsec.Token(`FAIL`, "FAIL")
	crash      = parsec.Token(`CRASH`, "CRASH")
	unknown    = parsec.Token(`UNKNOWN`, "UNKNOWN")
	status     = parsec.OrdChoice(statusF, pass, ok, errStatus, timeout, notRun, fail, unknown)
	eq         = parsec.Token("=", "EQ")
	neq        = parsec.Token("!=", "NEQ")
	statusOp   = parsec.OrdChoice(first, eq, neq)
	statusExpr = parsec.And(statusExprF, id, statusOp, status)
	atomExpr   = parsec.OrdChoice(first, nameExpr, statusExpr)
	andExpr    = parsec.And(andExprF, atomExpr, parsec.Maybe(first, and), atomExpr)
	orExpr     = parsec.And(orExprF, atomExpr, or, atomExpr)
	innerExpr  = parsec.OrdChoice(first, orExpr, andExpr, atomExpr)
	parenExpr  parsec.Parser // Defined in init() to avoid parenExpr<-->expr loop.
	expr       = parsec.OrdChoice(first, &parenExpr, innerExpr)

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
	andExprF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		return &And{[]Filterable{pns[0].(Filterable), pns[2].(Filterable)}}
	}
	orExprF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		return &Or{[]Filterable{pns[0].(Filterable), pns[2].(Filterable)}}
	}
	parenExprF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		return pns[1]
	}
)

func init() {
	parenExpr = parsec.And(parenExprF, parsec.Token(`[(]`, "LPAREN"), &expr, parsec.Token(`[)]`, "RPAREN"))
}

func Parse(query string) (Filterable, error) {
	pn, s := expr(parsec.NewScanner([]byte(query)))
	if !s.Endof() {
		return nil, errors.New("Parse did not consume all input")
	}
	fable, ok := pn.(Filterable)
	if !ok {
		return nil, errors.New("Parser returned unexpected type of result")
	}
	return fable, nil
}
