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

type Not struct {
	Part Filterable
}

var (
	ws        parsec.Parser
	and       parsec.Parser
	or        parsec.Parser
	nameTok   parsec.Parser
	nameExpr  parsec.Parser
	id        parsec.Parser
	pass      parsec.Parser
	ok        parsec.Parser
	errStatus parsec.Parser
	timeout   parsec.Parser
	notRun    parsec.Parser
	fail      parsec.Parser
	crash     parsec.Parser
	unknown   parsec.Parser
	status    parsec.Parser
	eq        parsec.Parser
	// neq        parsec.Parser
	not        parsec.Parser
	statusOp   parsec.Parser
	statusExpr parsec.Parser
	atomExpr   parsec.Parser
	notExpr    parsec.Parser
	parenExpr  parsec.Parser
	expr       parsec.Parser
	orPart     parsec.Parser
	andPart    parsec.Parser
	q          parsec.Parser

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
	notExprF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		return &Not{pns[1].(Filterable)}
	}
	nameExprF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		name := pns[0].(*parsec.Terminal).GetValue()
		return parsec.ParsecNode(&NameFragment{name})
	}
	andF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		if len(pns) == 1 {
			return pns[0]
		}
		return &And{fable(pns)}
	}
	orF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		if len(pns) == 1 {
			return pns[0]
		}
		return &Or{fable(pns)}
	}
	parenF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		return pns[1]
	}
	qF = func(pns []parsec.ParsecNode) parsec.ParsecNode {
		if len(pns) == 1 {
			return pns[0]
		}

		fs := make([]Filterable, 0, len(pns))
		for _, pn := range pns {
			fs = append(fs, pn.(Filterable))
		}
		return &And{fs}
	}

	fable = func(pns []parsec.ParsecNode) []Filterable {
		res := make([]Filterable, 0, len(pns))
		for _, pn := range pns {
			res = append(res, pn.(Filterable))
		}
		return res
	}
)

func init() {
	ws = parsec.TokenExact(`[ \t\r\n\v]+`, "WHITESPACE")
	and = parsec.Token(`([aA][nN][dD][ \t\r\n\v]+|[&])`, "AND")
	or = parsec.Token(`([oO][rR][ \t\r\n\v]+|[|])`, "OR")
	nameTok = parsec.Token(`[0-9a-zA-Z/._][0-9a-zA-Z/._-]*`, "NAME")
	nameExpr = parsec.And(nameExprF, &nameTok)
	id = parsec.Int()
	pass = parsec.Token(`PASS`, "PASS")
	ok = parsec.Token(`OK`, "OK")
	errStatus = parsec.Token(`ERROR`, "ERROR")
	timeout = parsec.Token(`TIMEOUT`, "TIMEOUT")
	notRun = parsec.Token(`NOT_RUN`, "NOT_RUN")
	fail = parsec.Token(`FAIL`, "FAIL")
	crash = parsec.Token(`CRASH`, "CRASH")
	unknown = parsec.Token(`UNKNOWN`, "UNKNOWN")
	status = parsec.OrdChoice(statusF, &pass, &ok, &errStatus, &timeout, &notRun, &fail, &unknown)
	eq = parsec.Token(`=`, "EQ")
	// neq = parsec.Token(`!=`, "NEQ")
	not = parsec.Token(`([nN][oO][tT][ \t\r\n\v]+|!)`, "NOT")
	statusOp = parsec.OrdChoice(first, &eq /* , &neq */)
	statusExpr = parsec.And(statusExprF, &id, &statusOp, &status)
	atomExpr = parsec.OrdChoice(first, &statusExpr, &nameExpr)
	notExpr = parsec.And(notExprF, &not, &expr)
	parenExpr = parsec.And(parenF, parsec.Token(`[(]`, "LPAREN"), &expr, parsec.Token(`[)]`, "RPAREN"))
	expr = parsec.Many(orF, &orPart, &or)
	orPart = parsec.Many(andF, &andPart, &and)
	andPart = parsec.OrdChoice(first, &notExpr, &parenExpr, &atomExpr)
	q = parsec.Many(qF, &expr, &ws)
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
