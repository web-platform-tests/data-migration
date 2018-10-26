package memparser

import (
	"testing"

	parsec "github.com/prataprc/goparsec"
	"github.com/stretchr/testify/assert"
	"github.com/web-platform-tests/data-migration/grid/mem"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

func parse(t *testing.T, query string) Filterable {
	pn, s := q(parsec.NewScanner([]byte(query)))
	assert.True(t, s.Endof())
	v, ok := pn.(Filterable)
	assert.True(t, ok)
	return v
}

func TestName_single(t *testing.T) {
	v := parse(t, "/2dcontext/")
	part, ok := v.(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, part)
	assert.Equal(t, NameFragment{"/2dcontext/"}, *part)
}

func TestName_multi(t *testing.T) {
	v := parse(t, "2dcontext andThis")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := a.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, NameFragment{"2dcontext"}, *p1)
	assert.Equal(t, NameFragment{"andThis"}, *p2)
}

func TestRun(t *testing.T) {
	v := parse(t, "-42=PASS")
	part, ok := v.(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, part)
	assert.Equal(t, ResultFragment{-42, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("PASS"))}, *part)
}

func TestMix(t *testing.T) {
	v := parse(t, "_foo -43=TIMEOUT")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := a.Parts[1].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, NameFragment{"_foo"}, *p1)
	assert.Equal(t, ResultFragment{-43, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("TIMEOUT"))}, *p2)
}

func TestAnd_name(t *testing.T) {
	v := parse(t, "a and orStart")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := a.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, NameFragment{"a"}, *p1)
	assert.Equal(t, NameFragment{"orStart"}, *p2)
}

func TestAnd_run(t *testing.T) {
	v := parse(t, "-43=TIMEOUT and -42=PASS")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := a.Parts[1].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, ResultFragment{-43, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("TIMEOUT"))}, *p1)
	assert.Equal(t, ResultFragment{-42, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("PASS"))}, *p2)
}

func TestAnd_mixed(t *testing.T) {
	v := parse(t, "-42=PASS and a")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := a.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, ResultFragment{-42, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("PASS"))}, *p1)
	assert.Equal(t, NameFragment{"a"}, *p2)
}

func TestAmpersand_name(t *testing.T) {
	v := parse(t, "a & b")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := a.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, NameFragment{"a"}, *p1)
	assert.Equal(t, NameFragment{"b"}, *p2)
}

func TestAmpersand_run(t *testing.T) {
	v := parse(t, "-43=TIMEOUT&-42=PASS")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := a.Parts[1].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, ResultFragment{-43, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("TIMEOUT"))}, *p1)
	assert.Equal(t, ResultFragment{-42, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("PASS"))}, *p2)
}

func TestAmpersand_mixed(t *testing.T) {
	v := parse(t, "-42=PASS & a")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := a.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, ResultFragment{-42, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("PASS"))}, *p1)
	assert.Equal(t, NameFragment{"a"}, *p2)
}

func TestOr_name(t *testing.T) {
	v := parse(t, "a or b")
	o, ok := v.(*Or)
	assert.True(t, ok)
	assert.NotNil(t, o)
	assert.Equal(t, 2, len(o.Parts))
	p1, ok := o.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := o.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, NameFragment{"a"}, *p1)
	assert.Equal(t, NameFragment{"b"}, *p2)
}

func TestOr_run(t *testing.T) {
	v := parse(t, "-43=TIMEOUT or -42=PASS")
	o, ok := v.(*Or)
	assert.True(t, ok)
	assert.NotNil(t, o)
	assert.Equal(t, 2, len(o.Parts))
	p1, ok := o.Parts[0].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := o.Parts[1].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, ResultFragment{-43, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("TIMEOUT"))}, *p1)
	assert.Equal(t, ResultFragment{-42, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("PASS"))}, *p2)
}

func TestOr_mixed(t *testing.T) {
	v := parse(t, "-42=PASS or a")
	o, ok := v.(*Or)
	assert.True(t, ok)
	assert.NotNil(t, o)
	assert.Equal(t, 2, len(o.Parts))
	p1, ok := o.Parts[0].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := o.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, ResultFragment{-42, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("PASS"))}, *p1)
	assert.Equal(t, NameFragment{"a"}, *p2)
}

func TestVbar_name(t *testing.T) {
	v := parse(t, "a|b")
	o, ok := v.(*Or)
	assert.True(t, ok)
	assert.NotNil(t, o)
	assert.Equal(t, 2, len(o.Parts))
	p1, ok := o.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := o.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, NameFragment{"a"}, *p1)
	assert.Equal(t, NameFragment{"b"}, *p2)
}

func TestVbar_run(t *testing.T) {
	v := parse(t, "-43=TIMEOUT | -42=PASS")
	o, ok := v.(*Or)
	assert.True(t, ok)
	assert.NotNil(t, o)
	assert.Equal(t, 2, len(o.Parts))
	p1, ok := o.Parts[0].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := o.Parts[1].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, ResultFragment{-43, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("TIMEOUT"))}, *p1)
	assert.Equal(t, ResultFragment{-42, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("PASS"))}, *p2)
}

func TestVbar_mixed(t *testing.T) {
	v := parse(t, "42=PASS | a")
	o, ok := v.(*Or)
	assert.True(t, ok)
	assert.NotNil(t, o)
	assert.Equal(t, 2, len(o.Parts))
	p1, ok := o.Parts[0].(*ResultFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := o.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	assert.Equal(t, ResultFragment{42, ResultOp{"EQ"}, mem.ResultID(shared.TestStatusValueFromString("PASS"))}, *p1)
	assert.Equal(t, NameFragment{"a"}, *p2)
}

func TestAndOr_looseAnd(t *testing.T) {
	v := parse(t, "a b or c")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	o, ok := a.Parts[1].(*Or)
	assert.True(t, ok)
	assert.NotNil(t, o)
	assert.Equal(t, 2, len(o.Parts))
	p2, ok := o.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	p3, ok := o.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p3)
	assert.Equal(t, NameFragment{"a"}, *p1)
	assert.Equal(t, NameFragment{"b"}, *p2)
	assert.Equal(t, NameFragment{"c"}, *p3)
}

func TestAndOr_tightAnd(t *testing.T) {
	v := parse(t, "a and b or c")
	o, ok := v.(*Or)
	assert.True(t, ok)
	assert.NotNil(t, o)
	assert.Equal(t, 2, len(o.Parts))
	a, ok := o.Parts[0].(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	p2, ok := a.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	p3, ok := o.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p3)
	assert.Equal(t, NameFragment{"a"}, *p1)
	assert.Equal(t, NameFragment{"b"}, *p2)
	assert.Equal(t, NameFragment{"c"}, *p3)
}

func TestAndOr_parens(t *testing.T) {
	v := parse(t, "a and (b or c)")
	a, ok := v.(*And)
	assert.True(t, ok)
	assert.NotNil(t, a)
	assert.Equal(t, 2, len(a.Parts))
	p1, ok := a.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p1)
	o, ok := a.Parts[1].(*Or)
	assert.True(t, ok)
	assert.NotNil(t, o)
	assert.Equal(t, 2, len(o.Parts))
	p2, ok := o.Parts[0].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p2)
	p3, ok := o.Parts[1].(*NameFragment)
	assert.True(t, ok)
	assert.NotNil(t, p3)
	assert.Equal(t, NameFragment{"a"}, *p1)
	assert.Equal(t, NameFragment{"b"}, *p2)
	assert.Equal(t, NameFragment{"c"}, *p3)
}

func TestNot(t *testing.T) {
	v := parse(t, "not a")
	n, ok := v.(*Not)
	assert.True(t, ok)
	assert.NotNil(t, n)
	p, ok := n.Part.(*NameFragment)
	assert.True(t, ok)
	assert.Equal(t, NameFragment{"a"}, *p)
}

func TestBang(t *testing.T) {
	v := parse(t, " ! a ")
	n, ok := v.(*Not)
	assert.True(t, ok)
	assert.NotNil(t, n)
	p, ok := n.Part.(*NameFragment)
	assert.True(t, ok)
	assert.Equal(t, NameFragment{"a"}, *p)
}

func TestQ(t *testing.T) {
	parse(t, "cssom (5818444842795008=PASS or 5818444842795008=OK) !(5096223207849984=PASS or 5096223207849984=OK)")
}
