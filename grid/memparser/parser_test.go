package memparser

import (
	"testing"

	parsec "github.com/prataprc/goparsec"
	"github.com/stretchr/testify/assert"
	"github.com/web-platform-tests/data-migration/grid/mem"
	"github.com/web-platform-tests/wpt.fyi/shared"
)

func parse(t *testing.T, query string) Queryable {
	pn, s := q(parsec.NewScanner([]byte(query)))
	assert.True(t, s.Endof())
	v, ok := pn.(Queryable)
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
	v := parse(t, "/2dcontext/ blob")
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
	assert.Equal(t, NameFragment{"/2dcontext/"}, *p1)
	assert.Equal(t, NameFragment{"blob"}, *p2)
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
