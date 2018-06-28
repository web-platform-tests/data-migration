package split

import (
	"crypto/sha256"

	r "github.com/web-platform-tests/data-migration/grid/reflect"
)

type TestKey [sha256.Size]byte

type RunKey int64

type TestStatus uint8

type RunTestStatus map[RunKey]map[TestKey]TestStatus

type Query struct {
	Predicate r.ValueFunctor
	Skip      r.ValueFunctor
	Limit     r.ValueFunctor
	Order     r.ValueFunctor
	Filter    r.ValueFunctor
}
