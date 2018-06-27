package reflect_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	r "github.com/web-platform-tests/data-migration/grid/reflect"
)

func v(i interface{}) reflect.Value {
	return reflect.ValueOf(i)
}

func testComparator(t *testing.T, small, big reflect.Value) {
	sc := r.GetComparable(small)
	assert.False(t, sc.EqualTo(big))
	assert.True(t, sc.LessThan(big))
	assert.True(t, r.LessThanOrEqualTo(sc, big))
	assert.True(t, r.NotEqualTo(sc, big))
	assert.False(t, r.GreaterThan(sc, big))
	assert.False(t, r.GreaterThanOrEqualTo(sc, big))

	bc := r.GetComparable(big)
	assert.False(t, bc.EqualTo(small))
	assert.False(t, bc.LessThan(small))
	assert.False(t, r.LessThanOrEqualTo(bc, small))
	assert.True(t, r.NotEqualTo(bc, small))
	assert.True(t, r.GreaterThan(bc, small))
	assert.True(t, r.GreaterThanOrEqualTo(bc, small))

	assert.True(t, sc.EqualTo(small))
	assert.False(t, sc.LessThan(small))
	assert.True(t, r.LessThanOrEqualTo(sc, small))
	assert.False(t, r.NotEqualTo(sc, small))
	assert.False(t, r.GreaterThan(sc, small))
	assert.True(t, r.GreaterThanOrEqualTo(sc, small))

	assert.True(t, bc.EqualTo(big))
	assert.False(t, bc.LessThan(big))
	assert.True(t, r.LessThanOrEqualTo(bc, big))
	assert.False(t, r.NotEqualTo(bc, big))
	assert.False(t, r.GreaterThan(bc, big))
	assert.True(t, r.GreaterThanOrEqualTo(bc, big))
}

func TestStringComparator(t *testing.T) {
	testComparator(t, v("a"), v("b"))
}

func TestBoolComparator(t *testing.T) {
	testComparator(t, v(false), v(true))
}

func TestNumComparator(t *testing.T) {
	testComparator(t, v(uint8(0)), v(uint8(1)))
	testComparator(t, v(uint16(0)), v(uint16(1)))
	testComparator(t, v(uint16(0)), v(uint16(1)))
	testComparator(t, v(uint32(0)), v(uint32(1)))
	testComparator(t, v(uint64(0)), v(uint64(1)))

	testComparator(t, v(int8(0)), v(int8(1)))
	testComparator(t, v(int16(0)), v(int16(1)))
	testComparator(t, v(int16(0)), v(int16(1)))
	testComparator(t, v(int32(0)), v(int32(1)))
	testComparator(t, v(int64(0)), v(int64(1)))

	testComparator(t, v(float32(0.1)), v(float32(0.2)))
	testComparator(t, v(float64(0.1)), v(float64(0.2)))
}

func TestArrayComparable(t *testing.T) {
	testComparator(t, v([2]int{0, 1}), v([2]int{0, 2}))
	testComparator(t, v([1]int{1}), v([2]int{0, 0}))
}

func TestSliceComparable(t *testing.T) {
	testComparator(t, v([]int{0, 1}), v([]int{0, 2}))
	testComparator(t, v([]int{1}), v([]int{0, 0}))
}
