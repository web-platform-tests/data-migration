package reflect_test

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

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
	smalls := []reflect.Value{
		v(uint8(0)),
		v(uint16(0)),
		v(uint32(0)),
		v(uint64(0)),
		v(int8(-1)),
		v(int16(-1)),
		v(int32(-1)),
		v(int64(-1)),
		v(int8(-int8(^uint8(0)>>1) - 1)),
		v(int16(-int16(^uint16(0)>>1) - 1)),
		v(int32(-int32(^uint32(0)>>1) - 1)),
		v(int64(-int64(^uint64(0)>>1) - 1)),
		v(int16(-1)),
		v(int32(-1)),
		v(int64(-1)),
		v(float32(-0.1)),
		v(float64(-0.1)),
		v(-math.MaxFloat32),
		v(-math.MaxFloat64),
	}
	bigs := []reflect.Value{
		v(uint8(1)),
		v(uint16(1)),
		v(uint32(1)),
		v(uint64(1)),
		v(^uint8(0)),
		v(^uint16(0)),
		v(^uint32(0)),
		v(^uint64(0)),
		v(int8(^uint8(0) >> 1)),
		v(int16(^uint16(0) >> 1)),
		v(int32(^uint32(0) >> 1)),
		v(int64(^uint64(0) >> 1)),
		v(float32(0.1)),
		v(float64(0.1)),
		v(math.MaxFloat32),
		v(math.MaxFloat64),
	}

	for _, s := range smalls {
		for _, b := range bigs {
			testComparator(t, s, b)
		}
	}
}

func TestArrayComparable(t *testing.T) {
	testComparator(t, v([2]int{0, 1}), v([2]int{0, 2}))
	testComparator(t, v([1]int{1}), v([2]int{0, 0}))
}

func TestSliceComparable(t *testing.T) {
	testComparator(t, v([]int{0, 1}), v([]int{0, 2}))
	testComparator(t, v([]int{1}), v([]int{0, 0}))
}

func TestMapComparable(t *testing.T) {
	testComparator(t, v(map[string]int{
		"a": 0,
		"b": 1,
	}), v(map[string]int{
		"a": 0,
		"b": 2,
	}))
	testComparator(t, v(map[string]int{
		"a": 0,
		"b": 1,
	}), v(map[string]int{
		"a": 0,
		"c": 1,
	}))
	m1 := v(map[string]int{"a": -1})
	m2 := v(map[uint]bool{0: true})
	if r.GetComparable(m1).LessThan(m2) {
		testComparator(t, m1, m2)
	} else {
		testComparator(t, m2, m1)
	}
}

type employee struct {
	ID   int
	Name string
}

func (e employee) HexID(prefix string) string {
	return fmt.Sprintf("%s%x", prefix, e.ID)
}

func TestStructComparable(t *testing.T) {
	testComparator(t, v(employee{1, "Alice"}), v(employee{2, "Bob"}))
}

func TestTimeComparable(t *testing.T) {
	oneHourEastOfUTC := time.FixedZone("UTC+1", 60*60)
	oneHourWestOfUTC := time.FixedZone("UTC-1", -60*60)
	lesser := time.Date(2018, 1, 1, 0, 0, 0, 0, oneHourEastOfUTC)
	greater := time.Date(2018, 1, 1, 0, 0, 0, 0, oneHourWestOfUTC)
	testComparator(t, v(lesser), v(greater))
}

func TestPropertyFunctor(t *testing.T) {
	a := v(employee{1, "Alice"})
	v, err := r.Property{
		PropertyName: "Name",
	}.F(a)
	assert.Nil(t, err)
	assert.Equal(t, "Alice", v.String())

	_, err = r.Property{
		PropertyName: "NotAnEmployeeProperty",
	}.F(a)
	assert.NotNil(t, err)
}

func TestMethodFunctor(t *testing.T) {
	a := v(employee{1, "Alice"})
	val, err := r.Method{
		MethodName: "HexID",
	}.F(a, v("0x"))
	assert.Nil(t, err)
	assert.Equal(t, "0x1", val.String())

	_, err = r.Method{
		MethodName: "HexID",
	}.F(a)
	assert.NotNil(t, err)

	_, err = r.Method{
		MethodName: "HexID",
	}.F(a, v(make(map[employee]employee)))
	assert.NotNil(t, err)

	_, err = r.Property{
		PropertyName: "NotAnEmployeeMethod",
	}.F(a)
	assert.NotNil(t, err)
}

func c(i interface{}) r.Constant {
	return r.Constant(v(i))
}

type predicateFunctor struct {
	VF func(r.ValueFunctor, r.ValueFunctor) r.ValueFunctor
	F  func(r.Comparable, reflect.Value) bool
}

var predicateFunctors = []predicateFunctor{
	predicateFunctor{
		r.EQ,
		func(c r.Comparable, v reflect.Value) bool { return c.EqualTo(v) },
	},
	predicateFunctor{
		r.NEQ,
		func(c r.Comparable, v reflect.Value) bool { return r.NotEqualTo(c, v) },
	},
	predicateFunctor{
		r.LT,
		func(c r.Comparable, v reflect.Value) bool { return c.LessThan(v) },
	},
	predicateFunctor{
		r.LTE,
		func(c r.Comparable, v reflect.Value) bool { return r.LessThanOrEqualTo(c, v) },
	},
	predicateFunctor{
		r.GT,
		func(c r.Comparable, v reflect.Value) bool { return r.GreaterThan(c, v) },
	},
	predicateFunctor{
		r.GTE,
		func(c r.Comparable, v reflect.Value) bool { return r.GreaterThanOrEqualTo(c, v) },
	},
}

func testComparisonFunctors(t *testing.T, a, b r.ValueFunctor, vs ...reflect.Value) {
	for _, pf := range predicateFunctors {
		val, err := pf.VF(a, b).F(vs...)
		assert.Nil(t, err)
		av, err := a.F(vs...)
		assert.Nil(t, err)
		bv, err := b.F(vs...)
		assert.Nil(t, err)
		assert.Equal(t, pf.F(r.GetComparable(av), bv), val.Bool())
	}
}

func TestComparisonFunctors(t *testing.T) {
	testComparisonFunctors(t, c("a"), c("b"))
	testComparisonFunctors(t, c("a"), c("a"))
	testComparisonFunctors(t, c(0), c("a"))
	testComparisonFunctors(t, c(struct{}{}), c(map[string]employee{}))
	testComparisonFunctors(t, c([]int{0, 1, 2}), c([1]employee{employee{}}))
}

func TestDistinct(t *testing.T) {
	d := r.DISTINCT(r.Property{
		PropertyName: "Name",
	})
	alice := employee{ID: 1, Name: "Alice"}
	bob := employee{ID: 2, Name: "Bob"}
	data := []employee{alice, bob, alice}
	result, err := d.F(v(data))
	assert.Nil(t, err)
	res, ok := result.Interface().([]employee)
	assert.True(t, ok)
	assert.Equal(t, 2, len(res))
	assert.Equal(t, alice, res[0])
	assert.Equal(t, bob, res[1])
}

func TestIndex(t *testing.T) {
	result, err := r.INDEX(c(2)).F(v([]string{"0", "1", "2"}))
	assert.Nil(t, err)
	res, ok := result.Interface().(string)
	assert.True(t, ok)
	assert.Equal(t, "2", res)

	_, err = r.INDEX(c(2)).F(v([]string{"0", "1"}))
	assert.NotNil(t, err)

	_, err = r.INDEX(c(-1)).F(v([]int{}))
	assert.NotNil(t, err)

	_, err = r.INDEX(c(struct{}{})).F(v([]int{}))
	assert.NotNil(t, err)

	_, err = r.INDEX(c(0)).F(v(struct{}{}))
	assert.NotNil(t, err)
}

func TestDesc(t *testing.T) {
	emps := []employee{
		employee{ID: 1, Name: "Charlie"},
		employee{ID: 2, Name: "Alice"},
		employee{ID: 3, Name: "Bob"},
	}
	v, err := r.FunctorSort(r.DESC(r.Property{
		PropertyName: "Name",
	}), v(emps))
	assert.Nil(t, err)
	emps, ok := v.Interface().([]employee)
	assert.True(t, ok)
	assert.Equal(t, []employee{
		employee{ID: 1, Name: "Charlie"},
		employee{ID: 3, Name: "Bob"},
		employee{ID: 2, Name: "Alice"},
	}, emps)
}

func testMarshalSymmetry(t *testing.T, data string, value interface{}) {
	err := json.Unmarshal([]byte(data), value)
	assert.Nil(t, err)
	result, err := json.Marshal(value)
	assert.Nil(t, err)
	assert.Equal(t, string(data), string(result))
}

func TestMarshalProperty(t *testing.T) {
	data := `{"property_name":"Name"}`
	var value r.MValueFunctor
	testMarshalSymmetry(t, data, &value)
	_, ok := value.ValueFunctor.(r.Property)
	assert.True(t, ok)
}

func TestMarshalMethod(t *testing.T) {
	data := `{"method_name":"HexID"}`
	var value r.MValueFunctor
	testMarshalSymmetry(t, data, &value)
	_, ok := value.ValueFunctor.(r.Method)
	assert.True(t, ok)
}

type marshalOp struct {
	OpName   string
	Exemplar interface{}
}

var marshalOps = []marshalOp{
	marshalOp{"eq", r.Eq{}},
	marshalOp{"neq", r.Neq{}},
	marshalOp{"lt", r.Lt{}},
	marshalOp{"lte", r.Lte{}},
	marshalOp{"gt", r.Gt{}},
	marshalOp{"gte", r.Gte{}},
	marshalOp{"and", r.And{}},
	marshalOp{"or", r.Or{}},
}

func TestMarshalBinary(t *testing.T) {
	for _, op := range marshalOps {
		data := `{"lhs":{"property_name":"IsSomething"},"op":"` + op.OpName + `","rhs":true}`
		var value r.MValueFunctor
		testMarshalSymmetry(t, data, &value)
		b, ok := value.ValueFunctor.(r.Binary)
		if ok {
			assert.True(t, ok)
		} else {
			assert.True(t, ok)
		}
		_, ok = b.LHS.(r.Property)
		assert.True(t, ok)
		_, ok = b.RHS.(r.Constant)
		assert.True(t, ok)
		assert.True(t, reflect.TypeOf(b.Op).ConvertibleTo(reflect.TypeOf(op.Exemplar)))
	}
}

func TestMarshalDistinct(t *testing.T) {
	data := `{"op":"distinct","arg":{"property_name":"IsSomething"}}`
	var value r.MValueFunctor
	testMarshalSymmetry(t, data, &value)
	ula, ok := value.ValueFunctor.(r.UnaryLazyArg)
	_, ok = ula.Op.(r.Distinct)
	assert.True(t, ok)
	_, ok = ula.Arg.(r.Property)
	assert.True(t, ok)
}
