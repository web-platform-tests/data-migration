package reflect

import (
	"go/types"
	"math/cmplx"
	"reflect"
	"sort"
)

type Value reflect.Value
type Chan types.Chan

type BasicComparable interface {
	EqualTo(o interface{}) bool
	LessThan(o interface{}) bool
}

type Comparable struct {
	BasicComparable
}

func (c Comparable) NotEqualTo(o interface{}) bool {
	return !c.EqualTo(o)
}

func (c Comparable) LessThanOrEqualTo(o interface{}) bool {
	return c.EqualTo(o) || c.LessThan(o)
}

func (c Comparable) GreaterThan(o interface{}) bool {
	return !c.LessThanOrEqualTo(o)
}

func (c Comparable) GreaterThanOrEqualTo(o interface{}) bool {
	return !c.LessThan(o)
}

type ComparableValue struct {
	Comparable
	reflect.Value
}

type ByComparison []ComparableValue

func (s ByComparison) Len() int {
	return len(s)
}

func (s ByComparison) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByComparison) Less(i, j int) bool {
	return s[i].LessThan(s[j])
}

type FieldsByName []reflect.StructField

func (s FieldsByName) Len() int {
	return len(s)
}

func (s FieldsByName) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s FieldsByName) Less(i, j int) bool {
	return s[i].Name < s[j].Name
}

func indirect(o interface{}) reflect.Value {
	v := reflect.ValueOf(o)
	for v.Type().Kind() == reflect.Interface || v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v
}

func typeLessThan(t1, t2 reflect.Type) bool {
	return t1.PkgPath()+"/"+t1.Name() < t2.PkgPath()+"/"+t2.Name()
}

type invalidValue struct{}

var invalid = invalidValue{}

func (c invalidValue) EqualTo(o interface{}) bool {
	return c == o
}

func (c invalidValue) LessThan(o interface{}) bool {
	return c != o
}

type String reflect.Value

func (c String) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.String {
		return reflect.Value(c).String() == v.String()
	}
	return false
}

func (c String) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.String {
		return reflect.Value(c).String() < v.String()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Bool Value

func (c Bool) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Bool {
		return reflect.Value(c).Bool() == v.Bool()
	}
	return false
}

func (c Bool) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Bool {
		return !reflect.Value(c).Bool() && v.Bool()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int Value

func (c Int) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int8 Value

func (c Int8) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int8 {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int8) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int8 {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int16 Value

func (c Int16) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int16 {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int16) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int16 {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int32 Value

func (c Int32) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int32 {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int32) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int32 {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int64 Value

func (c Int64) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int64 {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int64) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int64 {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint Value

func (c Uint) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint8 Value

func (c Uint8) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint8 {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint8) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint8 {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint16 Value

func (c Uint16) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint16 {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint16) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint16 {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint32 Value

func (c Uint32) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint32 {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint32) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint32 {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint64 Value

func (c Uint64) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint64 {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint64) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint64 {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uintptr Value

func (c Uintptr) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uintptr {
		return reflect.Value(c).Pointer() == v.Pointer()
	}
	return false
}

func (c Uintptr) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uintptr {
		return reflect.Value(c).Pointer() < v.Pointer()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Float32 Value

func (c Float32) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Float32 {
		return reflect.Value(c).Float() == v.Float()
	}
	return false
}

func (c Float32) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Float32 {
		return reflect.Value(c).Float() < v.Float()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Float64 Value

func (c Float64) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Float64 {
		return reflect.Value(c).Float() == v.Float()
	}
	return false
}

func (c Float64) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Float64 {
		return reflect.Value(c).Float() < v.Float()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Complex64 Value

func (c Complex64) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Complex64 {
		return reflect.Value(c).Complex() == v.Complex()
	}
	return false
}

func (c Complex64) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Complex64 {
		return cmplx.Abs(reflect.Value(c).Complex()) < cmplx.Abs(v.Complex())
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Complex128 Value

func (c Complex128) EqualTo(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Complex128 {
		return reflect.Value(c).Complex() == v.Complex()
	}
	return false
}

func (c Complex128) LessThan(o interface{}) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Complex128 {
		return cmplx.Abs(reflect.Value(c).Complex()) < cmplx.Abs(v.Complex())
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Array struct {
	v interface{}
}

func (c Array) EqualTo(o interface{}) bool {
	v1 := indirect(c.v)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Array {
		return false
	}

	if v1.Len() != v2.Len() {
		return false
	}

	for i := 0; i < v1.Len(); i++ {
		c1 := GetComparable(v1.Index(i))
		c2 := GetComparable(v2.Index(i))
		if !c1.EqualTo(c2) {
			return false
		}
	}

	return true
}

func (c Array) LessThan(o interface{}) bool {
	v1 := indirect(c.v)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Array {
		return typeLessThan(v1.Type(), v2.Type())
	}

	if v1.Len() < v2.Len() {
		return true
	} else if v1.Len() > v2.Len() {
		return false
	}

	for i := 0; i < v1.Len(); i++ {
		c1 := GetComparable(v1.Index(i))
		c2 := GetComparable(v2.Index(i))
		if c1.LessThan(c2) {
			return true
		} else if c1.GreaterThan(c2) {
			return false
		}
	}

	return false
}

type Slice struct {
	v interface{}
}

func (c Slice) EqualTo(o interface{}) bool {
	v1 := indirect(c.v)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Slice {
		return false
	}
	if v1.Len() != v2.Len() {
		return false
	}

	for i := 0; i < v1.Len(); i++ {
		c1 := GetComparable(v1.Index(i))
		c2 := GetComparable(v2.Index(i))
		if !c1.EqualTo(c2) {
			return false
		}
	}

	return true
}

func (c Slice) LessThan(o interface{}) bool {
	v1 := indirect(c.v)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Slice {
		return typeLessThan(v1.Type(), v2.Type())
	}

	if v1.Len() < v2.Len() {
		return true
	} else if v1.Len() > v2.Len() {
		return false
	}

	for i := 0; i < v1.Len(); i++ {
		c1 := GetComparable(v1.Index(i))
		c2 := GetComparable(v2.Index(i))
		if c1.LessThan(c2) {
			return true
		} else if c1.GreaterThan(c2) {
			return false
		}
	}

	return false
}

type Map reflect.Value

func (c Map) EqualTo(o interface{}) bool {
	v1 := reflect.Value(c)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Map {
		return false
	}
	if v1.Len() != v2.Len() {
		return false
	}

	for _, k := range v1.MapKeys() {
		c1 := GetComparable(v1.MapIndex(k))
		c2 := GetComparable(v2.MapIndex(k))
		if !c1.EqualTo(c2) {
			return false
		}
	}

	return true
}

func (c Map) LessThan(o interface{}) bool {
	v1 := reflect.Value(c)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Map {
		return typeLessThan(v1.Type(), v2.Type())
	}

	if v1.Len() < v2.Len() {
		return true
	} else if v1.Len() > v2.Len() {
		return false
	}

	ks := v1.MapKeys()
	cs := make([]ComparableValue, 0, len(ks))
	for _, k := range ks {
		cs = append(cs, ComparableValue{
			GetComparable(k),
			k,
		})
	}
	sort.Sort(ByComparison(cs))
	for i, c := range cs {
		ks[i] = c.Value
	}

	for _, k := range v1.MapKeys() {
		c1 := GetComparable(v1.MapIndex(k))
		c2 := GetComparable(v2.MapIndex(k))
		if c1.LessThan(c2) {
			return true
		} else if c1.GreaterThan(c2) {
			return false
		}
	}

	return false
}

type Struct reflect.Value

func (c Struct) EqualTo(o interface{}) bool {
	v1 := reflect.Value(c)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Struct {
		return false
	}

	t1 := v1.Type()
	t2 := v2.Type()
	if t1.NumField() != t2.NumField() {
		return false
	}

	for i := 0; i < t1.NumField(); i++ {
		f := t1.Field(i)

		c1 := GetComparable(v1.FieldByName(f.Name))
		c2 := GetComparable(v2.FieldByName(f.Name))
		if !c1.EqualTo(c2) {
			return false
		}
	}

	return true
}

func (c Struct) LessThan(o interface{}) bool {
	v1 := reflect.Value(c)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Struct {
		return typeLessThan(v1.Type(), v2.Type())
	}

	t1 := v1.Type()
	t2 := v2.Type()
	if t1.NumField() < t2.NumField() {
		return true
	} else if t1.NumField() > t2.NumField() {
		return false
	}

	fields := make([]reflect.StructField, t1.NumField(), t1.NumField())
	for i := 0; i < t1.NumField(); i++ {
		fields[i] = t1.Field(i)
	}
	sort.Sort(FieldsByName(fields))

	for _, f := range fields {
		c1 := GetComparable(v1.FieldByName(f.Name))
		c2 := GetComparable(v2.FieldByName(f.Name))
		if c1.LessThan(c2) {
			return true
		} else if c1.GreaterThan(c2) {
			return false
		}
	}

	return false
}

func GetComparable(v reflect.Value) Comparable {
	k := indirect(v.Type()).Kind()
	switch {
	case k == reflect.Int:
		return Comparable{Int(v)}
	case k == reflect.Int8:
		return Comparable{Int8(v)}
	case k == reflect.Int16:
		return Comparable{Int16(v)}
	case k == reflect.Int32:
		return Comparable{Int32(v)}
	case k == reflect.Int64:
		return Comparable{Int64(v)}
	case k == reflect.Uint:
		return Comparable{Uint(v)}
	case k == reflect.Uint8:
		return Comparable{Uint8(v)}
	case k == reflect.Uint16:
		return Comparable{Uint16(v)}
	case k == reflect.Uint32:
		return Comparable{Uint32(v)}
	case k == reflect.Uint64:
		return Comparable{Uint64(v)}
	case k == reflect.Uintptr:
		return Comparable{Uintptr(v)}
	case k == reflect.Float32:
		return Comparable{Float32(v)}
	case k == reflect.Float64:
		return Comparable{Float64(v)}
	case k == reflect.Complex64:
		return Comparable{Complex64(v)}
	case k == reflect.Complex128:
		return Comparable{Complex128(v)}
	case k == reflect.Array:
		return Comparable{Array{v}}
	case k == reflect.Slice:
		return Comparable{Slice{v}}
	case k == reflect.Map:
		return Comparable{Map(v)}
	case k == reflect.Struct:
		return Comparable{Struct(v)}
	default:
		return Comparable{invalid}
	}
}

type Executable interface {
	Execute(...interface{}) interface{}
}
