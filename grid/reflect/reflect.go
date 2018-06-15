package reflect

import (
	"encoding/json"
	"errors"
	"go/types"
	"math/cmplx"
	"reflect"
	"sort"
)

type Value reflect.Value
type Chan types.Chan

type BasicComparable interface {
	EqualTo(o reflect.Value) bool
	LessThan(o reflect.Value) bool
}

type Comparable struct {
	BasicComparable
}

func (c Comparable) NotEqualTo(o reflect.Value) bool {
	return !c.EqualTo(o)
}

func (c Comparable) LessThanOrEqualTo(o reflect.Value) bool {
	return c.EqualTo(o) || c.LessThan(o)
}

func (c Comparable) GreaterThan(o reflect.Value) bool {
	return !c.LessThanOrEqualTo(o)
}

func (c Comparable) GreaterThanOrEqualTo(o reflect.Value) bool {
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
	return s[i].LessThan(s[j].Value)
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

func indirect(o reflect.Value) reflect.Value {
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

// TODO: This doesn't really work quite right.
func (c invalidValue) EqualTo(o reflect.Value) bool {
	return !o.IsValid()
}

// TODO: This doesn't really work quite right.
func (c invalidValue) LessThan(o reflect.Value) bool {
	return true
}

type String reflect.Value

func (c String) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.String {
		return reflect.Value(c).String() == v.String()
	}
	return false
}

func (c String) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.String {
		return reflect.Value(c).String() < v.String()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Bool Value

func (c Bool) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Bool {
		return reflect.Value(c).Bool() == v.Bool()
	}
	return false
}

func (c Bool) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Bool {
		return !reflect.Value(c).Bool() && v.Bool()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int Value

func (c Int) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int8 Value

func (c Int8) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int8 {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int8) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int8 {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int16 Value

func (c Int16) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int16 {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int16) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int16 {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int32 Value

func (c Int32) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int32 {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int32) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int32 {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Int64 Value

func (c Int64) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int64 {
		return reflect.Value(c).Int() == v.Int()
	}
	return false
}

func (c Int64) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Int64 {
		return reflect.Value(c).Int() < v.Int()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint Value

func (c Uint) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint8 Value

func (c Uint8) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint8 {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint8) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint8 {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint16 Value

func (c Uint16) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint16 {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint16) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint16 {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint32 Value

func (c Uint32) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint32 {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint32) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint32 {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uint64 Value

func (c Uint64) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint64 {
		return reflect.Value(c).Uint() == v.Uint()
	}
	return false
}

func (c Uint64) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uint64 {
		return reflect.Value(c).Uint() < v.Uint()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Uintptr Value

func (c Uintptr) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uintptr {
		return reflect.Value(c).Pointer() == v.Pointer()
	}
	return false
}

func (c Uintptr) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Uintptr {
		return reflect.Value(c).Pointer() < v.Pointer()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Float32 Value

func (c Float32) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Float32 {
		return reflect.Value(c).Float() == v.Float()
	}
	return false
}

func (c Float32) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Float32 {
		return reflect.Value(c).Float() < v.Float()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Float64 Value

func (c Float64) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Float64 {
		return reflect.Value(c).Float() == v.Float()
	}
	return false
}

func (c Float64) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Float64 {
		return reflect.Value(c).Float() < v.Float()
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Complex64 Value

func (c Complex64) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Complex64 {
		return reflect.Value(c).Complex() == v.Complex()
	}
	return false
}

func (c Complex64) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Complex64 {
		return cmplx.Abs(reflect.Value(c).Complex()) < cmplx.Abs(v.Complex())
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Complex128 Value

func (c Complex128) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Complex128 {
		return reflect.Value(c).Complex() == v.Complex()
	}
	return false
}

func (c Complex128) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if v.Type().Kind() == reflect.Complex128 {
		return cmplx.Abs(reflect.Value(c).Complex()) < cmplx.Abs(v.Complex())
	}
	return typeLessThan(reflect.TypeOf(c), v.Type())
}

type Array struct {
	v reflect.Value
}

func (c Array) EqualTo(o reflect.Value) bool {
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
		if !c1.EqualTo(v2.Index(i)) {
			return false
		}
	}

	return true
}

func (c Array) LessThan(o reflect.Value) bool {
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
		v2i := v2.Index(i)
		if c1.LessThan(v2i) {
			return true
		} else if c1.GreaterThan(v2i) {
			return false
		}
	}

	return false
}

type Slice struct {
	v reflect.Value
}

func (c Slice) EqualTo(o reflect.Value) bool {
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
		if !c1.EqualTo(v2.Index(i)) {
			return false
		}
	}

	return true
}

func (c Slice) LessThan(o reflect.Value) bool {
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
		v2i := v2.Index(i)
		if c1.LessThan(v2i) {
			return true
		} else if c1.GreaterThan(v2i) {
			return false
		}
	}

	return false
}

type Map reflect.Value

func (c Map) EqualTo(o reflect.Value) bool {
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
		if !c1.EqualTo(v2.MapIndex(k)) {
			return false
		}
	}

	return true
}

func (c Map) LessThan(o reflect.Value) bool {
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
		v2i := v2.MapIndex(k)
		if c1.LessThan(v2i) {
			return true
		} else if c1.GreaterThan(v2i) {
			return false
		}
	}

	return false
}

type Struct reflect.Value

func (c Struct) EqualTo(o reflect.Value) bool {
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
		if !c1.EqualTo(v2.FieldByName(f.Name)) {
			return false
		}
	}

	return true
}

func (c Struct) LessThan(o reflect.Value) bool {
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
		v2i := v2.FieldByName(f.Name)
		if c1.LessThan(v2i) {
			return true
		} else if c1.GreaterThan(v2i) {
			return false
		}
	}

	return false
}

func GetComparable(v reflect.Value) Comparable {
	k := indirect(v).Type().Kind()
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

type BasicFunctor interface {
	F(...interface{}) interface{}
}

type Functor struct {
	BasicFunctor
	*ValueFunctor
}

func (f Functor) F(args ...interface{}) interface{} {
	return f.BasicFunctor.F(args...)
}

func (f Functor) ToValueFunctor() ValueFunctor {
	if f.ValueFunctor == nil {
		f.ValueFunctor = &ValueFunctor{
			Functor: &f,
		}
	}
	return *f.ValueFunctor
}

type BasicValueFunctor interface {
	F(...reflect.Value) reflect.Value
}

type ValueFunctor struct {
	BasicValueFunctor
	*Functor
}

func (f ValueFunctor) F(args ...reflect.Value) reflect.Value {
	return f.BasicValueFunctor.F(args...)
}

func (f ValueFunctor) ToFunctor() Functor {
	if f.Functor == nil {
		f.Functor = &Functor{
			ValueFunctor: &f,
		}
	}
	return *f.Functor
}

type Constant reflect.Value

func (c Constant) F(args ...reflect.Value) reflect.Value {
	return reflect.Value(c)
}

type Property struct {
	PropertyName string
}

func (d Property) F(args ...reflect.Value) reflect.Value {
	return args[0].FieldByName(d.PropertyName)
}

type Method struct {
	MethodName string
}

func (m Method) F(args ...reflect.Value) reflect.Value {
	return args[0].MethodByName(m.MethodName).Call(args[1:])[0]
}

type Dot struct {
	First  Property
	Second Property
}

func (d Dot) F(args ...reflect.Value) reflect.Value {
	return d.Second.F(d.First.F(args[0]))
}

type ComparableValueFunctor struct {
	Name string
}

func (c ComparableValueFunctor) F(args ...reflect.Value) reflect.Value {
	return reflect.ValueOf(GetComparable(args[0])).MethodByName(c.Name).Call([]reflect.Value{args[1]})[0]
}

var eq = ValueFunctor{
	ComparableValueFunctor{"EqualTo"},
	nil,
}
var neq = ValueFunctor{
	ComparableValueFunctor{"NotEqualTo"},
	nil,
}
var lt = ValueFunctor{
	ComparableValueFunctor{"LessThan"},
	nil,
}
var lte = ValueFunctor{
	ComparableValueFunctor{"LessThanOrEqualTo"},
	nil,
}
var gt = ValueFunctor{
	ComparableValueFunctor{"GreaterThan"},
	nil,
}
var gte = ValueFunctor{
	ComparableValueFunctor{"GreaterThanOrEqualTo"},
	nil,
}

type Binary struct {
	LHS ValueFunctor
	Op  ValueFunctor
	RHS ValueFunctor
}

func (b Binary) F(args ...reflect.Value) reflect.Value {
	return b.Op.F(b.LHS.F(args...), b.RHS.F(args...))
}

func EQ(lhs, rhs ValueFunctor) ValueFunctor {
	return ValueFunctor{
		Binary{
			LHS: lhs,
			Op:  eq,
			RHS: rhs,
		},
		nil,
	}
}

func NEQ(lhs, rhs ValueFunctor) ValueFunctor {
	return ValueFunctor{
		Binary{
			LHS: lhs,
			Op:  neq,
			RHS: rhs,
		},
		nil,
	}
}

func LT(lhs, rhs ValueFunctor) ValueFunctor {
	return ValueFunctor{
		Binary{
			LHS: lhs,
			Op:  lt,
			RHS: rhs,
		},
		nil,
	}
}

func LTE(lhs, rhs ValueFunctor) ValueFunctor {
	return ValueFunctor{
		Binary{
			LHS: lhs,
			Op:  lte,
			RHS: rhs,
		},
		nil,
	}
}

func GT(lhs, rhs ValueFunctor) ValueFunctor {
	return ValueFunctor{
		Binary{
			LHS: lhs,
			Op:  gt,
			RHS: rhs,
		},
		nil,
	}
}

func GTE(lhs, rhs ValueFunctor) ValueFunctor {
	return ValueFunctor{
		Binary{
			LHS: lhs,
			Op:  gte,
			RHS: rhs,
		},
		nil,
	}
}

type AndOp struct{}

func (AndOp) F(args ...reflect.Value) reflect.Value {
	return reflect.ValueOf(args[0].Bool() && args[1].Bool())
}

type OrOp struct{}

func (OrOp) F(args ...reflect.Value) reflect.Value {
	return reflect.ValueOf(args[0].Bool() || args[1].Bool())
}

var and = ValueFunctor{
	AndOp{},
	nil,
}
var or = ValueFunctor{
	OrOp{},
	nil,
}

func AND(lhs, rhs ValueFunctor) ValueFunctor {
	return ValueFunctor{
		Binary{
			LHS: lhs,
			Op:  and,
			RHS: rhs,
		},
		nil,
	}
}

func OR(lhs, rhs ValueFunctor) ValueFunctor {
	return ValueFunctor{
		Binary{
			LHS: lhs,
			Op:  or,
			RHS: rhs,
		},
		nil,
	}
}

type Distinct struct {
	Arg ValueFunctor
}

func (d Distinct) F(args ...reflect.Value) reflect.Value {
	seen := make(map[interface{}]bool)
	results := reflect.MakeSlice(reflect.TypeOf(args[0]), 0, 0)

	for _, v := range args {
		k := d.Arg.F(v).Interface()
		if _, ok := seen[k]; !ok {
			seen[k] = true
			results = reflect.Append(results, v)
		}
	}

	return results
}

func DISTINCT(arg ValueFunctor) ValueFunctor {
	return ValueFunctor{
		Distinct{arg},
		nil,
	}
}

type BasicJSONSelector interface {
	Select([]byte) *interface{}
}

type JSONSelector struct {
	BasicJSONSelector
}

func (s JSONSelector) Unmarshal(data []byte) (*interface{}, error) {
	ptr := s.Select(data)
	if ptr == nil {
		return ptr, errors.New("Failed to select type for JSON")
	}

	if err := json.Unmarshal(data, ptr); err != nil {
		return nil, err
	}

	return ptr, nil
}

type FieldExistsJSONSelector struct {
	Ptr   *interface{}
	Field string
}

func (s FieldExistsJSONSelector) Select(data []byte) *interface{} {
	if err := json.Unmarshal(data, s.Ptr); err != nil {
		return nil
	}

	f := reflect.ValueOf(*s.Ptr).FieldByName(s.Field)
	z := reflect.Zero(f.Type()).Interface()
	v := f.Interface()
	if v == z {
		return nil
	}
	var copied interface{}
	copied = v
	return &copied
}

type StringFieldValueJSONSelector struct {
	Map map[string]interface{}

	FieldExistsJSONSelector
}

func (s StringFieldValueJSONSelector) Select(data []byte) *interface{} {
	if err := json.Unmarshal(data, s.Ptr); err != nil {
		return nil
	}

	k := (reflect.ValueOf(*s.Ptr).FieldByName(s.Field).Interface()).(string)
	v, ok := s.Map[k]
	if !ok {
		return nil
	}
	var copied interface{}
	copied = v
	return &copied
}

var opJSON interface{} = OpJSON{}
var opSelector = StringFieldValueJSONSelector{
	FieldExistsJSONSelector: FieldExistsJSONSelector{
		Ptr:   &opJSON,
		Field: "Op",
	},
	Map: map[string]interface{}{
		"eq":       eq,
		"neq":      neq,
		"lt":       lt,
		"lte":      lte,
		"gt":       gt,
		"gte":      gte,
		"and":      and,
		"or":       or,
		"distinct": ValueFunctor{Distinct{}, nil},
	},
}

type OpJSON struct {
	Op   string
	Rest json.RawMessage
}

/*
type BinaryJSON struct {
	LHS JSON
	RHS JSON
}

var ErrUnclassedValue = errors.New("Unclassed value")

var CanonicalBasicValueFunctor BasicValueFunctor
var BasicValueFunctorType = reflect.TypeOf(CanonicalBasicValueFunctor)
var CanonicalBasicFunctor BasicValueFunctor
var BasicFunctorType = reflect.TypeOf(CanonicalBasicValueFunctor)

func wrapFunctor(f interface{}) interface{} {
	var ret interface{}
	if reflect.TypeOf(f).ConvertibleTo(BasicValueFunctorType) {
		bvf := (f).(BasicValueFunctor)
		ret = ValueFunctor{
			bvf,
			nil,
		}
	} else if reflect.TypeOf(f).ConvertibleTo(BasicFunctorType) {
		bf := (f).(BasicFunctor)
		ret = Functor{
			bf,
			nil,
		}
	} else {
		ret = f
	}

	return ret
}

func (l *JSONClassLoader) UnmarshalJSON(data []byte) error {
	var v JSON
	var ptr *interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		var copied interface{}
		copied = *l.Default
		ptr = &copied
	} else {
		ptr := l.Lookup(v.Class)
		if ptr == nil {
			var copied interface{}
			copied = *l.Default
			ptr = &copied
		}
	}

	if err := json.Unmarshal(v.Rest, ptr); err != nil {
		return err
	}

	l.Value = l.Convert(*ptr)
	return nil
}

func (f *ValueFunctor) UnmarshalJSON(data []byte) error {
	var binOp Binary
	if err := json.Unmarshal(bytes, &binOp); err != nil {
		//...
	}
}

func (f *Binary) UnmarshalJSON(data []byte) error {
	var opJSON FunctorJSON
	if err := json.Unmarshal(data, &opJSON); err != nil {
		return err
	}
	return nil
}
*/
