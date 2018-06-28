package reflect

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/types"
	"math/cmplx"
	"reflect"
	"sort"
	"time"
)

type Value reflect.Value
type Chan types.Chan

type Comparable interface {
	EqualTo(o reflect.Value) bool
	LessThan(o reflect.Value) bool
}

func NotEqualTo(c Comparable, o reflect.Value) bool {
	return !c.EqualTo(o)
}

func LessThanOrEqualTo(c Comparable, o reflect.Value) bool {
	return c.EqualTo(o) || c.LessThan(o)
}

func GreaterThan(c Comparable, o reflect.Value) bool {
	return !LessThanOrEqualTo(c, o)
}

func GreaterThanOrEqualTo(c Comparable, o reflect.Value) bool {
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

func indirect(v reflect.Value) reflect.Value {
	for v.Type().Kind() == reflect.Interface || v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v
}

func typeLessThan(v reflect.Value, t2 reflect.Type) bool {
	t1 := reflect.TypeOf(v.Interface())
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
	return typeLessThan(reflect.Value(c), v.Type())
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
	return typeLessThan(reflect.Value(c), v.Type())
}

type Int Value

func isInt(k reflect.Kind) bool {
	return k == reflect.Int8 || k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64 || k == reflect.Int
}

func (c Int) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	k := v.Type().Kind()
	if isInt(k) {
		return reflect.Value(c).Int() == v.Int()
	} else if isUint(k) {
		u := v.Uint()
		if u == u<<1>>1 {
			return reflect.Value(c).Int() == int64(u)
		}
		return false
	} else if isFloat(k) {
		return float64(reflect.Value(c).Int()) == v.Float()
	}
	return false
}

func (c Int) LessThan(o reflect.Value) bool {
	v := indirect(o)
	k := v.Type().Kind()
	if isInt(k) {
		return reflect.Value(c).Int() < v.Int()
	} else if isUint(k) {
		u := v.Uint()
		if u == u<<1>>1 {
			return reflect.Value(c).Int() < int64(u)
		}
		return true
	} else if isFloat(k) {
		return float64(reflect.Value(c).Int()) < v.Float()
	}
	return typeLessThan(reflect.Value(c), v.Type())
}

type Uint Value

func isUint(k reflect.Kind) bool {
	return k == reflect.Uint8 || k == reflect.Uint16 || k == reflect.Uint32 || k == reflect.Uint64 || k == reflect.Uint
}

func (c Uint) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	k := v.Type().Kind()
	if isUint(k) {
		return reflect.Value(c).Uint() == v.Uint()
	} else if isInt(k) {
		i := v.Int()
		if i >= 0 {
			return reflect.Value(c).Uint() == uint64(i)
		}
		return false
	} else if isFloat(k) {
		return float64(reflect.Value(c).Uint()) == v.Float()
	}
	return false
}

func (c Uint) LessThan(o reflect.Value) bool {
	v := indirect(o)
	k := v.Type().Kind()
	if isUint(k) {
		return reflect.Value(c).Uint() < v.Uint()
	} else if isInt(k) {
		i := v.Int()
		if i >= 0 {
			return reflect.Value(c).Uint() < uint64(i)
		}
		return false
	} else if isFloat(k) {
		return float64(reflect.Value(c).Uint()) < v.Float()
	}
	return typeLessThan(reflect.Value(c), v.Type())
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
	return typeLessThan(reflect.Value(c), v.Type())
}

type Float Value

func isFloat(k reflect.Kind) bool {
	return k == reflect.Float32 || k == reflect.Float64
}

func (c Float) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	k := v.Type().Kind()
	if isFloat(k) {
		return reflect.Value(c).Float() == v.Float()
	} else if isUint(k) {
		return reflect.Value(c).Float() == float64(v.Uint())
	} else if isInt(k) {
		return reflect.Value(c).Float() == float64(v.Int())
	}
	return false
}

func (c Float) LessThan(o reflect.Value) bool {
	v := indirect(o)
	k := v.Type().Kind()
	if isFloat(k) {
		return reflect.Value(c).Float() < v.Float()
	} else if isUint(k) {
		return reflect.Value(c).Float() < float64(v.Uint())
	} else if isInt(k) {
		return reflect.Value(c).Float() < float64(v.Int())
	}
	return typeLessThan(reflect.Value(c), v.Type())
}

type Complex Value

func isComplex(k reflect.Kind) bool {
	return k == reflect.Complex64 || k == reflect.Complex128
}

func (c Complex) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	if isComplex(v.Type().Kind()) {
		return reflect.Value(c).Complex() == v.Complex()
	}
	return false
}

func (c Complex) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if isComplex(v.Type().Kind()) {
		return cmplx.Abs(reflect.Value(c).Complex()) < cmplx.Abs(v.Complex())
	}
	return typeLessThan(reflect.Value(c), v.Type())
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
		return typeLessThan(v1, v2.Type())
	}

	if v1.Len() < v2.Len() {
		return true
	} else if v1.Len() > v2.Len() {
		return false
	}

	for i := 0; i < v1.Len(); i++ {
		c1 := GetComparable(v1.Index(i))
		v2i := v2.Index(i)
		if !c1.EqualTo(v2i) {
			return c1.LessThan(v2i)
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
		return typeLessThan(v1, v2.Type())
	}

	if v1.Len() < v2.Len() {
		return true
	} else if v1.Len() > v2.Len() {
		return false
	}

	for i := 0; i < v1.Len(); i++ {
		c1 := GetComparable(v1.Index(i))
		v2i := v2.Index(i)
		if !c1.EqualTo(v2i) {
			return c1.LessThan(v2i)
		}
	}

	return false
}

type Map reflect.Value

func sortedMapKeys(m reflect.Value) []ComparableValue {
	cks := make([]ComparableValue, 0, len(m.MapKeys()))
	for _, k := range m.MapKeys() {
		cks = append(cks, ComparableValue{
			Comparable: GetComparable(k),
			Value:      k,
		})
	}
	sort.Sort(ByComparison(cks))
	return cks
}

func (c Map) EqualTo(o reflect.Value) bool {
	v1 := reflect.Value(c)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Map {
		return false
	}
	if v1.Len() != v2.Len() {
		return false
	}

	cks1 := sortedMapKeys(v1)
	cks2 := sortedMapKeys(v2)
	for i := 0; i < len(cks1); i++ {
		if !cks1[i].Comparable.EqualTo(cks2[i].Value) {
			return false
		}
	}
	for _, ck := range cks1 {
		mv1 := v1.MapIndex(ck.Value)
		mv2 := v2.MapIndex(ck.Value)
		if !GetComparable(mv1).EqualTo(mv2) {
			return false
		}
	}

	return true
}

func (c Map) LessThan(o reflect.Value) bool {
	v1 := reflect.Value(c)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Map {
		return typeLessThan(v1, v2.Type())
	}

	if v1.Len() < v2.Len() {
		return true
	} else if v1.Len() > v2.Len() {
		return false
	}

	cks1 := sortedMapKeys(v1)
	cks2 := sortedMapKeys(v2)
	for i := 0; i < len(cks1); i++ {
		c := cks1[i].Comparable
		if !c.EqualTo(cks2[i].Value) {
			return c.LessThan(cks2[i].Value)
		}
	}
	for _, ck := range cks1 {
		mv1 := v1.MapIndex(ck.Value)
		mv2 := v2.MapIndex(ck.Value)
		c := GetComparable(mv1)
		if !c.EqualTo(mv2) {
			return c.LessThan(mv2)
		}
	}

	return false
}

type Struct reflect.Value

type Lexicographical []string

func (s Lexicographical) Len() int {
	return len(s)
}

func (s Lexicographical) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Lexicographical) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func sortedFieldNames(t reflect.Type) []string {
	names := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		names = append(names, t.Field(i).Name)
	}
	sort.Sort(Lexicographical(names))
	return names
}

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

	f1 := sortedFieldNames(t1)
	f2 := sortedFieldNames(t2)
	for i := range f1 {
		if f1[i] != f2[i] {
			return false
		}
	}

	for _, name := range f1 {
		c1 := GetComparable(v1.FieldByName(name))
		if !c1.EqualTo(v2.FieldByName(name)) {
			return false
		}
	}

	return true
}

func (c Struct) LessThan(o reflect.Value) bool {
	v1 := reflect.Value(c)
	v2 := indirect(o)

	if v2.Type().Kind() != reflect.Struct {
		return typeLessThan(v1, v2.Type())
	}

	t1 := v1.Type()
	t2 := v2.Type()
	if t1.NumField() < t2.NumField() {
		return true
	} else if t1.NumField() > t2.NumField() {
		return false
	}

	f1 := sortedFieldNames(t1)
	f2 := sortedFieldNames(t2)
	for i := range f1 {
		if f1[i] != f2[i] {
			return f1[i] < f2[i]
		}
	}

	for _, name := range f1 {
		c1 := GetComparable(v1.FieldByName(name))
		f2 := v2.FieldByName(name)
		if !c1.EqualTo(f2) {
			return c1.LessThan(f2)
		}
	}

	return false
}

type Time reflect.Value

func isTime(v reflect.Value) bool {
	ot := v.Type()
	return ot.PkgPath() == "time" && ot.Name() == "Time"
}

func (t Time) LessThan(o reflect.Value) bool {
	v := indirect(o)
	if isTime(v) {
		rv := reflect.Value(t).MethodByName("Before").Call([]reflect.Value{o})[0]
		return rv.Bool()
	}
	return typeLessThan(reflect.Value(t), v.Type())
}

func (t Time) EqualTo(o reflect.Value) bool {
	v := indirect(o)
	ot := v.Type()
	if ot.PkgPath() == "time" || ot.Name() == "Time" {
		rv := reflect.Value(t).MethodByName("Equal").Call([]reflect.Value{o})[0]
		return rv.Bool()
	}
	return false
}

func GetComparable(v reflect.Value) Comparable {
	k := indirect(v).Type().Kind()
	switch {
	case k == reflect.Bool:
		return Bool(v)
	case isInt(k):
		return Int(v)
	case isUint(k):
		return Uint(v)
	case k == reflect.Uintptr:
		return Uintptr(v)
	case isFloat(k):
		return Float(v)
	case isComplex(k):
		return Complex(v)
	case k == reflect.Array:
		return Array{v}
	case k == reflect.Slice:
		return Slice{v}
	case k == reflect.Map:
		return Map(v)
	case k == reflect.String:
		return String(v)
	case k == reflect.Struct:
		if isTime(v) {
			return Time(v)
		} else {
			return Struct(v)
		}
	default:
		return invalid
	}
}

func MethodByName(v reflect.Value, name string) (reflect.Value, error) {
	t := indirect(v).Type()
	if _, ok := t.MethodByName(name); !ok {
		return v, fmt.Errorf("Failed to find method, %s, for %v of indirected type %v", name, v, t)
	}
	return v.MethodByName(name), nil
}

func FieldByName(v reflect.Value, name string) (reflect.Value, error) {
	t := indirect(v).Type()
	if _, ok := t.FieldByName(name); !ok {
		return v, fmt.Errorf("Failed to find field, %s, for %v of indirected type %v", name, v, t)
	}
	return v.FieldByName(name), nil
}

type Functor interface {
	F(...interface{}) (interface{}, error)
}

func F(f Functor, args ...interface{}) (interface{}, error) {
	return f.F(args...)
}

type ValueFunctor interface {
	F(...reflect.Value) (reflect.Value, error)
}

type ConvertedValueFunctor struct {
	Functor
}

func (cvf ConvertedValueFunctor) F(vs ...reflect.Value) (reflect.Value, error) {
	args := make([]interface{}, 0, len(vs))
	for _, v := range vs {
		args = append(args, v.Interface())
	}
	i, err := cvf.Functor.F(args...)
	return reflect.ValueOf(i), err
}

type ConvertedFunctor struct {
	ValueFunctor
}

func (cf ConvertedFunctor) F(is ...interface{}) (interface{}, error) {
	args := make([]reflect.Value, 0, len(is))
	for _, i := range is {
		args = append(args, reflect.ValueOf(i))
	}
	v, err := cf.ValueFunctor.F(args...)
	return v.Interface(), err
}

func ToValueFunctor(f Functor) ValueFunctor {
	cf, ok := f.(ConvertedFunctor)
	if ok {
		return cf.ValueFunctor
	}

	return ConvertedValueFunctor{
		Functor: f,
	}
}

func ToFunctor(vf ValueFunctor) Functor {
	cvf, ok := vf.(ConvertedValueFunctor)
	if ok {
		return cvf.Functor
	}

	return ConvertedFunctor{
		ValueFunctor: vf,
	}
}

func VF(vf ValueFunctor, args ...reflect.Value) (reflect.Value, error) {
	return vf.F(args...)
}

type Desc struct {
	ValueFunctor
}

func DESC(vf ValueFunctor) ValueFunctor {
	if d, ok := vf.(Desc); ok {
		return d.ValueFunctor
	} else {
		return Desc{vf}
	}
}

func FunctorSort(vf ValueFunctor, s reflect.Value) (reflect.Value, error) {
	st := s.Type()
	if st.Kind() != reflect.Slice {
		stn := st.PkgPath() + "/" + st.Name()
		return s, fmt.Errorf("Expected second argument of FunctorSort to be slice value, but got %s value", stn)
	}

	if _, ok := vf.(Desc); ok {
		sort.Sort(sort.Reverse(ByFunctor{
			Values:       s,
			ValueFunctor: vf,
		}))
	} else {
		sort.Sort(ByFunctor{
			Values:       s,
			ValueFunctor: vf,
		})
	}
	return s, nil
}

type ByFunctor struct {
	Values reflect.Value
	ValueFunctor
}

func (bf ByFunctor) Len() int {
	return bf.Values.Len()
}

func (bf ByFunctor) Swap(i, j int) {
	// Get copy of value by making it concrete, and taking value again.
	vi := reflect.ValueOf(bf.Values.Index(i).Interface())

	bf.Values.Index(i).Set(bf.Values.Index(j))
	bf.Values.Index(j).Set(vi)
}

func (bf ByFunctor) Less(i, j int) bool {
	v1, _ := bf.ValueFunctor.F(bf.Values.Index(i))
	v2, _ := bf.ValueFunctor.F(bf.Values.Index(j))
	c := GetComparable(v1)
	return c.LessThan(v2)
}

type Constant reflect.Value

func (c Constant) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.Value(c), nil
}

type Identity struct{}

func (Identity) F(args ...reflect.Value) (reflect.Value, error) {
	return args[0], nil
}

var TRUE = Constant(reflect.ValueOf(true))
var FALSE = Constant(reflect.ValueOf(true))
var ZERO = Constant(reflect.ValueOf(0))
var INT_MAX = Constant(reflect.ValueOf(int(^uint(0) >> 1)))
var UINT_MAX = Constant(reflect.ValueOf(^uint(0)))
var IDENTITY = Identity{}

type Property struct {
	PropertyName string `json:"property_name"`
}

func (p Property) F(args ...reflect.Value) (reflect.Value, error) {
	return FieldByName(args[0], p.PropertyName)
}

type Method struct {
	MethodName string `json:"method_name"`
}

func (m Method) F(args ...reflect.Value) (reflect.Value, error) {
	method, err := MethodByName(args[0], m.MethodName)
	if err != nil {
		return method, err
	}

	mArgs := args[1:]
	mt := method.Type()
	if len(mArgs) != mt.NumIn() && !mt.IsVariadic() {
		return args[0], fmt.Errorf("Method %s expected %d arguments but got %d", m.MethodName, mt.NumIn(), len(mArgs))
	}

	for i, a := range mArgs {
		at := a.Type()
		mat := mt.In(i)
		if !at.ConvertibleTo(mat) {
			return a, fmt.Errorf("Method %s expected %dth arguments be convertible to %s but got %s", m.MethodName, i, mat.PkgPath()+"/"+mat.Name(), at.PkgPath()+"/"+at.Name())
		}
	}

	return method.Call(mArgs)[0], nil
}

type Dot struct {
	First  Property `json:"first"`
	Second Property `json:"second"`
}

func (d Dot) F(args ...reflect.Value) (reflect.Value, error) {
	fst, err := d.First.F(args[0])
	if err != nil {
		return fst, err
	}
	return d.Second.F(fst)
}

type Eq struct{}

func (c Eq) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(GetComparable(args[0]).EqualTo(args[1])), nil
}

type Lt struct{}

func (c Lt) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(GetComparable(args[0]).LessThan(args[1])), nil
}

type Neq struct{}

func (c Neq) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(NotEqualTo(GetComparable(args[0]), args[1])), nil
}

type Lte struct{}

func (c Lte) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(LessThanOrEqualTo(GetComparable(args[0]), args[1])), nil
}

type Gt struct{}

func (c Gt) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(GreaterThan(GetComparable(args[0]), args[1])), nil
}

type Gte struct{}

func (c Gte) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(GreaterThanOrEqualTo(GetComparable(args[0]), args[1])), nil
}

type Unary struct {
	Op  ValueFunctor `json:"op"`
	Arg ValueFunctor `json:"arg"`
}

type UnaryLazyArg Unary

func (c UnaryLazyArg) F(args ...reflect.Value) (reflect.Value, error) {
	allArgs := make([]reflect.Value, 0, len(args)+1)
	allArgs = append(allArgs, reflect.ValueOf(c.Arg))
	for _, a := range args {
		allArgs = append(allArgs, a)
	}
	return c.Op.F(allArgs...)
}

type Binary struct {
	LHS ValueFunctor `json:"lhs"`
	Op  ValueFunctor `json:"op"`
	RHS ValueFunctor `json:"rhs"`
}

func (b Binary) F(args ...reflect.Value) (reflect.Value, error) {
	l, err := b.LHS.F(args...)
	if err != nil {
		return l, err
	}
	r, err := b.RHS.F(args...)
	if err != nil {
		return r, err
	}
	return b.Op.F(l, r)
}

var (
	eq  = Eq{}
	lt  = Lt{}
	neq = Neq{}
	lte = Lte{}
	gt  = Gt{}
	gte = Gte{}
)

func EQ(lhs, rhs ValueFunctor) ValueFunctor {
	return Binary{
		LHS: lhs,
		Op:  eq,
		RHS: rhs,
	}
}

func NEQ(lhs, rhs ValueFunctor) ValueFunctor {
	return Binary{
		LHS: lhs,
		Op:  neq,
		RHS: rhs,
	}
}

func LT(lhs, rhs ValueFunctor) ValueFunctor {
	return Binary{
		LHS: lhs,
		Op:  lt,
		RHS: rhs,
	}
}

func LTE(lhs, rhs ValueFunctor) ValueFunctor {
	return Binary{
		LHS: lhs,
		Op:  lte,
		RHS: rhs,
	}
}

func GT(lhs, rhs ValueFunctor) ValueFunctor {
	return Binary{
		LHS: lhs,
		Op:  gt,
		RHS: rhs,
	}
}

func GTE(lhs, rhs ValueFunctor) ValueFunctor {
	return Binary{
		LHS: lhs,
		Op:  gte,
		RHS: rhs,
	}
}

type And struct{}

func (And) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(args[0].Bool() && args[1].Bool()), nil
}

type Or struct{}

func (Or) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(args[0].Bool() || args[1].Bool()), nil
}

var (
	and = And{}
	or  = Or{}
)

func AND(lhs, rhs ValueFunctor) ValueFunctor {
	return Binary{
		LHS: lhs,
		Op:  and,
		RHS: rhs,
	}
}

func OR(lhs, rhs ValueFunctor) ValueFunctor {
	return Binary{
		LHS: lhs,
		Op:  or,
		RHS: rhs,
	}
}

type Distinct struct{}

func (d Distinct) F(args ...reflect.Value) (reflect.Value, error) {
	if len(args) < 2 {
		return args[0], errors.New("Too few args passed to Distinct.F(); expected at least arg ValueFunctor, data []T")
	}

	arg, ok := args[0].Interface().(ValueFunctor)
	if !ok {
		t := args[0].Type()
		tn := t.PkgPath() + "/" + t.Name()
		return args[0], fmt.Errorf("Expected ValueFunctor as first argument for Distinct.F(), but got %s", tn)
	}

	dv := args[1]
	dt := dv.Type()
	if dt.Kind() != reflect.Slice {
		dn := dt.PkgPath() + "/" + dt.Name()
		return args[0], fmt.Errorf("Expected []T as second argument for Distinct.F(), but got %s", dn)
	}

	seen := make(map[interface{}]bool)
	results := reflect.MakeSlice(dt, 0, dv.Len())
	for i := 0; i < dv.Len(); i++ {
		v := dv.Index(i)
		argv, err := arg.F(v)

		if err != nil {
			return v, err
		}

		i := argv.Interface()
		if _, ok := seen[i]; !ok {
			seen[i] = true
			results = reflect.Append(results, v)
		}
	}

	return results, nil
}

var distinct = Distinct{}

func DISTINCT(arg ValueFunctor) ValueFunctor {
	return UnaryLazyArg{
		Op:  distinct,
		Arg: arg,
	}
}

type Index struct{}

func (i Index) F(args ...reflect.Value) (reflect.Value, error) {
	if len(args) < 2 {
		return args[0], errors.New("Too few args passed to Index.F(); expected at least arg ValueFunctor, data []T")
	}

	arg, ok := args[0].Interface().(ValueFunctor)
	if !ok {
		t := args[0].Type()
		tn := t.PkgPath() + "/" + t.Name()
		return args[0], fmt.Errorf("Expected ValueFunctor as first argument for Index.F(), but got %s", tn)
	}

	dv := args[len(args)-1]
	dt := dv.Type()
	if dt.Kind() != reflect.Slice {
		dn := dt.PkgPath() + "/" + dt.Name()
		return args[0], fmt.Errorf("Expected []T as last argument for Index.F(), but got %s", dn)
	}

	argv, err := arg.F(args[1 : len(args)-1]...)
	if err != nil {
		return args[0], err
	}

	idx, ok := argv.Interface().(int)
	if !ok {
		t := argv.Type()
		tn := t.PkgPath() + "/" + t.Name()
		return args[0], fmt.Errorf("Expected int from Index.Arg.F(), but got %s", tn)
	}

	dvl := dv.Len()
	if idx < 0 || idx >= dvl {
		return args[0], fmt.Errorf("Index computed from Index.F(), %d, out of bounds (len=%d)", idx, dvl)
	}

	return dv.Index(idx), nil
}

var index = Index{}

func INDEX(arg ValueFunctor) ValueFunctor {
	return UnaryLazyArg{
		Op:  index,
		Arg: arg,
	}
}

type DescJSON struct {
	Desc json.RawMessage `json:"desc"`
}

type UnaryJSON struct {
	Op  string          `json:"op"`
	Arg json.RawMessage `json:"arg"`
}

type BinaryJSON struct {
	LHS json.RawMessage `json:"lhs"`
	Op  string          `json:"op"`
	RHS json.RawMessage `json:"rhs"`
}

func (d *Desc) UnmarshalJSON(bs []byte) error {
	var raw DescJSON
	if err := json.Unmarshal(bs, &raw); err != nil {
		return err
	}
	if len(raw.Desc) == 0 {
		return errors.New(`Desc with empty "desc" field`)
	}

	var mvf MValueFunctor
	if err := json.Unmarshal(raw.Desc, &mvf); err != nil {
		return err
	}
	d.ValueFunctor = mvf.ValueFunctor
	return nil
}

func (u *UnaryLazyArg) UnmarshalJSON(bs []byte) error {
	var raw UnaryJSON
	if err := json.Unmarshal(bs, &raw); err != nil {
		return err
	}
	var arg MValueFunctor
	if err := json.Unmarshal(raw.Arg, &arg); err != nil {
		return err
	}
	u.Arg = arg.ValueFunctor
	switch raw.Op {
	case "distinct":
		u.Op = distinct
	case "index":
		u.Op = index
	default:
		return fmt.Errorf("Unknown unary operation: \"%s\"", raw.Op)
	}
	return nil
}

func (b *Binary) UnmarshalJSON(bs []byte) error {
	var raw BinaryJSON
	if err := json.Unmarshal(bs, &raw); err != nil {
		return err
	}
	var lhs MValueFunctor
	var rhs MValueFunctor
	if err := json.Unmarshal(raw.LHS, &lhs); err != nil {
		return err
	}
	if err := json.Unmarshal(raw.RHS, &rhs); err != nil {
		return err
	}
	b.LHS = lhs.ValueFunctor
	b.RHS = rhs.ValueFunctor
	switch raw.Op {
	case "eq":
		b.Op = eq
	case "lt":
		b.Op = lt
	case "neq":
		b.Op = neq
	case "lte":
		b.Op = lte
	case "gt":
		b.Op = gt
	case "gte":
		b.Op = gte
	case "and":
		b.Op = and
	case "or":
		b.Op = or
	default:
		return fmt.Errorf("Unknown binary operation: \"%s\"", raw.Op)
	}
	return nil
}

type MValueFunctor struct {
	ValueFunctor
}

func (mvf *MValueFunctor) UnmarshalJSON(bs []byte) error {
	var ula UnaryLazyArg
	if err := json.Unmarshal(bs, &ula); err == nil && ula.Arg != nil {
		mvf.ValueFunctor = ula
		return nil
	}
	var b Binary
	if err := json.Unmarshal(bs, &b); err == nil {
		mvf.ValueFunctor = b
		return nil
	}
	var p Property
	if err := json.Unmarshal(bs, &p); err == nil && p.PropertyName != "" {
		mvf.ValueFunctor = p
		return nil
	}
	var d Desc
	if err := json.Unmarshal(bs, &d); err == nil && d.ValueFunctor != nil {
		mvf.ValueFunctor = d
		return nil
	}
	var dot Dot
	if err := json.Unmarshal(bs, &dot); err == nil && dot.First.PropertyName != "" && dot.Second.PropertyName != "" {
		mvf.ValueFunctor = dot
		return nil
	}
	var m Method
	if err := json.Unmarshal(bs, &m); err == nil && m.MethodName != "" {
		mvf.ValueFunctor = m
		return nil
	}
	var c Constant
	if err := json.Unmarshal(bs, &c); err == nil {
		mvf.ValueFunctor = c
		return nil
	}

	return fmt.Errorf("Failed to unmarshal one of %v from %s", []interface{}{ula, b, p, d, m, c}, string(bs))
}

func (c *Constant) UnmarshalJSON(bs []byte) error {
	vs := []interface{}{
		int(0),
		float32(0),
		false,
		time.Time{},
		"",
	}
	// TODO(markdittmer): Handle null values.
	for _, v := range vs {
		if err := json.Unmarshal(bs, &v); err == nil {
			*c = Constant(reflect.ValueOf(v))
			return nil
		}
	}
	return fmt.Errorf("Failed to unmarshal one of %v from %s", vs, string(bs))
}

func (mvf MValueFunctor) MarshalJSON() ([]byte, error) {
	return json.Marshal(mvf.ValueFunctor)
}

func (c Constant) MarshalJSON() ([]byte, error) {
	return json.Marshal(reflect.Value(c).Interface())
}

func (d Desc) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	_, err := b.WriteString(`{"desc":`)
	if err != nil {
		return nil, err
	}
	inner, err := json.Marshal(d.ValueFunctor)
	if err != nil {
		return nil, err
	}
	_, err = b.Write(inner)
	if err != nil {
		return nil, err
	}
	b.WriteString("}")
	return b.Bytes(), nil
}

func (o Index) MarshalJSON() ([]byte, error) {
	return []byte(`"index"`), nil
}

func (o Distinct) MarshalJSON() ([]byte, error) {
	return []byte(`"distinct"`), nil
}

func (o Eq) MarshalJSON() ([]byte, error) {
	return []byte(`"eq"`), nil
}

func (o Neq) MarshalJSON() ([]byte, error) {
	return []byte(`"neq"`), nil
}

func (o Lt) MarshalJSON() ([]byte, error) {
	return []byte(`"lt"`), nil
}

func (o Lte) MarshalJSON() ([]byte, error) {
	return []byte(`"lte"`), nil
}

func (o Gt) MarshalJSON() ([]byte, error) {
	return []byte(`"gt"`), nil
}

func (o Gte) MarshalJSON() ([]byte, error) {
	return []byte(`"gte"`), nil
}

func (o And) MarshalJSON() ([]byte, error) {
	return []byte(`"and"`), nil
}

func (o Or) MarshalJSON() ([]byte, error) {
	return []byte(`"or"`), nil
}
