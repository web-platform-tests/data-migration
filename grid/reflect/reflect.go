package reflect

import (
	"encoding/json"
	"fmt"
	"go/types"
	"math/cmplx"
	"reflect"
	"sort"
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
		} else if GreaterThan(c1, v2i) {
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
		} else if GreaterThan(c1, v2i) {
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
		} else if GreaterThan(c1, v2i) {
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
		} else if GreaterThan(c1, v2i) {
			return false
		}
	}

	return false
}

func GetComparable(v reflect.Value) Comparable {
	k := indirect(v).Type().Kind()
	switch k {
	case reflect.Int:
		return Int(v)
	case reflect.Int8:
		return Int8(v)
	case reflect.Int16:
		return Int16(v)
	case reflect.Int32:
		return Int32(v)
	case reflect.Int64:
		return Int64(v)
	case reflect.Uint:
		return Uint(v)
	case reflect.Uint8:
		return Uint8(v)
	case reflect.Uint16:
		return Uint16(v)
	case reflect.Uint32:
		return Uint32(v)
	case reflect.Uint64:
		return Uint64(v)
	case reflect.Uintptr:
		return Uintptr(v)
	case reflect.Float32:
		return Float32(v)
	case reflect.Float64:
		return Float64(v)
	case reflect.Complex64:
		return Complex64(v)
	case reflect.Complex128:
		return Complex128(v)
	case reflect.Array:
		return Array{v}
	case reflect.Slice:
		return Slice{v}
	case reflect.Map:
		return Map(v)
	case reflect.String:
		return String(v)
	case reflect.Struct:
		return Struct(v)
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

/*
type Functor struct {
	BasicFunctor
	*ValueFunctor
}
*/

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

/*
type ValueFunctor struct {
	BasicValueFunctor
	*Functor
}
*/

func VF(vf ValueFunctor, args ...reflect.Value) (reflect.Value, error) {
	return vf.F(args...)
}

type Constant reflect.Value

func (c Constant) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.Value(c), nil
}

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
	return method.Call(args[1:])[0], nil
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

type AndOp struct{}

func (AndOp) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(args[0].Bool() && args[1].Bool()), nil
}

type OrOp struct{}

func (OrOp) F(args ...reflect.Value) (reflect.Value, error) {
	return reflect.ValueOf(args[0].Bool() || args[1].Bool()), nil
}

var (
	and = AndOp{}
	or  = OrOp{}
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

type Distinct struct {
	Arg ValueFunctor `json:"distinct"`
}

func (d Distinct) F(args ...reflect.Value) (reflect.Value, error) {
	seen := make(map[interface{}]bool)
	results := reflect.MakeSlice(reflect.TypeOf(args[0]), 0, 0)

	for _, v := range args {
		kv, err := d.Arg.F(v)
		if err != nil {
			return kv, err
		}

		k := kv.Interface()
		if _, ok := seen[k]; !ok {
			seen[k] = true
			results = reflect.Append(results, v)
		}
	}

	return results, nil
}

func DISTINCT(arg ValueFunctor) ValueFunctor {
	return Distinct{arg}
}

type BinaryJSON struct {
	LHS json.RawMessage `json:"lhs"`
	Op  string          `json:"op"`
	RHS json.RawMessage `json:"rhs"`
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
	default:
		return fmt.Errorf("Unknown binary operation: \"%s\"", raw.Op)
	}
	return nil
}

type MValueFunctor struct {
	ValueFunctor
}

func (mvf *MValueFunctor) UnmarshalJSON(bs []byte) error {
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
	var d Dot
	if err := json.Unmarshal(bs, &d); err == nil && d.First.PropertyName != "" && d.Second.PropertyName != "" {
		mvf.ValueFunctor = d
		return nil
	}
	var dis Distinct
	if err := json.Unmarshal(bs, &dis); err == nil && dis.Arg != nil {
		mvf.ValueFunctor = dis
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

	return fmt.Errorf("Failed to unmarshal one of %v from %s", []interface{}{b, p, d, dis, m, c}, string(bs))
}

func (c *Constant) UnmarshalJSON(bs []byte) error {
	vs := []interface{}{
		int(0),
		float32(0),
		false,
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

func (c Constant) MarshalJSON() ([]byte, error) {
	return json.Marshal(reflect.Value(c).Interface())
}

func (o Neq) MarshalJSON() ([]byte, error) {
	return []byte(`"neq"`), nil
}
