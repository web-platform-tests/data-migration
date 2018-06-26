package main

import (
	"encoding/json"
	"log"
	"reflect"

	r "github.com/web-platform-tests/data-migration/grid/reflect"
)

type Thing struct {
	Name string `json:"name"`
}

func (t Thing) GetName() string {
	return t.Name
}

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile | log.LUTC)

	log.Printf("%v", r.Eq{})

	var v reflect.Value
	var err error

	{
		str := `{"lhs":{"property_name":"Name"},"op":"neq","rhs":"Mark"}`

		var mvf r.MValueFunctor
		err = json.Unmarshal([]byte(str), &mvf)
		if err != nil {
			log.Fatal(err)
		}
		v, err = mvf.F(reflect.ValueOf(Thing{"Joe"}))
		if err != nil {
			log.Printf("ERRO: %v", err)
		} else {
			log.Printf("Joe != Mark %v", v.Interface())
		}
		v, err = mvf.F(reflect.ValueOf(Thing{"Mark"}))
		if err != nil {
			log.Printf("ERRO: %v", err)
		} else {
			log.Printf("Mark != Mark %v", v.Interface())
		}

		bs, err := json.Marshal(mvf.ValueFunctor)
		if err != nil {
			log.Printf("ERR: %v", err)
		} else {
			log.Printf("%s", string(bs))
		}
	}

	{
		str := `{"lhs":{"method_name":"GetName"},"op":"neq","rhs":"Mark"}`

		var mvf r.MValueFunctor
		err = json.Unmarshal([]byte(str), &mvf)
		if err != nil {
			log.Fatal(err)
		}
		v, err = mvf.F(reflect.ValueOf(Thing{"Joe"}))
		if err != nil {
			log.Printf("ERRO: %v", err)
		} else {
			log.Printf("Joe != Mark %v", v.Interface())
		}
		v, err = mvf.F(reflect.ValueOf(Thing{"Mark"}))
		if err != nil {
			log.Printf("ERRO: %v", err)
		} else {
			log.Printf("Mark != Mark %v", v.Interface())
		}

		bs, err := json.Marshal(mvf.ValueFunctor)
		if err != nil {
			log.Printf("ERR: %v", err)
		} else {
			log.Printf("%s", string(bs))
		}
	}

	/*
		log.Printf("%v", reflect.ValueOf(Thing{"Joe"}).FieldByName("Name").Type().Kind() == reflect.String)
	*/
}
