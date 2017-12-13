// reflect helpers

package papergres

import (
	"errors"
	"reflect"
)

// GetTypeName gets the type name of an object
func getTypeName(v interface{}) string {
	t := reflect.TypeOf(v)
	switch t.Kind() {
	case reflect.Ptr:
		return t.Elem().Name()
	case reflect.Slice:
		return "[]" + t.Elem().Name()
	}
	return t.Name()
}

// IsSlice determines if an interface is a slice
func isSlice(v interface{}) bool {
	t := reflect.TypeOf(v)
	return t.Kind() == reflect.Slice
}

// IsPointer determines if an object is a pointer
func isPointer(v interface{}) bool {
	t := reflect.TypeOf(v)
	return t.Kind() == reflect.Ptr
}

// ConvertToSlice converts an interface into a slice of interfaces.
// v's underlying type has to be a slice
func convertToSlice(v interface{}) ([]interface{}, error) {
	if !isSlice(v) {
		return nil, errors.New("value is not a slice")
	}
	val := reflect.ValueOf(v)
	s := make([]interface{}, val.Len())
	for i := 0; i < val.Len(); i++ {
		s[i] = val.Index(i).Interface()
	}
	return s, nil
}

// Field is a struct field
type Field struct {
	Typeof string
	Name   string
	Tag    string
	Value  interface{}
}

// Fields returns a struct's fields and their values
func fields(v interface{}) []*Field {
	var val reflect.Value
	if isPointer(v) {
		val = reflect.ValueOf(v).Elem()
	} else {
		val = reflect.ValueOf(v)
	}

	fields := make([]*Field, val.NumField())
	vtype := val.Type()
	for i := 0; i < val.NumField(); i++ {
		f := val.Field(i)
		field := &Field{
			Typeof: getTypeName(f.Interface()),
			Name:   vtype.Field(i).Name,
			Value:  f.Interface(),
			Tag:    vtype.Field(i).Tag.Get("db"),
		}
		fields[i] = field
	}

	return fields
}
