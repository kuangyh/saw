package saw

import (
	"encoding/json"
	"io"
	"reflect"
)

type ValueEncoder interface {
	EncodeValue(value interface{}, w io.Writer) error
}

type ValueDecoder interface {
	DecodeValue(r io.Reader) (interface{}, error)
}

type JSONEncoder struct{}

func (je JSONEncoder) EncodeValue(value interface{}, w io.Writer) error {
	return json.NewEncoder(w).Encode(value)
}

type JSONDecoder struct {
	ValueType reflect.Type
}

func (jd JSONDecoder) DecodeValue(r io.Reader) (interface{}, error) {
	value := reflect.New(jd.ValueType).Interface()
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(value); err != nil {
		return nil, err
	}
	return value, nil
}

func NewJSONDecoder(example interface{}) JSONDecoder {
	return JSONDecoder{
		ValueType: reflect.TypeOf(example).Elem(),
	}
}
