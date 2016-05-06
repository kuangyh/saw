package saw

import (
	"encoding/json"
	"io"
)

var JSONEncoder = jsonEncoder{}

var JSONDecoder = jsonDecoder{}

type ValueEncoder interface {
	EncodeValue(value interface{}, w io.Writer) error
}

type ValueDecoder interface {
	DecodeValue(r io.Reader, value interface{}) error
}

type jsonEncoder struct{}

func (je jsonEncoder) EncodeValue(value interface{}, w io.Writer) error {
	return json.NewEncoder(w).Encode(value)
}

type jsonDecoder struct{}

func (jd jsonDecoder) DecodeValue(r io.Reader, value interface{}) error {
	return json.NewDecoder(r).Decode(value)
}
