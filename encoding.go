package saw

import (
	"bytes"
	"encoding/json"
	"reflect"

	"github.com/golang/protobuf/proto"
)

type ValueEncoder interface {
	EncodeValue(value interface{}, buf []byte) ([]byte, error)
}

type ValueDecoder interface {
	DecodeValue(buf []byte) (interface{}, error)
}

type JSONEncoder struct{}

func (je JSONEncoder) EncodeValue(value interface{}, buf []byte) ([]byte, error) {
	w := bytes.NewBuffer(buf)
	w.Reset()
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

type JSONDecoder struct {
	ValueType reflect.Type
}

func (jd JSONDecoder) DecodeValue(buf []byte) (interface{}, error) {
	value := reflect.New(jd.ValueType).Interface()
	if err := json.Unmarshal(buf, value); err != nil {
		return nil, err
	}
	return value, nil
}

func NewJSONDecoder(example interface{}) JSONDecoder {
	return JSONDecoder{
		ValueType: reflect.TypeOf(example).Elem(),
	}
}

type ProtoEncoder struct{}

func (pe ProtoEncoder) EncodeValue(value interface{}, buf []byte) ([]byte, error) {
	pbuf := proto.NewBuffer(buf)
	pbuf.Reset()
	if err := pbuf.Marshal(value.(proto.Message)); err != nil {
		return nil, err
	}
	return pbuf.Bytes(), nil
}

type ProtoDecoder struct {
	ValueType reflect.Type
}

func (pd ProtoDecoder) DecodeValue(buf []byte) (interface{}, error) {
	message := reflect.New(pd.ValueType).Interface().(proto.Message)
	if err := proto.Unmarshal(buf, message); err != nil {
		return nil, err
	}
	return message, nil
}

func NewProtoDecoder(example interface{}) ProtoDecoder {
	return ProtoDecoder{
		ValueType: reflect.TypeOf(example).Elem(),
	}
}
