package saw

import (
	"golang.org/x/net/context"
)

type TransformFunc func(input Datum) (output Datum, err error)

// TransformSpec configures a Transform.
//
// Transform is a stateless Saw, that for each Datum input --- normally from Hub,
// it calls a pure TransformFunc to get a Datum output, then publish the output
// to a set of Hub topics.
//
// Use RegisterTransform() to create a Transform saw and register it to a Hub.
type TransformSpec struct {
	Name      string
	Transfrom TransformFunc
	Inputs    []TopicID
	Outputs   []TopicID
}

type transfromSaw struct {
	spec   TransformSpec
	errVar VarInt
	hub    *Hub
}

func (ts *transfromSaw) Emit(datum Datum) error {
	output, err := ts.spec.Transfrom(datum)
	if err != nil {
		ts.errVar.Add(1)
		return err
	}
	for _, topic := range ts.spec.Outputs {
		ts.hub.Publish(topic, output)
	}
	return nil
}

func (ts *transfromSaw) Result(ctx context.Context) (interface{}, error) {
	return nil, nil
}

// RegisterTransform creates a Transform Saw instance, register it on hub with
// spec.Inputs topics subscribed.
func RegisterTransform(hub *Hub, spec TransformSpec) {
	ts := &transfromSaw{
		spec:   spec,
		errVar: ReportInt(spec.Name, "errors"),
		hub:    hub,
	}
	hub.Register(ts, spec.Inputs...)
}
