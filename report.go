package saw

import (
	"expvar"
	"sync"
)

type VarInt interface {
	Add(delta int64)
	Set(value int64)
}

type VarFloat interface {
	Add(delta float64)
	Set(value float64)
}

var varLock sync.Mutex

func ReportInt(ns, name string) VarInt {
	varName := ns + "." + name
	varLock.Lock()
	defer varLock.Unlock()

	if v := expvar.Get(varName); v != nil {
		return v.(*expvar.Int)
	}
	return expvar.NewInt(varName)
}

func ReportFloat(ns, name string) VarFloat {
	varName := ns + "." + name
	varLock.Lock()
	defer varLock.Unlock()

	if v := expvar.Get(varName); v != nil {
		return v.(*expvar.Float)
	}
	return expvar.NewFloat(varName)
}
