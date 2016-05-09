package table

import (
	"errors"
	"github.com/kuangyh/saw"
	"github.com/kuangyh/saw/storage"
	"golang.org/x/net/context"
	"hash/fnv"
	"sync"
)

var InvalidTableSpecErr = errors.New("saw.table: invalid table spec")

type KeyHashFunc func(saw.DatumKey) int

type TableItemFactory func(tableName string, key saw.DatumKey) (saw.Saw, error)

type TableResultMap map[saw.DatumKey]interface{}

type TableSpec struct {
	Name        string
	ItemFactory TableItemFactory

	// Settings for parallel tables

	// KeyHashFunc assigns incoming Datum to one of its shard, defaults to fnv32
	KeyHashFunc KeyHashFunc
	// Defaults to 127
	NumShards int

	// When not empty, table state will be eventually persistent
	PersistentResource storage.ResourceSpec
	// It depends on table type to determine what data get persistent and what
	// encoder to use. Defaults to verbatim (accepts and store []byte)
	ValueEncoder saw.ValueEncoder
	// Implementation may pre-allocate and reuse buffer for encoding values, to avoid
	// frequent malloc, defaults to 4096
	ValueEncodeBufferSize int
}

func defaultGetKeyHash(key saw.DatumKey) int {
	hash := fnv.New32()
	hash.Write([]byte(key))
	return int(hash.Sum32())
}

func fillSpecDefaults(spec *TableSpec) {
	if spec.KeyHashFunc == nil {
		spec.KeyHashFunc = defaultGetKeyHash
	}
	if spec.NumShards == 0 {
		spec.NumShards = 127
	}
	if spec.PersistentResource.HasSpec() {
		if spec.ValueEncodeBufferSize == 0 {
			spec.ValueEncodeBufferSize = 4096
		}
	}
}

type SimpleTable struct {
	spec       TableSpec
	items      map[saw.DatumKey]saw.Saw
	banned     map[saw.DatumKey]error
	numKeysVar saw.VarInt
	errVar     saw.VarInt
}

func NewSimpleTable(spec TableSpec) *SimpleTable {
	fillSpecDefaults(&spec)
	return &SimpleTable{
		spec:       spec,
		items:      make(map[saw.DatumKey]saw.Saw),
		banned:     make(map[saw.DatumKey]error),
		numKeysVar: saw.ReportInt(spec.Name, "keys"),
		errVar:     saw.ReportInt(spec.Name, "errors"),
	}
}

func (tbl *SimpleTable) Emit(kv saw.Datum) (err error) {
	saw, ok := tbl.items[kv.Key]
	if !ok {
		if err, banned := tbl.banned[kv.Key]; banned {
			return err
		}
		var err error
		saw, err = tbl.spec.ItemFactory(tbl.spec.Name, kv.Key)
		if err != nil {
			tbl.banned[kv.Key] = err
			return err
		}
		tbl.items[kv.Key] = saw
		tbl.numKeysVar.Add(1)
	}
	err = saw.Emit(kv)
	if err != nil {
		tbl.errVar.Add(1)
	}
	return err
}

func (tbl *SimpleTable) Result(ctx context.Context) (interface{}, error) {
	result := make(TableResultMap)
	for key, saw := range tbl.items {
		var err error
		v, err := saw.Result(ctx)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		result[key] = v
	}
	return result, nil
}

type MemTable struct {
	spec   TableSpec
	shards []*SimpleTable
	locks  []sync.Mutex
}

func NewMemTable(spec TableSpec) *MemTable {
	fillSpecDefaults(&spec)
	shards := make([]*SimpleTable, spec.NumShards)
	for i := 0; i < spec.NumShards; i++ {
		shards[i] = NewSimpleTable(spec)
	}
	return &MemTable{
		spec:   spec,
		shards: shards,
		locks:  make([]sync.Mutex, spec.NumShards),
	}
}

func (tbl *MemTable) Emit(kv saw.Datum) error {
	shardIdx := tbl.spec.KeyHashFunc(kv.Key) % len(tbl.shards)
	simpleTable := tbl.shards[shardIdx]
	tbl.locks[shardIdx].Lock()
	defer tbl.locks[shardIdx].Unlock()
	return simpleTable.Emit(kv)
}

func (tbl *MemTable) Result(ctx context.Context) (result interface{}, err error) {
	resultMap := make(TableResultMap)

	var collectTable *CollectTable
	if tbl.spec.PersistentResource.HasSpec() {
		collectTableSpec := tbl.spec
		collectTableSpec.Name = collectTableSpec.Name + "_collect"
		collectTable, err = NewCollectTable(ctx, collectTableSpec)
		if err != nil {
			return nil, err
		}
		defer collectTable.Result(ctx)
	}

	for shardIdx, st := range tbl.shards {
		tbl.locks[shardIdx].Lock()
		shardRet, err := st.Result(ctx)
		tbl.locks[shardIdx].Unlock()
		if err != nil {
			return nil, err
		}
		for k, v := range shardRet.(TableResultMap) {
			resultMap[k] = v
			if collectTable != nil {
				if err = collectTable.Emit(saw.Datum{Key: k, Value: v}); err != nil {
					return nil, err
				}
			}
		}
	}
	return resultMap, nil
}
