package table

import (
	"bytes"
	"errors"
	"github.com/kuangyh/saw"
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
	PersistentPath string
	// It depends on table type to determine what data get persistent and what
	// encoder to use.
	ValueEncoder saw.ValueEncoder
	// Implementation uses a sync.Pool to avoid frequent alloc encoding buffer
	// adjust it according to your normal value size to be persistent, defualts to
	// 256
	EncodingPoolBufferSize int
}

func getKeyHash(key saw.DatumKey) int {
	hash := fnv.New32()
	hash.Write([]byte(key))
	return int(hash.Sum32())
}

func fillSpecDefaults(spec *TableSpec) {
	if spec.KeyHashFunc == nil {
		spec.KeyHashFunc = getKeyHash
	}
	if spec.NumShards == 0 {
		spec.NumShards = 127
	}
	if spec.EncodingPoolBufferSize == 0 {
		spec.EncodingPoolBufferSize = 256
	}
}

type SimpleTable struct {
	spec       TableSpec
	items      map[saw.DatumKey]saw.Saw
	banned     map[saw.DatumKey]error
	numKeysVar saw.VarInt
}

func NewSimpleTable(spec TableSpec) *SimpleTable {
	fillSpecDefaults(&spec)
	return &SimpleTable{
		spec:       spec,
		items:      make(map[saw.DatumKey]saw.Saw),
		banned:     make(map[saw.DatumKey]error),
		numKeysVar: saw.ReportInt(spec.Name, "keys"),
	}
}

func (tbl *SimpleTable) Emit(kv saw.Datum) error {
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
	return saw.Emit(kv)
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
	var tableWriter TableWriter
	resultMap := make(TableResultMap)
	result = resultMap

	if len(tbl.spec.PersistentPath) > 0 {
		tableWriter, err = openSSTableWriter(tbl.spec.PersistentPath)
		if err != nil {
			return nil, err
		}
		defer tableWriter.Close()
	}

	var valueBuffer bytes.Buffer
	for shardIdx, st := range tbl.shards {
		tbl.locks[shardIdx].Lock()
		shardRet, err := st.Result(ctx)
		tbl.locks[shardIdx].Unlock()
		if err != nil {
			return nil, err
		}
		for k, v := range shardRet.(TableResultMap) {
			resultMap[k] = v
			if tableWriter != nil {
				valueBuffer.Reset()
				if err = tbl.spec.ValueEncoder.EncodeValue(v, &valueBuffer); err != nil {
					return nil, err
				}
				if err = tableWriter.Put(shardIdx, saw.Datum{
					Key:       k,
					Value:     valueBuffer.Bytes(),
					SortOrder: 0,
				}); err != nil {
					return nil, err
				}
			}
		}
	}
	return resultMap, nil
}

type collectionWriteOp struct {
	shard int
	datum saw.Datum
}

type CollectionTable struct {
	spec        TableSpec
	tableWriter TableWriter
	bufferPool  *sync.Pool
	countVar    saw.VarInt
}

func NewCollectionTable(spec TableSpec) (*CollectionTable, error) {
	fillSpecDefaults(&spec)
	if len(spec.PersistentPath) == 0 || spec.ValueEncoder == nil {
		return nil, InvalidTableSpecErr
	}
	tableWriter, err := openSSTableWriter(spec.PersistentPath)
	if err != nil {
		return nil, err
	}
	bufferPool := sync.Pool{}
	bufferSize := spec.EncodingPoolBufferSize
	bufferPool.New = func() interface{} {
		return make([]byte, bufferSize)
	}
	return &CollectionTable{
		spec:        spec,
		tableWriter: tableWriter,
		bufferPool:  &bufferPool,
		countVar:    saw.ReportInt(spec.Name, "count"),
	}, nil
}

func (tbl *CollectionTable) Emit(kv saw.Datum) error {
	tbl.countVar.Add(1)
	shardIdx := tbl.spec.KeyHashFunc(kv.Key) % tbl.spec.NumShards
	b := tbl.bufferPool.Get().([]byte)
	defer tbl.bufferPool.Put(b)
	buf := bytes.NewBuffer(b)
	if err := tbl.spec.ValueEncoder.EncodeValue(kv.Value, buf); err != nil {
		return err
	}
	kv.Value = buf.Bytes()
	return tbl.tableWriter.Put(shardIdx, kv)
}

func (tbl *CollectionTable) Result(ctx context.Context) (result interface{}, err error) {
	return nil, tbl.tableWriter.Close()
}
