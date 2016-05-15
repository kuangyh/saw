package table

import (
	"errors"
	"github.com/kuangyh/saw"
	"github.com/kuangyh/saw/storage"
	"golang.org/x/net/context"
	"hash/fnv"
	"reflect"
	"sync"
	"sync/atomic"
)

var ErrInvalidTableSpec = errors.New("saw.table: invalid table spec")

type KeyHashFunc func(saw.DatumKey) int

type TableItemFactory func(tableName string, key saw.DatumKey) (saw.Saw, error)

// A simple item factory that creates zero value (not copy!) instance of saw type
// in paramter, panic if saw is not pointer receiver.
func ItemFactoryOf(example saw.Saw) TableItemFactory {
	instanceType := reflect.TypeOf(example).Elem()
	return func(tableName string, key saw.DatumKey) (saw.Saw, error) {
		return reflect.New(instanceType).Interface().(saw.Saw), nil
	}
}

// SimpleTable and MemTable result type
type TableResultMap map[saw.DatumKey]interface{}

type InspectCallback func(key saw.DatumKey, item saw.Saw) error

// Inspectable tables allows a callback to inspect one, a set of, or all saws
// in the table, table gurantees no concurrent Emit() for the saw being inspected.
//
// InspectSet() and InspectAll() can inspect items in parallel when concurrent=true.
// All inspect functions return number of items been inspected without error, and
// one of the error callback returned if there's any, inspect functions stops as
// soon as it encounter error.
type Inspectable interface {
	Inspect(key saw.DatumKey, callback InspectCallback) (inspected int, err error)
	InspectSet(keys []saw.DatumKey, callback InspectCallback, concurrent bool) (inspected int, err error)
	InspectAll(callback InspectCallback, concurrent bool) (inspected int, err error)
}

// TableSpec is shared configuration of Table implementations in this package.
type TableSpec struct {
	Name        string
	ItemFactory TableItemFactory

	// Settings for parallel tables

	// KeyHashFunc assigns incoming Datum to one of its shard, defaults to fnv32
	KeyHashFunc KeyHashFunc
	// Defaults to 127
	NumShards int

	// When not empty, table state will be stored at external storage.
	PersistentResource storage.ResourceSpec
	// It depends on table type to determine what data get persistent and what
	// encoder to use. Defaults to verbatim (accepts and stores []byte)
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

// SimpleTable is a in-memory, non-storable memory table, concurrent non-safe
// table. Good for handling small set of data in mini-batch --- aggregate stats
// for a single user session etc.
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

func (tbl *SimpleTable) Inspect(key saw.DatumKey, callback InspectCallback) (int, error) {
	saw, ok := tbl.items[key]
	if !ok {
		return 0, nil
	}
	if err := callback(key, saw); err != nil {
		return 0, err
	}
	return 1, nil
}

func (tbl *SimpleTable) InspectSet(
	keys []saw.DatumKey, callback InspectCallback, concurrent bool) (int, error) {
	total := 0
	for _, key := range keys {
		n, err := tbl.Inspect(key, callback)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (tbl *SimpleTable) InspectAll(callback InspectCallback, concurrent bool) (int, error) {
	total := 0
	for key, saw := range tbl.items {
		err := callback(key, saw)
		if err != nil {
			return total, err
		}
		total += 1
	}
	return total, nil
}

// Returns TableResultMap, each item as Result() of item saw. nil item results are ignored.
//
// When error presents in individual items Result(), it still tries  to get results
// of all others, then a partial result and one of the item result error will be
// returned.
func (tbl *SimpleTable) Result(ctx context.Context) (interface{}, error) {
	result := make(TableResultMap)
	var err error
	for key, saw := range tbl.items {
		var v interface{}
		v, err = saw.Result(ctx)
		if v == nil || err != nil {
			continue
		}
		result[key] = v
	}
	return result, err
}

// MemTable manages a set (spec.NumShards) of SimpleTables, provides concurrent
// safe Emit(), stores finaly result when Result() called if there is a
// spec.PersistentResource setting.
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

func (tbl *MemTable) forEachShard(
	callback func(shardIdx int, shard *SimpleTable) error, concurrent bool, stopWhenErr bool) error {
	if !concurrent {
		var lastErr error
		for i, shard := range tbl.shards {
			tbl.locks[i].Lock()
			err := callback(i, shard)
			tbl.locks[i].Unlock()
			if err != nil {
				if stopWhenErr {
					return err
				}
				lastErr = err
			}
		}
		return lastErr
	} else {
		var collectedErr atomic.Value
		var wg sync.WaitGroup
		for i, shard := range tbl.shards {
			wg.Add(1)
			go func(shardIdx int, shard *SimpleTable) {
				defer wg.Done()
				tbl.locks[shardIdx].Lock()
				defer tbl.locks[shardIdx].Unlock()
				err := callback(shardIdx, shard)
				if err != nil {
					collectedErr.Store(err)
				}
			}(i, shard)
		}
		wg.Wait()
		if err := collectedErr.Load(); err != nil {
			return err.(error)
		}
		return nil
	}
}

func (tbl *MemTable) Inspect(key saw.DatumKey, callback InspectCallback) (int, error) {
	shardIdx := tbl.spec.KeyHashFunc(key) % len(tbl.shards)
	tbl.locks[shardIdx].Lock()
	defer tbl.locks[shardIdx].Unlock()
	return tbl.shards[shardIdx].Inspect(key, callback)
}

func (tbl *MemTable) InspectSet(
	keys []saw.DatumKey, callback InspectCallback, concurrent bool) (int, error) {
	// TODO(yuheng): that's slow when key set are small, implement fast path if we need.
	keysByShard := make([][]saw.DatumKey, len(tbl.shards))
	for _, key := range keys {
		shardIdx := tbl.spec.KeyHashFunc(key) % len(tbl.shards)
		keysByShard[shardIdx] = append(keysByShard[shardIdx], key)
	}
	var total int64
	err := tbl.forEachShard(func(shardIdx int, shard *SimpleTable) error {
		shardTotal, err := shard.InspectSet(keysByShard[shardIdx], callback, concurrent)
		atomic.AddInt64(&total, int64(shardTotal))
		return err
	}, concurrent, true)
	return int(total), err
}

func (tbl *MemTable) InspectAll(callback InspectCallback, concurrent bool) (int, error) {
	var total int64
	err := tbl.forEachShard(func(shardIdx int, shard *SimpleTable) error {
		shardTotal, err := shard.InspectAll(callback, concurrent)
		atomic.AddInt64(&total, int64(shardTotal))
		return err
	}, concurrent, true)
	return int(total), err
}

// Returns TableResultMap, each item as Result() of item saw. nil item results are ignored.
//
// When error presents in individual items Result(), it still tries  to get results
// of all others, then a partial result and one of the item result error will be
// returned.
//
// When tbl.spec.PersistentResource set, results will be write to persistent store,
// all items' Result() will still be called when persistent fails.
func (tbl *MemTable) Result(ctx context.Context) (interface{}, error) {
	var finalErr error
	var collectTable *CollectTable
	if tbl.spec.PersistentResource.HasSpec() {
		collectTableSpec := tbl.spec
		collectTableSpec.Name = collectTableSpec.Name + "_collect"
		var err error
		collectTable, err = NewCollectTable(ctx, collectTableSpec)
		if err != nil {
			finalErr = err
		} else {
			defer collectTable.Result(ctx)
		}
	}

	retByShard := make([]TableResultMap, len(tbl.shards))
	err := tbl.forEachShard(func(shardIdx int, shard *SimpleTable) error {
		var lastErr error
		shardRet, lastErr := shard.Result(ctx)
		if shardRet != nil {
			retByShard[shardIdx] = shardRet.(TableResultMap)
		}
		if collectTable != nil {
			for k, v := range retByShard[shardIdx] {
				if err := collectTable.Emit(saw.Datum{Key: k, Value: v}); err != nil {
					lastErr = err
				}
			}
		}
		return lastErr
	}, true, false)
	if err != nil {
		finalErr = err
	}

	resultMap := make(TableResultMap)
	for _, m := range retByShard {
		if m == nil {
			continue
		}
		for k, v := range m {
			resultMap[k] = v
		}
	}
	return resultMap, finalErr
}
