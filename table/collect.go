package table

import (
	"github.com/kuangyh/saw"
	"github.com/kuangyh/saw/storage"
	"golang.org/x/net/context"
	"sync"
)

// Encode and write to shard, conforms to saw.DatumWriter but should not be use
// externally
type shardDatumWriter struct {
	internal     storage.DatumWriter
	mu           sync.Mutex
	valueEncoder saw.ValueEncoder
	encodeBuffer []byte
}

func (shard *shardDatumWriter) WriteDatum(datum saw.Datum) (err error) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.valueEncoder != nil {
		var encoded []byte
		encoded, err = shard.valueEncoder.EncodeValue(datum.Value, shard.encodeBuffer)
		if err != nil {
			return err
		}
		datum.Value = encoded
	}
	return shard.internal.WriteDatum(datum)
}

func (shard *shardDatumWriter) Close() error {
	return shard.internal.Close()
}

// Collect is a special table that it doesn't do any computation, but simply
// stores everything it receives.
type CollectTable struct {
	spec     TableSpec
	shards   []*shardDatumWriter
	countVar saw.VarInt
	errVar   saw.VarInt
}

// Creates a new CollectTable, returns error when underling DatumWriter creation
// fails.
//
// TableItemFactory and NumShards in TableSpec is no-op, # shards outputed is
// soley determined by PersistentResource.NumShards
//
func NewCollectTable(ctx context.Context, spec TableSpec) (table *CollectTable, err error) {
	fillSpecDefaults(&spec)

	var numShards int
	if spec.PersistentResource.Sharded() {
		numShards = spec.PersistentResource.NumShards
	} else {
		numShards = 1
	}
	internalWriters := make([]storage.DatumWriter, numShards)
	for i := 0; i < numShards; i++ {
		internalWriters[i], err = spec.PersistentResource.DatumWriter(ctx, i)
		if err != nil {
			for j := 0; j < i; j++ {
				internalWriters[j].Close()
			}
			return nil, err
		}
	}
	shards := make([]*shardDatumWriter, len(internalWriters))
	for i, internal := range internalWriters {
		shards[i] = &shardDatumWriter{
			internal:     internal,
			valueEncoder: spec.ValueEncoder,
			encodeBuffer: make([]byte, spec.ValueEncodeBufferSize),
		}
	}
	return &CollectTable{
		spec:     spec,
		shards:   shards,
		countVar: saw.ReportInt(spec.Name, "count"),
		errVar:   saw.ReportInt(spec.Name, "errors"),
	}, nil
}

func (tbl *CollectTable) Emit(datum saw.Datum) (err error) {
	shardIdx := tbl.spec.KeyHashFunc(datum.Key) % len(tbl.shards)
	err = tbl.shards[shardIdx].WriteDatum(datum)
	tbl.countVar.Add(1)
	if err != nil {
		tbl.errVar.Add(1)
	}
	return err
}

func (tbl *CollectTable) Result(ctx context.Context) (interface{}, error) {
	for _, shard := range tbl.shards {
		shard.Close()
	}
	// panic if reuse
	tbl.shards = nil
	return tbl.spec.PersistentResource, nil
}
