package table

import (
	"encoding/binary"
	"errors"
	"github.com/kuangyh/saw"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"os"
	"sync"
	"sync/atomic"
)

type TableWriter interface {
	Put(shard int, datum saw.Datum) error
	Close() error
}

var malformedSSTableKeyErr = errors.New("saw.table: malformed sstable key")

type ssTableKey struct {
	shard uint32
	key   []byte
	order [2]uint64
}

func parseSSTableKey(src []byte) (ssTableKey, error) {
	output := ssTableKey{}

	if len(src) < 8 {
		return output, malformedSSTableKeyErr
	}
	output.shard = binary.BigEndian.Uint32(src[:4])
	keyLen := binary.BigEndian.Uint32(src[4:8])
	if len(src) != int(4+4+keyLen+16) {
		return output, malformedSSTableKeyErr
	}
	output.key = src[4+4 : 4+4+keyLen]
	output.order[0] = binary.BigEndian.Uint64(src[4+4+keyLen:])
	output.order[1] = binary.BigEndian.Uint64(src[4+4+keyLen+8:])
	return output, nil
}

func (k ssTableKey) encode() []byte {
	// Layout: shard: uint32, keyLen: uint32, keyString, order: uint64
	output := make([]byte, 4+4+len(k.key)+16)
	binary.BigEndian.PutUint32(output[:4], k.shard)
	binary.BigEndian.PutUint32(output[4:8], uint32(len(k.key)))
	copy(output[8:], k.key)
	binary.BigEndian.PutUint64(output[4+4+len(k.key):], k.order[0])
	binary.BigEndian.PutUint64(output[4+4+len(k.key)+8:], k.order[1])
	return output
}

type ssTableWriter struct {
	globalOrder uint64
	db          *leveldb.DB
}

func openSSTableWriter(path string, writeBufferSize int) (TableWriter, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{
		CompactionTableSize: 4 * MiB,
		CompactionTotalSize: 16 * MiB,
		WriteBuffer:         writeBufferSize,
	})
	if err != nil {
		return nil, err
	}
	return &ssTableWriter{db: db}, nil
}

func (w *ssTableWriter) Put(shard int, datum saw.Datum) error {
	ssTableKey := ssTableKey{
		shard: uint32(shard),
		key:   []byte(datum.Key),
		order: [2]uint64{datum.SortOrder, atomic.AddUint64(&w.globalOrder, 1)},
	}
	return w.db.Put(ssTableKey.encode(), datum.Value.([]byte), nil)
}

func (w *ssTableWriter) Close() error {
	return w.db.Close()
}

type mockFileWriter struct {
	f         *os.File
	writeChan chan []byte
	wg        sync.WaitGroup
}

func openMockFileWriter(path string) (TableWriter, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	writer := &mockFileWriter{
		f:         f,
		writeChan: make(chan []byte, 10000),
	}
	go writer.handleWrite()
	return writer, nil
}

func (w *mockFileWriter) handleWrite() {
	w.wg.Add(1)
	for buf := range w.writeChan {
		w.f.Write(buf)
	}
	w.wg.Done()
}
func (w *mockFileWriter) Put(shard int, datum saw.Datum) error {
	ssTableKey := ssTableKey{
		shard: uint32(shard),
		key:   []byte(datum.Key),
		order: [2]uint64{datum.SortOrder, 0},
	}
	w.writeChan <- ssTableKey.encode()
	w.writeChan <- datum.Value.([]byte)
	return nil
}

func (w *mockFileWriter) Close() error {
	close(w.writeChan)
	w.wg.Wait()
	return w.f.Close()
}
