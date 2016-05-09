package storage

import (
	"io"
	"strconv"

	"github.com/kuangyh/recordio"
	"github.com/kuangyh/saw"
	"golang.org/x/net/context"
)

// Format: recordio, recordkv
// Reads and stores data using recordio format specified in github.com/kuangyh/recordio
// recordkv stores one datum in two records: one for key and one for value.
// recordio ignores datum.Key.
type RecordIOFormat struct {
	withKey bool
}

func (rf RecordIOFormat) DatumReader(
	ctx context.Context, rc ResourceSpec, shard int) (DatumReader, error) {
	f, err := rc.IOReader(ctx, shard)
	if err != nil {
		return nil, err
	}
	// TODO(yuheng): consider using bufio
	return &recordIODatumReader{
		rr:       recordio.NewReader(f),
		internal: f,
		readKey:  rf.withKey,
		shardKey: saw.DatumKey(strconv.Itoa(shard)),
	}, nil
}

func (rf RecordIOFormat) DatumWriter(
	ctx context.Context, rc ResourceSpec, shard int) (DatumWriter, error) {
	f, err := rc.IOWriter(ctx, shard)
	if err != nil {
		return nil, err
	}
	// TODO(yuheng): consider using bufio
	return &recordIODatumWriter{
		rw:       recordio.NewWriter(f, recordio.DefaultFlags),
		internal: f,
		writeKey: rf.withKey,
	}, nil
}

type recordIODatumReader struct {
	rr       *recordio.Reader
	internal io.ReadCloser

	readKey  bool
	shardKey saw.DatumKey
	keyBuf   [1024]byte
}

func (reader *recordIODatumReader) ReadDatum() (datum saw.Datum, err error) {
	if reader.readKey {
		var keyBytes []byte
		keyBytes, err = reader.rr.ReadRecord(reader.keyBuf[:])
		if err != nil {
			return
		}
		datum.Key = saw.DatumKey(keyBytes)
	} else {
		datum.Key = reader.shardKey
	}
	datum.Value, err = reader.rr.ReadRecord(nil)
	return
}

func (reader *recordIODatumReader) Close() error {
	return reader.internal.Close()
}

type recordIODatumWriter struct {
	rw       *recordio.Writer
	internal io.WriteCloser

	writeKey bool
	keyBuf   [1024]byte
}

func (writer *recordIODatumWriter) WriteDatum(datum saw.Datum) (err error) {
	if writer.writeKey {
		var keyBytes []byte
		if len(datum.Key) <= len(writer.keyBuf) {
			keyBytes = writer.keyBuf[:len(datum.Key)]
			copy(keyBytes, string(datum.Key))
		} else {
			keyBytes = []byte(datum.Key)
		}
		if err = writer.rw.WriteRecord(keyBytes, recordio.NoCompression); err != nil {
			return err
		}
	}
	writeBytes := datum.Value.([]byte)
	var flags recordio.Flags
	if len(writeBytes) < 1024 {
		flags |= recordio.NoCompression
	}
	return writer.rw.WriteRecord(writeBytes, flags)
}

func (writer *recordIODatumWriter) Close() error {
	return writer.internal.Close()
}

func init() {
	RegisterStorageFormat("recordio", RecordIOFormat{withKey: false})
	RegisterStorageFormat("recordkv", RecordIOFormat{withKey: true})
}
