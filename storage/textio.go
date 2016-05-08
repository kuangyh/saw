package storage

import (
	"bufio"
	"io"
	"strconv"

	"github.com/kuangyh/saw"
	"golang.org/x/net/context"
)

type textFormat struct {
}

func (tf textFormat) DatumReader(
	ctx context.Context, rc ResourceSpec, shard int) (DatumReader, error) {
	f, err := rc.IOReader(ctx, shard)
	if err != nil {
		return nil, err
	}
	return &textDatumReader{
		key:      saw.DatumKey(strconv.Itoa(shard)),
		internal: f,
		reader:   bufio.NewReader(f),
	}, nil
}

func (tf textFormat) DatumWriter(
	ctx context.Context, rc ResourceSpec, shard int) (DatumWriter, error) {
	f, err := rc.IOWriter(ctx, shard)
	if err != nil {
		return nil, err
	}
	// TODO: check media and decide whether to buffer write, maybe for GCS, don't
	// need yet another buffer.
	return &textDatumWriter{internal: f, writer: bufio.NewWriter(f)}, nil
}

type textDatumReader struct {
	key      saw.DatumKey
	internal io.ReadCloser
	reader   *bufio.Reader
}

func (dr *textDatumReader) ReadDatum() (datum saw.Datum, err error) {
	datum.Key = dr.key
	datum.Value, err = dr.reader.ReadBytes('\n')
	return
}

func (dr *textDatumReader) Close() error {
	return dr.internal.Close()
}

type textDatumWriter struct {
	internal io.WriteCloser
	writer   *bufio.Writer
}

func (dw *textDatumWriter) WriteDatum(datum saw.Datum) error {
	if _, err := dw.writer.Write(datum.Value.([]byte)); err != nil {
		return err
	}
	return dw.writer.WriteByte('\n')
}

func (dw *textDatumWriter) Close() error {
	return dw.internal.Close()
}

func init() {
	RegisterStorageFormat("textio", textFormat{})
}
