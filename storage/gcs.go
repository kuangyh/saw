package storage

import (
	"io"
	"log"
	"strings"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	gcs "google.golang.org/api/storage/v1"
)

type gcsMedia struct {
}

func (gm gcsMedia) IOReader(
	ctx context.Context, rc ResourceSpec, shard int) (io.ReadCloser, error) {
	pair := strings.SplitN(rc.Path[1:], "/", 2)
	if len(pair) != 2 {
		return nil, ErrMalformedPath
	}
	cli, err := google.DefaultClient(ctx, gcs.DevstorageReadWriteScope)
	if err != nil {
		return nil, err
	}
	serv, err := gcs.New(cli)
	if err != nil {
		return nil, err
	}
	res, err := serv.Objects.Get(pair[0], pair[1]).Download()
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

type waitWriteHalf struct {
	*io.PipeWriter
	err    error
	finish chan struct{}
}

func (wh *waitWriteHalf) Close() error {
	wh.PipeWriter.Close()
	<-wh.finish
	return wh.err
}

func (gm gcsMedia) IOWriter(
	ctx context.Context, rc ResourceSpec, shard int) (io.WriteCloser, error) {
	pair := strings.SplitN(rc.Path[1:], "/", 2)
	if len(pair) != 2 {
		return nil, ErrMalformedPath
	}
	cli, err := google.DefaultClient(ctx, gcs.DevstorageReadWriteScope)
	if err != nil {
		return nil, err
	}
	serv, err := gcs.New(cli)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	handle := &waitWriteHalf{PipeWriter: pw, finish: make(chan struct{})}

	call := serv.Objects.Insert(pair[0], &gcs.Object{Name: pair[1]})
	go func() {
		if _, err := call.Media(pr).Do(); err != nil {
			log.Printf("gcs write bucket=%s name=%s err %v, %v", pair[0], pair[1], err)
			handle.err = err
		}
		pr.Close()
		close(handle.finish)
	}()
	return handle, nil
}

func init() {
	RegisterStorageMedia("gs", &gcsMedia{})
}
