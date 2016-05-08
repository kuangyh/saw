package storage

import (
	"io"
	"os"

	"golang.org/x/net/context"
)

type localMedia struct {
}

func (lm localMedia) IOReader(
	ctx context.Context, rc ResourceSpec, shard int) (io.ReadCloser, error) {
	if rc.Path == "STDIN" {
		return os.Stdin, nil
	}
	return os.Open(rc.ShardPath(shard))
}

func (lm localMedia) IOWriter(
	ctx context.Context, rc ResourceSpec, shard int) (io.WriteCloser, error) {
	if rc.Path == "STDOUT" {
		return os.Stdout, nil
	}
	if rc.Path == "STDERR" {
		return os.Stderr, nil
	}
	return os.OpenFile(rc.ShardPath(shard), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
}

func init() {
	RegisterStorageMedia("local", localMedia{})
}
