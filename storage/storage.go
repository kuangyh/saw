package storage

import (
	"errors"
	"fmt"
	"github.com/kuangyh/saw"
	"io"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/net/context"
)

var (
	ErrMalformedPath              = errors.New("malformed path")
	ErrUnknownStorageForamt       = errors.New("unknown storage format")
	ErrUnknownStorageMedia        = errors.New("unknown storage media")
	ErrStorageFeatureNotSupported = errors.New("storage feature not supported")
)

type ResourceSpec struct {
	Format    string
	Media     string
	Path      string
	NumShards int
}

func (rc *ResourceSpec) ShardPath(shard int) string {
	if rc.NumShards == 0 {
		return rc.Path
	}
	return fmt.Sprintf("%s-%05d-of-%05d", rc.Format, shard, rc.NumShards)
}

func (rc *ResourceSpec) IOReader(ctx context.Context, shard int) (io.ReadCloser, error) {
	media, ok := storageMediaMap[rc.Media]
	if !ok {
		return nil, ErrUnknownStorageMedia
	}
	return media.IOReader(ctx, *rc, shard)
}

func (rc *ResourceSpec) IOWriter(ctx context.Context, shard int) (io.WriteCloser, error) {
	media, ok := storageMediaMap[rc.Media]
	if !ok {
		return nil, ErrUnknownStorageMedia
	}
	return media.IOWriter(ctx, *rc, shard)
}

func (rc *ResourceSpec) DatumReader(ctx context.Context, shard int) (DatumReader, error) {
	format, ok := storageFormatMap[rc.Format]
	if !ok {
		return nil, ErrUnknownStorageForamt
	}
	return format.DatumReader(ctx, *rc, shard)
}

func (rc *ResourceSpec) DatumWriter(ctx context.Context, shard int) (DatumWriter, error) {
	format, ok := storageFormatMap[rc.Format]
	if !ok {
		return nil, ErrUnknownStorageForamt
	}
	return format.DatumWriter(ctx, *rc, shard)
}

type StorageFormat interface {
	DatumReader(ctx context.Context, rc ResourceSpec, shard int) (DatumReader, error)
	DatumWriter(ctx context.Context, rc ResourceSpec, shard int) (DatumWriter, error)
}

type StorageMedia interface {
	IOReader(ctx context.Context, rc ResourceSpec, shard int) (io.ReadCloser, error)
	IOWriter(ctx context.Context, rc ResourceSpec, shard int) (io.WriteCloser, error)
}

type DatumReader interface {
	ReadDatum() (saw.Datum, error)
	Close() error
}

type DatumWriter interface {
	WriteDatum(datum saw.Datum) error
	Close() error
}

var (
	storageFormatMap = make(map[string]StorageFormat)
	storageMediaMap  = make(map[string]StorageMedia)
)

func RegisterStorageFormat(name string, format StorageFormat) {
	if _, ok := storageMediaMap[name]; ok {
		panic("duplicated storage format " + name)
	}
	storageFormatMap[name] = format
}

func RegisterStorageMedia(name string, media StorageMedia) {
	if _, ok := storageMediaMap[name]; ok {
		panic("duplicated storage media " + name)
	}
	storageMediaMap[name] = media
}

var resourcePathPattern = regexp.MustCompile("^([^\\s]+)\\:([^@\\s]+)(@\\d+)?$")

func ParseResourcePath(path string) (ResourceSpec, error) {
	m := resourcePathPattern.FindAllStringSubmatch(path, 1)
	if len(m) != 1 {
		return ResourceSpec{}, ErrMalformedPath
	}
	rc := ResourceSpec{Format: m[0][1]}

	rc.Path = m[0][2]
	rc.Media = "local"
	if rc.Path[0] == '/' {
		pair := strings.SplitN(rc.Path[1:], "/", 2)
		if len(pair) == 2 {
			if _, ok := storageMediaMap[pair[0]]; ok {
				rc.Media = pair[0]
				rc.Path = "/" + pair[1]
			}
		}
	}
	if len(m[0][3]) > 0 {
		var err error
		rc.NumShards, err = strconv.Atoi(m[0][3])
		if err != nil {
			return ResourceSpec{}, ErrMalformedPath
		}
	}
	return rc, nil
}

func MustParseResourcePath(path string) ResourceSpec {
	spec, err := ParseResourcePath(path)
	if err != nil {
		panic(err)
	}
	return spec
}
