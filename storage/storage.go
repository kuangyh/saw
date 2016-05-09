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

// ResourceSpec specifies a external data source / destination in Saw.
type ResourceSpec struct {
	Format    string
	Media     string
	Path      string
	NumShards int
}

const localMediaName = "local"

func (rc *ResourceSpec) String() string {
	var path = rc.Path
	if rc.Media == localMediaName && path[0] != '/' {
		path = "./" + path
	}
	if rc.Sharded() {
		return fmt.Sprintf("%s:/%s%s@%d", rc.Format, rc.Media, path, rc.NumShards)
	} else {
		return fmt.Sprintf("%s:/%s%s", rc.Format, rc.Media, path)
	}
}

// Check if it's not zero value.
func (rc *ResourceSpec) HasSpec() bool {
	return len(rc.Format) > 0
}

func (rc *ResourceSpec) Sharded() bool {
	return rc.NumShards > 0
}

// For shared path, it returns {path}-{shardIndex}-of-{totalShards}, it's a
// recommended format when stores sharded data in filesystem, but individual
// impelementation can have there own rules.
func (rc *ResourceSpec) ShardPath(shard int) string {
	if !rc.Sharded() {
		return rc.Path
	}
	return fmt.Sprintf("%s-%05d-of-%05d", rc.Path, shard, rc.NumShards)
}

// Returns io.ReaderCloser for Media specified in ResourceSpec, that can read
// from specified shard, it would not points to local file system, or even not
// points to a persistent storage (as consumer of message system eg.)
func (rc *ResourceSpec) IOReader(ctx context.Context, shard int) (io.ReadCloser, error) {
	media, ok := storageMediaMap[rc.Media]
	if !ok {
		return nil, ErrUnknownStorageMedia
	}
	return media.IOReader(ctx, *rc, shard)
}

// Returns io.ReaderCloser for Media specified in ResourceSpec, that can write to
// specified shard, it would not points to local file system, or even not points
// to a persistent storage (emits to message system eg.)
func (rc *ResourceSpec) IOWriter(ctx context.Context, shard int) (io.WriteCloser, error) {
	media, ok := storageMediaMap[rc.Media]
	if !ok {
		return nil, ErrUnknownStorageMedia
	}
	return media.IOWriter(ctx, *rc, shard)
}

// Returns a DatumReader for format specified in ResourceSpec, it may or may not
// use underling IOReader for reading, when the specified format cannot be
// implemented on a specific media, ErrStorageFeatureNotSupported will be returned.
func (rc *ResourceSpec) DatumReader(ctx context.Context, shard int) (DatumReader, error) {
	format, ok := storageFormatMap[rc.Format]
	if !ok {
		return nil, ErrUnknownStorageForamt
	}
	return format.DatumReader(ctx, *rc, shard)
}

// Returns a DatumWriter for format specified in ResourceSpec, it may or may not
// use underling IOWriter for reading, when the specified format cannot be
// implemented on a specific media, ErrStorageFeatureNotSupported will be returned.
func (rc *ResourceSpec) DatumWriter(ctx context.Context, shard int) (DatumWriter, error) {
	format, ok := storageFormatMap[rc.Format]
	if !ok {
		return nil, ErrUnknownStorageForamt
	}
	return format.DatumWriter(ctx, *rc, shard)
}

// StorageFormat specifies how to read/write datum from underling StorageMedia
// StorageMedia implementation can be globally regsitered by
// RegisterStorageFormat()
type StorageFormat interface {
	DatumReader(ctx context.Context, rc ResourceSpec, shard int) (DatumReader, error)
	DatumWriter(ctx context.Context, rc ResourceSpec, shard int) (DatumWriter, error)
}

// StorageMedia specifies how to read/write bytes from external stroages.
// StorageMedia implementation can be globally regsitered by
// RegisterStorageMedia()
type StorageMedia interface {
	IOReader(ctx context.Context, rc ResourceSpec, shard int) (io.ReadCloser, error)
	IOWriter(ctx context.Context, rc ResourceSpec, shard int) (io.WriteCloser, error)
}

type DatumReader interface {
	// Read next datum, implementation doesn't need to be concurrent safe. caller
	// is expected to not further call it once an error is received.
	ReadDatum() (saw.Datum, error)
	Close() error
}

type DatumWriter interface {
	// Write a datum, implementation doesn't need to be concurrent safe. caller
	// is expected to not further call it once an error is received.
	WriteDatum(datum saw.Datum) error
	Close() error
}

var (
	storageFormatMap = make(map[string]StorageFormat)
	storageMediaMap  = make(map[string]StorageMedia)
)

// Register a StorageFormat, should be run in init()
func RegisterStorageFormat(name string, format StorageFormat) {
	if _, ok := storageMediaMap[name]; ok {
		panic("duplicated storage format " + name)
	}
	storageFormatMap[name] = format
}

// Register a StorageMedia, should be run in init()
func RegisterStorageMedia(name string, media StorageMedia) {
	if _, ok := storageMediaMap[name]; ok {
		panic("duplicated storage media " + name)
	}
	storageMediaMap[name] = media
}

var resourcePathPattern = regexp.MustCompile("^([^\\s]+)\\:([^@\\s]+)(@\\d+)?$")

// A resource path has the format: format:{path}{@numShards}?
// format and media should already be registered.
// If path is started by '/', parser tries to map its first section to a media,
// fallsaback to local FS if nothing specified --- do not name your media after
// well known UNIX root dir, or it would cause confusion.
func ParseResourcePath(path string) (ResourceSpec, error) {
	m := resourcePathPattern.FindAllStringSubmatch(path, 1)
	if len(m) != 1 {
		return ResourceSpec{}, ErrMalformedPath
	}
	rc := ResourceSpec{Format: m[0][1]}

	rc.Path = m[0][2]
	rc.Media = localMediaName
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
		rc.NumShards, err = strconv.Atoi(m[0][3][1:])
		if err != nil {
			return ResourceSpec{}, ErrMalformedPath
		}
	}
	return rc, nil
}

// Parse or die.
func MustParseResourcePath(path string) ResourceSpec {
	spec, err := ParseResourcePath(path)
	if err != nil {
		panic(err)
	}
	return spec
}
