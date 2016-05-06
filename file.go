package saw

import (
	"errors"
	"io"
	"strings"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	gcs "google.golang.org/api/storage/v1"
)

var malformedPathErr = errors.New("malformed path")

func OpenGCSFile(ctx context.Context, path string) (io.ReadCloser, error) {
	if len(path) == 0 || path[0] != '/' {
		return nil, malformedPathErr
	}
	pair := strings.SplitN(path[1:], "/", 2)
	if len(pair) != 2 {
		return nil, malformedPathErr
	}
	cli, err := google.DefaultClient(ctx, gcs.DevstorageReadOnlyScope)
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
