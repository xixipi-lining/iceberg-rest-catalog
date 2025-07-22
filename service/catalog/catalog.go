package catalog

import (
	"context"
	"errors"
)

var ErrNamespaceNotFound = errors.New("namespace not found")
var ErrNamespaceAlreadyExists = errors.New("namespace already exists")

type Catalog interface {
	ListNamespaces(ctx context.Context, parent []string, pageToken *string, pageSize *int) (namespaces [][]string, nextPageToken *string, err error)
	CreateNamespace(ctx context.Context, namespace []string, properties map[string]string) (fullProperties map[string]string, err error)
	LoadNamespaceMetadata(ctx context.Context, namespace []string) (properties map[string]string, err error)
}
