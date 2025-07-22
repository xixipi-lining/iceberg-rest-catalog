package catalog

import (
	"context"
	"errors"
)

var ErrNamespaceNotFound = errors.New("namespace not found")
var ErrNamespaceAlreadyExists = errors.New("namespace already exists")
var ErrNamespaceNotEmpty = errors.New("namespace is not empty")

type Catalog interface {
	ListNamespaces(ctx context.Context, parent []string, pageToken *string, pageSize *int) (namespaces [][]string, nextPageToken *string, err error)
	CreateNamespace(ctx context.Context, namespace []string, properties map[string]string) (fullProperties map[string]string, err error)
	LoadNamespaceMetadata(ctx context.Context, namespace []string) (properties map[string]string, err error)
	NamespaceExists(ctx context.Context, namespace []string) (exists bool, err error)
	DropNamespace(ctx context.Context, namespace []string) (err error)
	UpdateProperties(ctx context.Context, namespace []string, removals []string, updates map[string]string) (updated []string, removed []string, missing []string, err error)
	ListTables(ctx context.Context, namespace []string, pageToken *string, pageSize *int) (tables [][]string, nextPageToken *string, err error)
}
