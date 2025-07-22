package catalog

import (
	"context"
	"errors"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

var ErrNamespaceNotFound = errors.New("namespace not found")
var ErrNamespaceAlreadyExists = errors.New("namespace already exists")
var ErrNamespaceNotEmpty = errors.New("namespace is not empty")

type Catalog interface {
	// CreateTable creates a new iceberg table in the catalog using the provided identifier
	// and schema. Options can be used to optionally provide location, partition spec, sort order,
	// and custom properties.
	CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error)
	// // CommitTable commits the table metadata and updates to the catalog, returning the new metadata
	// CommitTable(context.Context, *table.Table, []table.Requirement, []table.Update) (table.Metadata, string, error)
	// ListTables returns a list of table identifiers in the catalog, with the returned
	// identifiers containing the information required to load the table via that catalog.
	ListTablesPaginated(ctx context.Context, namespace table.Identifier, pageToken *string, pageSize *int) (tables []table.Identifier, nextPageToken *string, err error)
	// // LoadTable loads a table from the catalog and returns a Table with the metadata.
	// LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error)
	// // DropTable tells the catalog to drop the table entirely.
	// DropTable(ctx context.Context, identifier table.Identifier) error
	// // RenameTable tells the catalog to rename a given table by the identifiers
	// // provided, and then loads and returns the destination table
	// RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error)
	// // CheckTableExists returns if the table exists
	// CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error)
	// ListNamespaces returns the list of available namespaces, optionally filtering by a
	// parent namespace
	ListNamespacesPaginated(ctx context.Context, parent table.Identifier, pageToken *string, pageSize *int) (namespaces []table.Identifier, nextPageToken *string, err error)
	// CreateNamespace tells the catalog to create a new namespace with the given properties
	CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error
	// DropNamespace tells the catalog to drop the namespace and all tables in that namespace
	DropNamespace(ctx context.Context, namespace table.Identifier) error
	// CheckNamespaceExists returns if the namespace exists
	CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error)
	// LoadNamespaceProperties returns the current properties in the catalog for
	// a given namespace
	LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error)
	// UpdateNamespaceProperties allows removing, adding, and/or updating properties of a namespace
	UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error)

}
