package test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/handlers"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/router"
)

// setupTestServer creates a test HTTP server
func setupTestServer(t *testing.T) (*httptest.Server, *rest.Catalog) {
	// Create in-memory SQLite catalog as backend
	backendCatalog, err := catalog.Load(context.Background(), "test", iceberg.Properties{
		"type":                "sql",
		"sql.driver":          "sqlite3",
		"sql.dialect":         "sqlite",
		"init_catalog_tables": "true",
		"warehouse":           "/tmp/warehouse",
	})
	require.NoError(t, err)

	txCat, ok := backendCatalog.(catalog.TransactionCatalog)
	if !ok {
		t.Fatal("catalog is not a transaction catalog")
	}

	// Create handler
	config := handlers.Config{
		Defaults:  map[string]string{"warehouse": "/tmp/warehouse"},
		Overrides: map[string]string{},
	}
	handler := handlers.NewCatalogHandler(config, txCat)

	// Setup Gin engine
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(cors.Default())
	engine.Use(gin.Recovery())

	// Setup routes
	router.Setup(engine, handler)

	// Create test server
	server := httptest.NewServer(engine)

	// Create REST client
	restCatalog, err := rest.NewCatalog(context.Background(), "test-client", server.URL)
	require.NoError(t, err)

	return server, restCatalog
}

func TestServerConfig(t *testing.T) {
	server, restCatalog := setupTestServer(t)
	defer server.Close()

	// Test catalog basic properties
	assert.Equal(t, "test-client", restCatalog.Name())
	assert.Equal(t, catalog.REST, restCatalog.CatalogType())
}

func TestNamespaceOperations(t *testing.T) {
	server, restCatalog := setupTestServer(t)
	defer server.Close()

	ctx := context.Background()

	t.Run("CreateNamespace", func(t *testing.T) {
		// Create namespace
		namespace := table.Identifier{"test_namespace"}
		props := iceberg.Properties{
			"description": "Test namespace",
			"owner":       "test_user",
		}

		err := restCatalog.CreateNamespace(ctx, namespace, props)
		require.NoError(t, err)
	})

	t.Run("ListNamespaces", func(t *testing.T) {
		// List namespaces
		namespaces, err := restCatalog.ListNamespaces(ctx, nil)
		require.NoError(t, err)
		assert.Contains(t, namespaces, table.Identifier{"test_namespace"})
	})

	t.Run("CheckNamespaceExists", func(t *testing.T) {
		// Check if namespace exists
		exists, err := restCatalog.CheckNamespaceExists(ctx, table.Identifier{"test_namespace"})
		require.NoError(t, err)
		assert.True(t, exists)

		// Check non-existent namespace
		exists, err = restCatalog.CheckNamespaceExists(ctx, table.Identifier{"non_existent"})
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("LoadNamespaceProperties", func(t *testing.T) {
		// Load namespace properties
		props, err := restCatalog.LoadNamespaceProperties(ctx, table.Identifier{"test_namespace"})
		require.NoError(t, err)
		assert.Equal(t, "Test namespace", props["description"])
		assert.Equal(t, "test_user", props["owner"])
	})

	t.Run("UpdateNamespaceProperties", func(t *testing.T) {
		// Update namespace properties
		updates := iceberg.Properties{
			"description": "Updated test namespace",
			"new_prop":    "new_value",
		}
		removals := []string{"owner"}

		summary, err := restCatalog.UpdateNamespaceProperties(ctx, table.Identifier{"test_namespace"}, removals, updates)
		require.NoError(t, err)
		assert.Contains(t, summary.Updated, "description")
		assert.Contains(t, summary.Updated, "new_prop")
		assert.Contains(t, summary.Removed, "owner")

		// Verify update results
		props, err := restCatalog.LoadNamespaceProperties(ctx, table.Identifier{"test_namespace"})
		require.NoError(t, err)
		assert.Equal(t, "Updated test namespace", props["description"])
		assert.Equal(t, "new_value", props["new_prop"])
		_, exists := props["owner"]
		assert.False(t, exists)
	})

	t.Run("DropNamespace", func(t *testing.T) {
		// Delete namespace (execute after table tests)
		// This test will be executed after TestTableOperations
	})
}

func TestTableOperations(t *testing.T) {
	server, restCatalog := setupTestServer(t)
	defer server.Close()

	ctx := context.Background()

	// First create namespace
	namespace := table.Identifier{"test_namespace"}
	err := restCatalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
	require.NoError(t, err)

	// Create test schema
	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		{ID: 3, Name: "created_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
	}
	schema := iceberg.NewSchema(0, fields...)

	tableIdent := table.Identifier{"test_namespace", "test_table"}

	t.Run("CreateTable", func(t *testing.T) {
		// Create table
		props := iceberg.Properties{
			"description": "Test table",
		}

		tbl, err := restCatalog.CreateTable(ctx, tableIdent, schema,
			catalog.WithProperties(props),
		)
		require.NoError(t, err)
		assert.Equal(t, tableIdent, tbl.Identifier())
		assert.Equal(t, schema.ID, tbl.Schema().ID)
		assert.Equal(t, schema.Fields(), tbl.Schema().Fields())
	})

	t.Run("CheckTableExists", func(t *testing.T) {
		// Check if table exists
		exists, err := restCatalog.CheckTableExists(ctx, tableIdent)
		require.NoError(t, err)
		assert.True(t, exists)

		// Check non-existent table
		exists, err = restCatalog.CheckTableExists(ctx, table.Identifier{"test_namespace", "non_existent"})
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("LoadTable", func(t *testing.T) {
		// Load table
		tbl, err := restCatalog.LoadTable(ctx, tableIdent, nil)
		require.NoError(t, err)
		assert.Equal(t, tableIdent, tbl.Identifier())
		assert.Equal(t, 3, len(tbl.Schema().Fields()))
		assert.Equal(t, schema.Fields(), tbl.Schema().Fields())
	})

	t.Run("ListTables", func(t *testing.T) {
		// List tables
		var tables []table.Identifier
		for tbl, err := range restCatalog.ListTables(ctx, namespace) {
			require.NoError(t, err)
			tables = append(tables, tbl)
		}
		assert.Contains(t, tables, tableIdent)
	})

	t.Run("UpdateTable", func(t *testing.T) {
		// Update table - add new field
		requirements := []table.Requirement{
			table.AssertCurrentSchemaID(0),
		}

		newField := iceberg.NestedField{ID: 4, Name: "updated_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: true}
		updates := []table.Update{
			table.NewAddSchemaUpdate(iceberg.NewSchema(1, append(fields, newField)...)),
			table.NewSetCurrentSchemaUpdate(1),
		}

		updatedTable, err := restCatalog.UpdateTable(ctx, tableIdent, requirements, updates)
		require.NoError(t, err)
		assert.Equal(t, 4, len(updatedTable.Schema().Fields()))
	})

	t.Run("RenameTable", func(t *testing.T) {
		// Rename table
		newIdent := table.Identifier{"test_namespace", "renamed_table"}

		renamedTable, err := restCatalog.RenameTable(ctx, tableIdent, newIdent)
		require.NoError(t, err)
		assert.Equal(t, newIdent, renamedTable.Identifier())

		// Verify original table doesn't exist
		exists, err := restCatalog.CheckTableExists(ctx, tableIdent)
		require.NoError(t, err)
		assert.False(t, exists)

		// Verify new table exists
		exists, err = restCatalog.CheckTableExists(ctx, newIdent)
		require.NoError(t, err)
		assert.True(t, exists)

		// Rename back for subsequent tests
		_, err = restCatalog.RenameTable(ctx, newIdent, tableIdent)
		require.NoError(t, err)
	})

	t.Run("DropTable", func(t *testing.T) {
		// Drop table
		err := restCatalog.DropTable(ctx, tableIdent)
		require.NoError(t, err)

		// Verify table doesn't exist
		exists, err := restCatalog.CheckTableExists(ctx, tableIdent)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestErrorHandling(t *testing.T) {
	server, restCatalog := setupTestServer(t)
	defer server.Close()

	ctx := context.Background()

	t.Run("NonExistentNamespace", func(t *testing.T) {
		// Try to operate on non-existent namespace
		_, err := restCatalog.LoadNamespaceProperties(ctx, table.Identifier{"non_existent"})
		assert.Error(t, err)
		assert.ErrorIs(t, err, catalog.ErrNoSuchNamespace)
	})

	t.Run("NonExistentTable", func(t *testing.T) {
		// First create namespace
		namespace := table.Identifier{"test_namespace"}
		err := restCatalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		require.NoError(t, err)

		// Try to load non-existent table
		_, err = restCatalog.LoadTable(ctx, table.Identifier{"test_namespace", "non_existent"}, nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, catalog.ErrNoSuchTable)
	})

	t.Run("DuplicateNamespace", func(t *testing.T) {
		// Create namespace
		namespace := table.Identifier{"duplicate_test"}
		err := restCatalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		require.NoError(t, err)

		// Try to create same namespace
		err = restCatalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, catalog.ErrNamespaceAlreadyExists)
	})

	t.Run("DuplicateTable", func(t *testing.T) {
		// Create schema and table
		schema := iceberg.NewSchema(1,
			[]iceberg.NestedField{
				{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			}...,
		)

		tableIdent := table.Identifier{"duplicate_test", "test_table"}
		_, err := restCatalog.CreateTable(ctx, tableIdent, schema)
		require.NoError(t, err)

		// Try to create same table
		_, err = restCatalog.CreateTable(ctx, tableIdent, schema)
		assert.Error(t, err)
	})
}

func TestCleanup(t *testing.T) {
	server, restCatalog := setupTestServer(t)
	defer server.Close()

	ctx := context.Background()

	// Clean up all test data
	t.Run("CleanupNamespaces", func(t *testing.T) {
		namespaces, err := restCatalog.ListNamespaces(ctx, nil)
		require.NoError(t, err)

		for _, ns := range namespaces {
			// First delete all tables under namespace
			for tbl, err := range restCatalog.ListTables(ctx, ns) {
				require.NoError(t, err)
				err = restCatalog.DropTable(ctx, tbl)
				require.NoError(t, err)
			}

			// Then delete namespace
			err := restCatalog.DropNamespace(ctx, ns)
			require.NoError(t, err)
		}

		// Verify all namespaces are deleted
		namespaces, err = restCatalog.ListNamespaces(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, namespaces)
	})
}
