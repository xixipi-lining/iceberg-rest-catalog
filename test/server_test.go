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

// setupTestServer 创建一个测试HTTP服务器
func setupTestServer(t *testing.T) (*httptest.Server, *rest.Catalog) {
	// 创建内存SQLite catalog作为后端
	backendCatalog, err := catalog.Load(context.Background(), "test", iceberg.Properties{
		"type":                "sql",
		"uri":                 ":memory:",
		"sql.driver":          "sqlite3",
		"sql.dialect":         "sqlite",
		"init_catalog_tables": "true",
		"warehouse":           "/tmp/warehouse",
	})
	require.NoError(t, err)

	// 创建handler
	config := handlers.Config{
		Defaults:  map[string]string{"warehouse": "/tmp/warehouse"},
		Overrides: map[string]string{},
	}
	handler := handlers.NewCatalogHandler(backendCatalog, config)

	// 设置Gin引擎
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(cors.Default())
	engine.Use(gin.Recovery())

	// 设置路由
	router.Setup(engine, handler)

	// 创建测试服务器
	server := httptest.NewServer(engine)

	// 创建REST客户端
	restCatalog, err := rest.NewCatalog(context.Background(), "test-client", server.URL)
	require.NoError(t, err)

	return server, restCatalog
}

func TestServerConfig(t *testing.T) {
	server, restCatalog := setupTestServer(t)
	defer server.Close()

	// 测试catalog基本属性
	assert.Equal(t, "test-client", restCatalog.Name())
	assert.Equal(t, catalog.REST, restCatalog.CatalogType())
}

func TestNamespaceOperations(t *testing.T) {
	server, restCatalog := setupTestServer(t)
	defer server.Close()

	ctx := context.Background()

	t.Run("CreateNamespace", func(t *testing.T) {
		// 创建namespace
		namespace := table.Identifier{"test_namespace"}
		props := iceberg.Properties{
			"description": "Test namespace",
			"owner":       "test_user",
		}

		err := restCatalog.CreateNamespace(ctx, namespace, props)
		require.NoError(t, err)
	})

	t.Run("ListNamespaces", func(t *testing.T) {
		// 列出namespaces
		namespaces, err := restCatalog.ListNamespaces(ctx, nil)
		require.NoError(t, err)
		assert.Contains(t, namespaces, table.Identifier{"test_namespace"})
	})

	t.Run("CheckNamespaceExists", func(t *testing.T) {
		// 检查namespace是否存在
		exists, err := restCatalog.CheckNamespaceExists(ctx, table.Identifier{"test_namespace"})
		require.NoError(t, err)
		assert.True(t, exists)

		// 检查不存在的namespace
		exists, err = restCatalog.CheckNamespaceExists(ctx, table.Identifier{"non_existent"})
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("LoadNamespaceProperties", func(t *testing.T) {
		// 加载namespace属性
		props, err := restCatalog.LoadNamespaceProperties(ctx, table.Identifier{"test_namespace"})
		require.NoError(t, err)
		assert.Equal(t, "Test namespace", props["description"])
		assert.Equal(t, "test_user", props["owner"])
	})

	t.Run("UpdateNamespaceProperties", func(t *testing.T) {
		// 更新namespace属性
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

		// 验证更新结果
		props, err := restCatalog.LoadNamespaceProperties(ctx, table.Identifier{"test_namespace"})
		require.NoError(t, err)
		assert.Equal(t, "Updated test namespace", props["description"])
		assert.Equal(t, "new_value", props["new_prop"])
		_, exists := props["owner"]
		assert.False(t, exists)
	})

	t.Run("DropNamespace", func(t *testing.T) {
		// 删除namespace (在table测试后执行)
		// 这个测试会在TestTableOperations之后执行
	})
}

func TestTableOperations(t *testing.T) {
	server, restCatalog := setupTestServer(t)
	defer server.Close()

	ctx := context.Background()

	// 先创建namespace
	namespace := table.Identifier{"test_namespace"}
	err := restCatalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
	require.NoError(t, err)

	// 创建测试schema
	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		{ID: 3, Name: "created_at", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
	}
	schema := iceberg.NewSchema(0, fields...)

	tableIdent := table.Identifier{"test_namespace", "test_table"}

	t.Run("CreateTable", func(t *testing.T) {
		// 创建table
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
		// 检查table是否存在
		exists, err := restCatalog.CheckTableExists(ctx, tableIdent)
		require.NoError(t, err)
		assert.True(t, exists)

		// 检查不存在的table
		exists, err = restCatalog.CheckTableExists(ctx, table.Identifier{"test_namespace", "non_existent"})
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("LoadTable", func(t *testing.T) {
		// 加载table
		tbl, err := restCatalog.LoadTable(ctx, tableIdent, nil)
		require.NoError(t, err)
		assert.Equal(t, tableIdent, tbl.Identifier())
		assert.Equal(t, 3, len(tbl.Schema().Fields()))
		assert.Equal(t, schema.Fields(), tbl.Schema().Fields())
	})

	t.Run("ListTables", func(t *testing.T) {
		// 列出tables
		var tables []table.Identifier
		for tbl, err := range restCatalog.ListTables(ctx, namespace) {
			require.NoError(t, err)
			tables = append(tables, tbl)
		}
		assert.Contains(t, tables, tableIdent)
	})

	t.Run("UpdateTable", func(t *testing.T) {
		// 更新table - 添加新字段
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
		// 重命名table
		newIdent := table.Identifier{"test_namespace", "renamed_table"}

		renamedTable, err := restCatalog.RenameTable(ctx, tableIdent, newIdent)
		require.NoError(t, err)
		assert.Equal(t, newIdent, renamedTable.Identifier())

		// 验证原table不存在
		exists, err := restCatalog.CheckTableExists(ctx, tableIdent)
		require.NoError(t, err)
		assert.False(t, exists)

		// 验证新table存在
		exists, err = restCatalog.CheckTableExists(ctx, newIdent)
		require.NoError(t, err)
		assert.True(t, exists)

		// 为后续测试重命名回来
		_, err = restCatalog.RenameTable(ctx, newIdent, tableIdent)
		require.NoError(t, err)
	})

	t.Run("DropTable", func(t *testing.T) {
		// 删除table
		err := restCatalog.DropTable(ctx, tableIdent)
		require.NoError(t, err)

		// 验证table不存在
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
		// 尝试操作不存在的namespace
		_, err := restCatalog.LoadNamespaceProperties(ctx, table.Identifier{"non_existent"})
		assert.Error(t, err)
		assert.ErrorIs(t, err, catalog.ErrNoSuchNamespace)
	})

	t.Run("NonExistentTable", func(t *testing.T) {
		// 先创建namespace
		namespace := table.Identifier{"test_namespace"}
		err := restCatalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		require.NoError(t, err)

		// 尝试加载不存在的table
		_, err = restCatalog.LoadTable(ctx, table.Identifier{"test_namespace", "non_existent"}, nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, catalog.ErrNoSuchTable)
	})

	t.Run("DuplicateNamespace", func(t *testing.T) {
		// 创建namespace
		namespace := table.Identifier{"duplicate_test"}
		err := restCatalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		require.NoError(t, err)

		// 尝试创建相同的namespace
		err = restCatalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, catalog.ErrNamespaceAlreadyExists)
	})

	t.Run("DuplicateTable", func(t *testing.T) {
		// 创建schema和table
		schema := iceberg.NewSchema(1,
			[]iceberg.NestedField{
				{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			}...,
		)

		tableIdent := table.Identifier{"duplicate_test", "test_table"}
		_, err := restCatalog.CreateTable(ctx, tableIdent, schema)
		require.NoError(t, err)

		// 尝试创建相同的table
		_, err = restCatalog.CreateTable(ctx, tableIdent, schema)
		assert.Error(t, err)
	})
}

func TestCleanup(t *testing.T) {
	server, restCatalog := setupTestServer(t)
	defer server.Close()

	ctx := context.Background()

	// 清理所有测试数据
	t.Run("CleanupNamespaces", func(t *testing.T) {
		namespaces, err := restCatalog.ListNamespaces(ctx, nil)
		require.NoError(t, err)

		for _, ns := range namespaces {
			// 先删除namespace下的所有tables
			for tbl, err := range restCatalog.ListTables(ctx, ns) {
				require.NoError(t, err)
				err = restCatalog.DropTable(ctx, tbl)
				require.NoError(t, err)
			}

			// 然后删除namespace
			err := restCatalog.DropNamespace(ctx, ns)
			require.NoError(t, err)
		}

		// 验证所有namespace都被删除
		namespaces, err = restCatalog.ListNamespaces(ctx, nil)
		require.NoError(t, err)
		assert.Empty(t, namespaces)
	})
}
