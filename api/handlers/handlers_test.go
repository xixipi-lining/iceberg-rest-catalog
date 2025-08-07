package handlers_test

import (
	"context"
	"iter"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	icetbl "github.com/apache/iceberg-go/table"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/handlers"
)

type MockCatalog struct {
	mock.Mock
}

func (m *MockCatalog) CatalogType() icecat.Type {
	return icecat.Type("mock")
}

func (m *MockCatalog) CreateNamespace(ctx context.Context, namespace icetbl.Identifier, properties iceberg.Properties) error {
	args := m.Called(ctx, namespace, properties)
	return args.Error(0)
}

func (m *MockCatalog) LoadNamespaceProperties(ctx context.Context, namespace icetbl.Identifier) (iceberg.Properties, error) {
	args := m.Called(ctx, namespace)
	return args.Get(0).(iceberg.Properties), args.Error(1)
}

func (m *MockCatalog) CheckNamespaceExists(ctx context.Context, namespace []string) (bool, error) {
	args := m.Called(ctx, namespace)
	return args.Get(0).(bool), args.Error(1)
}

func (m *MockCatalog) DropNamespace(ctx context.Context, namespace []string) error {
	args := m.Called(ctx, namespace)
	return args.Error(0)
}

func (m *MockCatalog) UpdateNamespaceProperties(ctx context.Context, namespace icetbl.Identifier, removals []string, updates iceberg.Properties) (icecat.PropertiesUpdateSummary, error) {
	args := m.Called(ctx, namespace, removals, updates)
	return args.Get(0).(icecat.PropertiesUpdateSummary), args.Error(1)
}

func (m *MockCatalog) ListNamespaces(ctx context.Context, parent icetbl.Identifier) ([]icetbl.Identifier, error) {
	args := m.Called(ctx, parent)
	return args.Get(0).([]icetbl.Identifier), args.Error(1)
}

func (m *MockCatalog) CheckTableExists(ctx context.Context, namespace []string) (bool, error) {
	args := m.Called(ctx, namespace)
	return args.Get(0).(bool), args.Error(1)
}

func (m *MockCatalog) CreateTable(ctx context.Context, identifier icetbl.Identifier, schema *iceberg.Schema, opts ...icecat.CreateTableOpt) (*icetbl.Table, error) {
	args := m.Called(ctx, identifier, schema, opts)
	return args.Get(0).(*icetbl.Table), args.Error(1)
}

func (m *MockCatalog) ListTables(ctx context.Context, namespace icetbl.Identifier) iter.Seq2[icetbl.Identifier, error] {
	args := m.Called(ctx, namespace)
	return args.Get(0).(iter.Seq2[icetbl.Identifier, error])
}

func (m *MockCatalog) CommitTable(ctx context.Context, table *icetbl.Table, requirements []icetbl.Requirement, updates []icetbl.Update) (icetbl.Metadata, string, error) {
	args := m.Called(ctx, table, requirements, updates)
	return args.Get(0).(icetbl.Metadata), args.Get(1).(string), args.Error(2)
}

func (m *MockCatalog) LoadTable(ctx context.Context, identifier icetbl.Identifier, props iceberg.Properties) (*icetbl.Table, error) {
	args := m.Called(ctx, identifier, props)
	return args.Get(0).(*icetbl.Table), args.Error(1)
}

func (m *MockCatalog) DropTable(ctx context.Context, identifier icetbl.Identifier) error {
	args := m.Called(ctx, identifier)
	return args.Error(0)
}

func (m *MockCatalog) RenameTable(ctx context.Context, from, to icetbl.Identifier) (*icetbl.Table, error) {
	args := m.Called(ctx, from, to)
	return args.Get(0).(*icetbl.Table), args.Error(1)
}

func setupRouter(catalog icecat.Catalog) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.Default()
	handler := handlers.NewCatalogHandler(catalog, handlers.Config{})
	router.GET("/v1/:prefix/namespaces", handler.ListNamespaces)
	return router
}

func TestListNamespaces(t *testing.T) {
	mockCatalog := new(MockCatalog)

	expectedNamespaces := [][]string{
		{"accounting", "tax", "2023"},
		{"accounting", "tax", "2024"},
		{"accounting", "expenses"},
	}

	mockCatalog.On("ListNamespacesPaginated",
		mock.Anything,
		[]string{"accounting", "tax"},
		(*string)(nil),
		(*int)(nil),
	).Return(expectedNamespaces, (*string)(nil), nil)

	router := setupRouter(mockCatalog)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/accounting/namespaces?parent=accounting%1Ftax", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// 验证mock是否被正确调用
	mockCatalog.AssertExpectations(t)
}

func TestListNamespacesWithError(t *testing.T) {
	mockCatalog := new(MockCatalog)

	mockCatalog.On("ListNamespacesPaginated",
		mock.Anything,
		[]string{"accounting", "tax"},
		(*string)(nil),
		(*int)(nil),
	).Return([][]string(nil), (*string)(nil), icecat.ErrNoSuchNamespace)

	router := setupRouter(mockCatalog)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/accounting/namespaces?parent=accounting%1Ftax", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)

	mockCatalog.AssertExpectations(t)
}

func TestListNamespacesWithPagination(t *testing.T) {
	mockCatalog := new(MockCatalog)

	expectedNamespaces := [][]string{
		{"accounting", "tax", "2023"},
	}
	nextPageToken := "next-page-token"

	mockCatalog.On("ListNamespacesPaginated",
		mock.Anything,
		[]string{"accounting", "tax"},
		&nextPageToken,
		(*int)(nil),
	).Return(expectedNamespaces, &nextPageToken, nil)

	router := setupRouter(mockCatalog)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/v1/accounting/namespaces?parent=accounting%1Ftax&pageToken=next-page-token", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	mockCatalog.AssertExpectations(t)
}
