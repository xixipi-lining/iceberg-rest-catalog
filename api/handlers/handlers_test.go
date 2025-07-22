package handlers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/handlers"
	"github.com/xixipi-lining/iceberg-rest-catalog/service/catalog"
)

type MockCatalog struct {
	mock.Mock
}

func (m *MockCatalog) ListNamespaces(ctx context.Context, parent []string, pageToken *string, pageSize *int) ([][]string, *string, error) {
	args := m.Called(ctx, parent, pageToken, pageSize)
	return args.Get(0).([][]string), args.Get(1).(*string), args.Error(2)
}

func (m *MockCatalog) CreateNamespace(ctx context.Context, namespace []string, properties map[string]string) (map[string]string, error) {
	args := m.Called(ctx, namespace, properties)
	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *MockCatalog) LoadNamespaceMetadata(ctx context.Context, namespace []string) (map[string]string, error) {
	args := m.Called(ctx, namespace)
	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *MockCatalog) NamespaceExists(ctx context.Context, namespace []string) (bool, error) {
	args := m.Called(ctx, namespace)
	return args.Get(0).(bool), args.Error(1)
}

func (m *MockCatalog) DropNamespace(ctx context.Context, namespace []string) error {
	args := m.Called(ctx, namespace)
	return args.Error(0)
}

func (m *MockCatalog) UpdateProperties(ctx context.Context, namespace []string, removals []string, updates map[string]string) ([]string, []string, []string, error) {
	args := m.Called(ctx, namespace, removals, updates)
	return args.Get(0).([]string), args.Get(1).([]string), args.Get(2).([]string), args.Error(3)
}

func (m *MockCatalog) ListTables(ctx context.Context, namespace []string, pageToken *string, pageSize *int) ([][]string, *string, error) {
	args := m.Called(ctx, namespace, pageToken, pageSize)
	return args.Get(0).([][]string), args.Get(1).(*string), args.Error(2)
}

func setupRouter(catalog catalog.Catalog) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.Default()
	handler := handlers.NewCatalogHandler(catalog)
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

	mockCatalog.On("ListNamespaces",
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

	mockCatalog.On("ListNamespaces",
		mock.Anything,
		[]string{"accounting", "tax"},
		(*string)(nil),
		(*int)(nil),
	).Return([][]string(nil), (*string)(nil), catalog.ErrNamespaceNotFound)

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

	mockCatalog.On("ListNamespaces",
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
