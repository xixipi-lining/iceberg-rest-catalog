package handlers

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/xixipi-lining/iceberg-rest-catalog/service/catalog"
)

const namespaceSeparator = "\x1F"

type ErrorResponse struct {
	Error ErrorModel `json:"error"`
}

type ErrorModel struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`
}

var ErrInternalServerError = ErrorModel{
	Message: "Internal Server Error",
	Type:    "InternalServerError",
	Code:    http.StatusInternalServerError,
}

var ErrBadRequest = ErrorModel{
	Message: "Malformed request",
	Type:    "BadRequestException",
	Code:    http.StatusBadRequest,
}

var ErrNamespaceNotFound = ErrorModel{
	Message: "The given namespace does not exist",
	Type:    "NoSuchNamespaceException",
	Code:    http.StatusNotFound,
}

var ErrNamespaceAlreadyExists = ErrorModel{
	Message: "The given namespace already exists",
	Type:    "AlreadyExistsException",
	Code:    http.StatusConflict,
}

var ErrNamespaceNotEmpty = ErrorModel{
	Message: "The given namespace is not empty",
	Type:    "NamespaceNotEmptyException",
	Code:    http.StatusConflict,
}

var ErrUnprocessableEntityDuplicateKey = ErrorModel{
	Message: "The request cannot be processed as there is a key present multiple times",
	Type:    "UnprocessableEntityException",
	Code:    http.StatusUnprocessableEntity,
}

type CatalogHandler struct {
	catalog catalog.Catalog
}

func NewCatalogHandler(catalog catalog.Catalog) *CatalogHandler {
	return &CatalogHandler{catalog: catalog}
}

func (h *CatalogHandler) ListNamespaces(c *gin.Context) {
	type listNamespacesRequest struct {
		Prefix    string  `uri:"prefix" binding:"required"`
		Parent    *string `form:"parent"`
		PageToken *string `form:"pageToken"`
		PageSize  *int    `form:"pageSize"`
	}

	type listNamespacesResponse struct {
		Namespaces    [][]string `json:"namespaces"`
		NextPageToken *string    `json:"next-page-token"`
	}
	var req listNamespacesRequest
	if err := c.BindUri(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	if err := c.BindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	var parent []string
	if req.Parent != nil {
		parent = strings.Split(*req.Parent, namespaceSeparator)
	}

	namespaces, nextPageToken, err := h.catalog.ListNamespaces(c.Request.Context(), parent, req.PageToken, req.PageSize)
	if err != nil {
		if errors.Is(err, catalog.ErrNamespaceNotFound) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	resp := listNamespacesResponse{
		Namespaces:    namespaces,
		NextPageToken: nextPageToken,
	}
	c.JSON(http.StatusOK, resp)
}

func (h *CatalogHandler) CreateNamespace(c *gin.Context) {
	type createNamespaceRequest struct {
		Prefix     string            `uri:"prefix" binding:"required"`
		Namespace  []string          `json:"namespace" binding:"required"`
		Properties map[string]string `json:"properties"`
	}

	type createNamespaceResponse struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}

	var req createNamespaceRequest
	if err := c.BindUri(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	properties, err := h.catalog.CreateNamespace(c.Request.Context(), req.Namespace, req.Properties)
	if err != nil {
		if errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			c.JSON(http.StatusConflict, ErrorResponse{
				Error: ErrNamespaceAlreadyExists,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	c.JSON(http.StatusOK, createNamespaceResponse{
		Namespace:  req.Namespace,
		Properties: properties,
	})
}

func (h *CatalogHandler) LoadNamespaceMetadata(c *gin.Context) {
	type loadNamespaceMetadataRequest struct {
		Prefix    string `uri:"prefix" binding:"required"`
		Namespace string `uri:"namespace" binding:"required"`
	}

	type loadNamespaceMetadataResponse struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}

	var req loadNamespaceMetadataRequest
	if err := c.BindUri(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	namespace := strings.Split(req.Namespace, namespaceSeparator)
	properties, err := h.catalog.LoadNamespaceMetadata(c.Request.Context(), namespace)
	if err != nil {
		if errors.Is(err, catalog.ErrNamespaceNotFound) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	c.JSON(http.StatusOK, loadNamespaceMetadataResponse{
		Namespace:  namespace,
		Properties: properties,
	})
}

func (h *CatalogHandler) NamespaceExists(c *gin.Context) {
	type namespaceExistsRequest struct {
		Prefix    string `uri:"prefix" binding:"required"`
		Namespace string `uri:"namespace" binding:"required"`
	}

	var req namespaceExistsRequest
	if err := c.BindUri(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	namespace := strings.Split(req.Namespace, namespaceSeparator)
	exists, err := h.catalog.NamespaceExists(c.Request.Context(), namespace)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	if !exists {
		c.JSON(http.StatusNotFound, ErrorResponse{
			Error: ErrNamespaceNotFound,
		})
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *CatalogHandler) DropNamespace(c *gin.Context) {
	type dropNamespaceRequest struct {
		Prefix    string `uri:"prefix" binding:"required"`
		Namespace string `uri:"namespace" binding:"required"`
	}

	var req dropNamespaceRequest
	if err := c.BindUri(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
	}
	namespace := strings.Split(req.Namespace, namespaceSeparator)
	err := h.catalog.DropNamespace(c.Request.Context(), namespace)
	if err != nil {
		if errors.Is(err, catalog.ErrNamespaceNotFound) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, catalog.ErrNamespaceNotEmpty) {
			c.JSON(http.StatusConflict, ErrorResponse{
				Error: ErrNamespaceNotEmpty,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *CatalogHandler) UpdateProperties(c *gin.Context) {
	type updatePropertiesRequest struct {
		Prefix    string            `uri:"prefix" binding:"required"`
		Namespace string            `uri:"namespace" binding:"required"`
		Removals  []string          `json:"removals"`
		Updates   map[string]string `json:"updates"`
	}

	type updatePropertiesResponse struct {
		Updated []string `json:"updated"`
		Removed []string `json:"removed"`
		Missing []string `json:"missing"`
	}

	var req updatePropertiesRequest
	if err := c.BindUri(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	for _, removal := range req.Removals {
		if _, exists := req.Updates[removal]; exists {
			c.JSON(http.StatusUnprocessableEntity, ErrorResponse{
				Error: ErrUnprocessableEntityDuplicateKey,
			})
			return
		}
	}

	namespace := strings.Split(req.Namespace, namespaceSeparator)
	updated, removed, missing, err := h.catalog.UpdateProperties(c.Request.Context(), namespace, req.Removals, req.Updates)
	if err != nil {
		if errors.Is(err, catalog.ErrNamespaceNotFound) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	c.JSON(http.StatusOK, updatePropertiesResponse{
		Updated: updated,
		Removed: removed,
		Missing: missing,
	})
}

func (h *CatalogHandler) ListTables(c *gin.Context) {
	type listTablesRequest struct {
		Prefix    string `uri:"prefix" binding:"required"`
		Namespace string `uri:"namespace" binding:"required"`
		PageToken *string `form:"pageToken"`
		PageSize  *int    `form:"pageSize"`
	}

	type listTablesResponse struct {
		Identifiers   []struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifiers"`
		NextPageToken *string    `json:"next-page-token"`
	}

	var req listTablesRequest
	if err := c.BindUri(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	if err := c.BindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	namespace := strings.Split(req.Namespace, namespaceSeparator)
	tables, nextPageToken, err := h.catalog.ListTables(c.Request.Context(), namespace, req.PageToken, req.PageSize)
	if err != nil {
		if errors.Is(err, catalog.ErrNamespaceNotFound) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	identifiers := make([]struct {
		Namespace []string `json:"namespace"`
		Name      string   `json:"name"`
	}, len(tables))
	for i, table := range tables {
		identifiers[i] = struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		}{
			Namespace: table[:len(table)-1],
			Name:      table[len(table)-1],
		}
	}
	c.JSON(http.StatusOK, listTablesResponse{
		Identifiers:   identifiers,
		NextPageToken: nextPageToken,
	})
}

func (h *CatalogHandler) CreateTable(c *gin.Context) {
	type createTableRequest struct {
		Prefix    string `uri:"prefix" binding:"required"`
		Namespace string `uri:"namespace" binding:"required"`
		Table     string `uri:"table" binding:"required"`
		StageCreate bool `json:"stage-create"`
		TableProperties map[string]string `json:"table-properties"`
	}

	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) LoadTable(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) UpdateTable(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) DropTable(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) TableExists(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) RenameTable(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}
