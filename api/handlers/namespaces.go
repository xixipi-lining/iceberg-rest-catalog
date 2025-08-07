package handlers

import (
	"errors"
	"net/http"
	"strings"

	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/gin-gonic/gin"
)

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

	namespaces, err := h.catalog.ListNamespaces(c.Request.Context(), parent)
	if err != nil {
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
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
		Namespaces: namespaces,
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
	err := h.catalog.CreateNamespace(c.Request.Context(), req.Namespace, req.Properties)
	if err != nil {
		if errors.Is(err, icecat.ErrNamespaceAlreadyExists) {
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
		Properties: req.Properties,
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
	properties, err := h.catalog.LoadNamespaceProperties(c.Request.Context(), namespace)
	if err != nil {
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
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
	exists, err := h.catalog.CheckNamespaceExists(c.Request.Context(), namespace)
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
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, icecat.ErrNamespaceNotEmpty) {
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
	summary, err := h.catalog.UpdateNamespaceProperties(c.Request.Context(), namespace, req.Removals, req.Updates)
	if err != nil {
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
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
		Updated: summary.Updated,
		Removed: summary.Removed,
		Missing: summary.Missing,
	})
}
