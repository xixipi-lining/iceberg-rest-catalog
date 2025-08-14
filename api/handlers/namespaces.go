package handlers

import (
	"errors"
	"net/http"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/gin-gonic/gin"
)

func (h *CatalogHandler) ListNamespaces(c *gin.Context) {
	log := getLogger(c)

	var req ListNamespacesRequest
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
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		log.Errorf("failed to list namespaces: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	resp := ListNamespacesResponse{
		Namespaces: namespaces,
	}
	c.JSON(http.StatusOK, resp)
}

func (h *CatalogHandler) CreateNamespace(c *gin.Context) {
	log := getLogger(c)

	var req CreateNamespaceRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	err := h.catalog.CreateNamespace(c.Request.Context(), req.Namespace, req.Properties)
	if err != nil {
		if errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
			c.JSON(http.StatusConflict, ErrorResponse{
				Error: ErrNamespaceAlreadyExists,
			})
			return
		}
		log.Errorf("failed to create namespace: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	c.JSON(http.StatusOK, CreateNamespaceResponse(req))
}

func (h *CatalogHandler) LoadNamespaceMetadata(c *gin.Context) {
	log := getLogger(c)

	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)
	properties, err := h.catalog.LoadNamespaceProperties(c.Request.Context(), namespace)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		log.Errorf("failed to load namespace metadata: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	c.JSON(http.StatusOK, LoadNamespaceMetadataResponse{
		Namespace:  namespace,
		Properties: properties,
	})
}

func (h *CatalogHandler) NamespaceExists(c *gin.Context) {
	log := getLogger(c)
	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)
	exists, err := h.catalog.CheckNamespaceExists(c.Request.Context(), namespace)
	if err != nil {
		log.Errorf("failed to check namespace exists: %s", err)
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
	log := getLogger(c)
	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)
	err := h.catalog.DropNamespace(c.Request.Context(), namespace)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
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
		log.Errorf("failed to drop namespace: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *CatalogHandler) UpdateProperties(c *gin.Context) {
	log := getLogger(c)

	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)

	var req UpdatePropertiesRequest
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

	summary, err := h.catalog.UpdateNamespaceProperties(c.Request.Context(), namespace, req.Removals, req.Updates)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		log.Errorf("failed to update namespace properties: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	c.JSON(http.StatusOK, UpdatePropertiesResponse{
		Updated: summary.Updated,
		Removed: summary.Removed,
		Missing: summary.Missing,
	})
}
