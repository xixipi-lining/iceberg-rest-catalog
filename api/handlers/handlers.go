package handlers

import (
	"net/http"

	"github.com/apache/iceberg-go/catalog"
	"github.com/gin-gonic/gin"
)

type CatalogHandler struct {
	catalog catalog.Catalog
}

func (h *CatalogHandler) ListNamespaces(c *gin.Context) {
	h.catalog.ListNamespaces(c.Request.Context(), nil)
	c.JSON(http.StatusOK, gin.H{
		"namespaces": []string{},
	})
}

func (h *CatalogHandler) CreateNamespace(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) LoadNamespaceMetadata(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}	

func (h *CatalogHandler) NamespaceExists(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) DropNamespace(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) UpdateProperties(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) ListTables(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented"})
}

func (h *CatalogHandler) CreateTable(c *gin.Context) {
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
