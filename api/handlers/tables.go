package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/gin-gonic/gin"
	"github.com/xixipi-lining/iceberg-rest-catalog/logger"
)

type Config struct {
	Defaults  map[string]string `json:"defaults" yaml:"defaults"`
	Overrides map[string]string `json:"overrides" yaml:"overrides"`
}

type CatalogHandler struct {
	config  Config
	catalog catalog.TransactionCatalog
	followers []catalog.FollowerCatalog
}

func getLogger(c *gin.Context) logger.Logger {
	log, ok := c.Get("logger")
	if !ok {
		return logger.NewLogger(&logger.Config{
			Debug: true,
		})
	}
	return log.(logger.Logger)
}

func NewCatalogHandler(config Config, catalog catalog.TransactionCatalog, followers ...catalog.FollowerCatalog) *CatalogHandler {
	return &CatalogHandler{catalog: catalog, config: config, followers: followers}
}

func (h *CatalogHandler) GetConfig(c *gin.Context) {
	log := getLogger(c)

	if _, hasWarehouse := c.GetQuery("warehouse"); hasWarehouse {
		log.Warn("warehouse query parameter is not supported")
	}

	c.JSON(http.StatusOK, h.config)
}

func (h *CatalogHandler) ListTables(c *gin.Context) {
	log := getLogger(c)

	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)

	var resTables []Identifier
	for table, err := range h.catalog.ListTables(c.Request.Context(), namespace) {
		if err != nil {
			log.Errorf("failed to list tables: %s", err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Error: ErrInternalServerError,
			})
			return
		}
		// Add boundary check to prevent panic
		if len(table) == 0 {
			log.Warn("received empty table identifier")
			continue
		}
		resTables = append(resTables, Identifier{
			Namespace: table[:len(table)-1],
			Name:      table[len(table)-1],
		})
	}

	c.JSON(http.StatusOK, ListTablesResponse{
		Identifiers: resTables,
	})
}

func (h *CatalogHandler) CreateTable(c *gin.Context) {
	log := getLogger(c)

	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)

	var req CreateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	if req.StageCreate {
		c.JSON(http.StatusNotImplemented, ErrorResponse{
			Error: ErrNotImplemented,
		})
		return
	}

	var opts []catalog.CreateTableOpt
	if req.Location != "" {
		opts = append(opts, catalog.WithLocation(req.Location))
	}
	if req.PartitionSpec != nil {
		opts = append(opts, catalog.WithPartitionSpec(req.PartitionSpec))
	}
	if req.WriteOrder != nil {
		opts = append(opts, catalog.WithSortOrder(*req.WriteOrder))
	}
	if req.Props != nil {
		opts = append(opts, catalog.WithProperties(req.Props))
	}

	table, err := h.catalog.CreateTable(c.Request.Context(), append(namespace, req.Name), req.Schema, opts...)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, catalog.ErrTableAlreadyExists) {
			c.JSON(http.StatusConflict, ErrorResponse{
				Error: ErrTableAlreadyExists,
			})
			return
		}
		log.Errorf("failed to create table: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	metadata, err := json.Marshal(table.Metadata())
	if err != nil {
		log.Errorf("failed to marshal metadata: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	resp := LoadTableResponse{
		MetadataLoc: table.MetadataLocation(),
		Metadata:    metadata,
		Config:      table.Properties(),
	}

	c.JSON(http.StatusOK, resp)
}

func (h *CatalogHandler) UpdateTable(c *gin.Context) {
	log := getLogger(c)

	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)

	tableName := c.Param("table")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	var req UpdateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	table, err := h.catalog.LoadTable(c.Request.Context(), append(namespace, tableName), nil)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, catalog.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		log.Errorf("failed to load table: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	metadata, metadataLoc, err := h.catalog.CommitTable(c.Request.Context(), table, req.Requirements, req.Updates)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, catalog.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		log.Errorf("failed to commit table: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		log.Errorf("failed to marshal metadata: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}
	resp := UpdateTableResponse{
		MetadataLoc: metadataLoc,
		Metadata:    metadataBytes,
	}

	c.JSON(http.StatusOK, resp)
}

func (h *CatalogHandler) LoadTable(c *gin.Context) {
	log := getLogger(c)

	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)

	tableName := c.Param("table")

	table, err := h.catalog.LoadTable(c.Request.Context(), append(namespace, tableName), nil)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, catalog.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		log.Errorf("failed to load table: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	metadata, err := json.Marshal(table.Metadata())
	if err != nil {
		log.Errorf("failed to marshal metadata: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	resp := LoadTableResponse{
		MetadataLoc: table.MetadataLocation(),
		Metadata:    metadata,
		Config:      table.Properties(),
	}

	c.JSON(http.StatusOK, resp)
}

func (h *CatalogHandler) DropTable(c *gin.Context) {
	log := getLogger(c)

	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)

	tableName := c.Param("table")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	purgeRequested := c.Query("purgeRequested")
	if purgeRequested == "true" {
		log.Warn("purgeRequested query parameter is not supported")
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrNotImplemented,
		})
		return
	}

	err := h.catalog.DropTable(c.Request.Context(), append(namespace, tableName))
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, catalog.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		log.Errorf("failed to drop table: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *CatalogHandler) TableExists(c *gin.Context) {
	log := getLogger(c)

	namespace := strings.Split(c.Param("namespace"), namespaceSeparator)

	tableName := c.Param("table")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	exists, err := h.catalog.CheckTableExists(c.Request.Context(), append(namespace, tableName))
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, catalog.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		log.Errorf("failed to check table exists: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	if !exists {
		c.JSON(http.StatusNotFound, ErrorResponse{
			Error: ErrTableNotFound,
		})
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *CatalogHandler) RenameTable(c *gin.Context) {
	log := getLogger(c)

	var req RenameTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	_, err := h.catalog.RenameTable(
		c.Request.Context(),
		append(req.Source.Namespace, req.Source.Name),
		append(req.Destination.Namespace, req.Destination.Name),
	)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, catalog.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		log.Errorf("failed to rename table: %s", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	c.Status(http.StatusOK)
}
