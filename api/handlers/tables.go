package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	icecat "github.com/xixipi-lining/iceberg-go/catalog"
	"github.com/xixipi-lining/iceberg-rest-catalog/logger"
	"github.com/xixipi-lining/iceberg-rest-catalog/service/catalog"
)

type Config struct {
	Defaults  map[string]string `json:"defaults"`
	Overrides map[string]string `json:"overrides"`
}

type CatalogHandler struct {
	config  Config
	catalog catalog.Catalog
}

func NewCatalogHandler(catalog catalog.Catalog) *CatalogHandler {
	return &CatalogHandler{catalog: catalog}
}

func (h *CatalogHandler) GetConfig(c *gin.Context) {
	log := c.MustGet("logger").(logger.Logger)

	if _, hasWarehouse := c.GetQuery("warehouse"); hasWarehouse {
		log.Warn("warehouse query parameter is not supported")
	}

	c.JSON(http.StatusOK, h.config)
}

func (h *CatalogHandler) ListTables(c *gin.Context) {
	log := c.MustGet("logger").(logger.Logger)

	prefix := c.Param("prefix")
	if prefix != "" {
		log.Warn("prefix query parameter is not supported")
	}

	namespace := c.Param("namespace")
	ns := strings.Split(namespace, namespaceSeparator)

	var resTables []Identifier
	pageToken, hasPageToken := c.GetQuery("pageToken")
	if !hasPageToken {
		pageToken = ""
		pageSize := 100
		for {
			tables, nextPageToken, err := h.catalog.ListTablesPaginated(c.Request.Context(), ns, &pageToken, &pageSize)
			if err != nil {
				log.Errorf("failed to list tables: %w", err)
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Error: ErrInternalServerError,
				})
			}
			for _, table := range tables {
				resTables = append(resTables, Identifier{
					Namespace: table[:len(table)-1],
					Name:      table[len(table)-1],
				})
			}
			if nextPageToken == nil {
				break
			}
			pageToken = *nextPageToken
		}
		c.JSON(http.StatusOK, ListTablesResponse{
			Identifiers: resTables,
		})
		return
	}

	pageSize := c.Query("pageSize")
	if pageSize == "" {
		pageSize = "10"
	}
	pageSizeInt, err := strconv.Atoi(pageSize)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	tables, nextPageToken, err := h.catalog.ListTablesPaginated(c.Request.Context(), ns, &pageToken, &pageSizeInt)
	if err != nil {
		log.Errorf("failed to list tables: %w", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	for _, table := range tables {
		resTables = append(resTables, Identifier{
			Namespace: table[:len(table)-1],
			Name:      table[len(table)-1],
		})
	}

	c.JSON(http.StatusOK, ListTablesResponse{
		Identifiers:   resTables,
		NextPageToken: nextPageToken,
	})
}

func (h *CatalogHandler) CreateTable(c *gin.Context) {
	log := c.MustGet("logger").(logger.Logger)

	prefix := c.Param("prefix")
	if prefix != "" {
		log.Warn("prefix query parameter is not supported")
	}

	namespace := c.Param("namespace")
	ns := strings.Split(namespace, namespaceSeparator)

	var req CreateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	if req.StageCreate {
		log.Warn("stage-create is not supported")
		c.JSON(http.StatusNotImplemented, ErrorResponse{
			Error: ErrNotImplemented,
		})
		return
	}

	var opts []icecat.CreateTableOpt
	if req.Location != "" {
		opts = append(opts, icecat.WithLocation(req.Location))
	}
	if req.PartitionSpec != nil {
		opts = append(opts, icecat.WithPartitionSpec(req.PartitionSpec))
	}
	if req.WriteOrder != nil {
		opts = append(opts, icecat.WithSortOrder(*req.WriteOrder))
	}
	if req.Props != nil {
		opts = append(opts, icecat.WithProperties(req.Props))
	}

	table, err := h.catalog.CreateTable(c.Request.Context(), append(ns, req.Name), req.Schema, opts...)
	if err != nil {
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, icecat.ErrTableAlreadyExists) {
			c.JSON(http.StatusConflict, ErrorResponse{
				Error: ErrTableAlreadyExists,
			})
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	metadata, err := json.Marshal(table.Metadata())
	if err != nil {
		log.Errorf("failed to marshal metadata: %w", err)
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
	log := c.MustGet("logger").(logger.Logger)

	prefix := c.Param("prefix")
	if prefix != "" {
		log.Warn("prefix query parameter is not supported")
	}

	namespace := c.Param("namespace")
	ns := strings.Split(namespace, namespaceSeparator)

	tableName := c.Param("table")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	var req UpdateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Errorf("failed to bind json: %w", err)
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	table, err := h.catalog.LoadTable(c.Request.Context(), append(ns, tableName), nil)
	if err != nil {
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, icecat.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	metadata, metadataLoc, err := h.catalog.CommitTable(c.Request.Context(), table, req.Requirements, req.Updates)
	if err != nil {
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, icecat.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		log.Errorf("failed to marshal metadata: %w", err)
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
	log := c.MustGet("logger").(logger.Logger)

	prefix := c.Param("prefix")
	if prefix != "" {
		log.Warn("prefix query parameter is not supported")
	}

	namespace := c.Param("namespace")
	ns := strings.Split(namespace, namespaceSeparator)

	tableName := c.Param("table")

	table, err := h.catalog.LoadTable(c.Request.Context(), append(ns, tableName), nil)
	if err != nil {
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, icecat.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	metadata, err := json.Marshal(table.Metadata())
	if err != nil {
		log.Errorf("failed to marshal metadata: %w", err)
	}

	resp := LoadTableResponse{
		MetadataLoc: table.MetadataLocation(),
		Metadata:    metadata,
		Config:      table.Properties(),
	}

	c.JSON(http.StatusOK, resp)
}

func (h *CatalogHandler) DropTable(c *gin.Context) {
	log := c.MustGet("logger").(logger.Logger)

	prefix := c.Param("prefix")
	if prefix != "" {
		log.Warn("prefix query parameter is not supported")
	}

	namespace := c.Param("namespace")
	ns := strings.Split(namespace, namespaceSeparator)

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

	err := h.catalog.DropTable(c.Request.Context(), append(ns, tableName))
	if err != nil {
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, icecat.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	c.JSON(http.StatusNoContent, nil)
}

func (h *CatalogHandler) TableExists(c *gin.Context) {
	log := c.MustGet("logger").(logger.Logger)

	prefix := c.Param("prefix")
	if prefix != "" {
		log.Warn("prefix query parameter is not supported")
	}

	namespace := c.Param("namespace")
	ns := strings.Split(namespace, namespaceSeparator)

	tableName := c.Param("table")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	exists, err := h.catalog.CheckTableExists(c.Request.Context(), append(ns, tableName))
	if err != nil {
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, icecat.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
			})
			return
		}
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
	log := c.MustGet("logger").(logger.Logger)

	prefix := c.Param("prefix")
	if prefix != "" {
		log.Warn("prefix query parameter is not supported")
	}

	var req RenameTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Errorf("failed to bind json: %w", err)
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
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrNamespaceNotFound,
			})
			return
		}
		if errors.Is(err, icecat.ErrNoSuchTable) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Error: ErrTableNotFound,
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
