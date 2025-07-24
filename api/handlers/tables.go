package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/gin-gonic/gin"
	"github.com/xixipi-lining/iceberg-rest-catalog/logger"
	"github.com/xixipi-lining/iceberg-rest-catalog/service/catalog"
)

const (
	SchemaTypeStruct SchemaType = "struct"
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
		NextPageToken: *nextPageToken,
	})
}

type CreateTableRequest struct {
	Name          string                 `json:"name"`
	Schema        *iceberg.Schema        `json:"schema"`
	Location      string                 `json:"location,omitempty"`
	PartitionSpec *iceberg.PartitionSpec `json:"partition-spec,omitempty"`
	WriteOrder    *table.SortOrder       `json:"write-order,omitempty"`
	StageCreate   bool                   `json:"stage-create"`
	Props         iceberg.Properties     `json:"properties,omitempty"`
}

type LoadTableResponse struct {
	MetadataLoc string             `json:"metadata-location"`
	Metadata    json.RawMessage    `json:"metadata"`
	Config      iceberg.Properties `json:"config"`
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

type Type struct {
	union json.RawMessage
}

type StructField struct {
	Doc      *string `json:"doc,omitempty"`
	Id       int     `json:"id"`
	Name     string  `json:"name"`
	Required bool    `json:"required"`
	Type     Type    `json:"type"`
}

type Schema struct {
	Fields             []StructField `json:"fields"`
	IdentifierFieldIds *[]int        `json:"identifier-field-ids,omitempty"`
	SchemaId           *int          `json:"schema-id,omitempty"`
	Type               SchemaType    `json:"type"`
}

type SchemaType string

type Transform = string

type PartitionField struct {
	FieldId   *int      `json:"field-id,omitempty"`
	Name      string    `json:"name"`
	SourceId  int       `json:"source-id"`
	Transform Transform `json:"transform"`
}

// PartitionSpec defines model for PartitionSpec.
type PartitionSpec struct {
	Fields []PartitionField `json:"fields"`
	SpecId *int             `json:"spec-id,omitempty"`
}

type SortDirection string

type NullOrder string

// SortField defines model for SortField.
type SortField struct {
	Direction SortDirection `json:"direction"`
	NullOrder NullOrder     `json:"null-order"`
	SourceId  int           `json:"source-id"`
	Transform Transform     `json:"transform"`
}

// SortOrder defines model for SortOrder.
type SortOrder struct {
	Fields  []SortField `json:"fields"`
	OrderId *int        `json:"order-id,omitempty"`
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
