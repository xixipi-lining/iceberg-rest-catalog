package handlers

import (
	"errors"
	"net/http"

	"github.com/apache/iceberg-go/catalog"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/gin-gonic/gin"
	"github.com/xixipi-lining/iceberg-rest-catalog/logger"
)

func (h *CatalogHandler) MultiTablesCommit(c *gin.Context) {
	log := c.MustGet("logger").(logger.Logger)

	prefix := c.Param("prefix")
	if prefix != "" {
		log.Warn("prefix query parameter is not supported")
	}

	var req []UpdateTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Errorf("failed to bind json: %w", err)
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	commits := make([]catalog.MultiTableCommit, len(req))
	for i, r := range req {
		ns := r.Identifier.Namespace
		tableName := r.Identifier.Name
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
		commits[i] = catalog.MultiTableCommit{
			Table: table,
			Requirements: r.Requirements,
			Updates: r.Updates,
		}
	}

	if err := h.catalog.MultiTableCommit(c.Request.Context(), commits, nil); err != nil {
		log.Errorf("failed to commit tables: %w", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	c.JSON(http.StatusOK, nil)
}