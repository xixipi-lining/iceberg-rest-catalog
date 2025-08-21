package handlers

import (
	"net/http"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/gin-gonic/gin"
)

type kv struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (h *CatalogHandler) SetKVSidecar(c *gin.Context) {
	var req kv
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}
	if err := h.catalog.SetKVSidecar(c.Request.Context(), req.Key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	c.Status(http.StatusAccepted)
}

func (h *CatalogHandler) GetKVSidecar(c *gin.Context) {
	key := c.Param("key")
	value, err := h.catalog.GetKVSidecar(c.Request.Context(), key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	c.JSON(http.StatusOK, kv{
		Key:   key,
		Value: value,
	})
}

type request struct {
	CreateTable *struct {
		CreateTableRequest
		Namespace string `json:"namespace"`
	} `json:"create_table"`
	UpdateTable *UpdateTableRequest `json:"update_table"`
	SetKVSidecar *kv `json:"set_kv_sidecar"`
}

func (h *CatalogHandler) Transaction(c *gin.Context) {
	var req []request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error: ErrBadRequest,
		})
		return
	}

	var reqs []catalog.TransactionRequest
	for _, r := range req {
		if r.CreateTable != nil {
			var opts []catalog.CreateTableOpt
			if r.CreateTable.Location != "" {
				opts = append(opts, catalog.WithLocation(r.CreateTable.Location))
			}
			if r.CreateTable.PartitionSpec != nil {
				opts = append(opts, catalog.WithPartitionSpec(r.CreateTable.PartitionSpec))
			}
			if r.CreateTable.WriteOrder != nil {
				opts = append(opts, catalog.WithSortOrder(*r.CreateTable.WriteOrder))
			}
			if r.CreateTable.Props != nil {
				opts = append(opts, catalog.WithProperties(r.CreateTable.Props))
			}
			reqs = append(reqs, &catalog.CreateTableRequest{
				Identifier: table.Identifier{r.CreateTable.Namespace, r.CreateTable.Name},
				Schema:     r.CreateTable.Schema,
				Opts:       opts,
			})
		}
		if r.UpdateTable != nil {
			ident := append(r.UpdateTable.Identifier.Namespace, r.UpdateTable.Identifier.Name)
			table, err := h.catalog.LoadTable(c.Request.Context(), ident, nil)
			if err != nil {
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Error: ErrInternalServerError,
				})
				return
			}
			reqs = append(reqs, &catalog.CommitTableRequest{
				Table: table,
				Updates: r.UpdateTable.Updates,
				Requirements: r.UpdateTable.Requirements,
			})
		}
		if r.SetKVSidecar != nil {
			reqs = append(reqs, &catalog.SetKVSidecarRequest{
				Key:   r.SetKVSidecar.Key,
				Value: r.SetKVSidecar.Value,
			})
		}
	}

	if err := h.catalog.Transaction(c.Request.Context(), reqs, h.followers...); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error: ErrInternalServerError,
		})
		return
	}

	c.Status(http.StatusOK)
}