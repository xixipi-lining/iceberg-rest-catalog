package handlers

import (
	"encoding/json"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

const namespaceSeparator = "\x1F"

type Namespace []string

type Identifier struct {
	Namespace Namespace `json:"namespace"`
	Name      string    `json:"name"`
}

type ListNamespacesRequest struct {
	Parent    *string `form:"parent"`
	PageToken *string `form:"pageToken"`
	PageSize  *int    `form:"pageSize"`
}

type ListNamespacesResponse struct {
	Namespaces    [][]string `json:"namespaces"`
	NextPageToken *string    `json:"next-page-token"`
}

type CreateNamespaceRequest struct {
	Namespace  []string          `json:"namespace" binding:"required"`
	Properties map[string]string `json:"properties"`
}

type CreateNamespaceResponse CreateNamespaceRequest

type LoadNamespaceMetadataResponse struct {
	Namespace  []string          `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

type UpdatePropertiesRequest struct {
	Removals []string          `json:"removals"`
	Updates  map[string]string `json:"updates"`
}

type UpdatePropertiesResponse struct {
	Updated []string `json:"updated"`
	Removed []string `json:"removed"`
	Missing []string `json:"missing"`
}
type ListTablesRequest struct {
	PageToken *string `form:"pageToken"`
	PageSize  *int    `form:"pageSize"`
}

type ListTablesResponse struct {
	Identifiers   []Identifier `json:"identifiers"`
	NextPageToken *string      `json:"next-page-token,omitempty"`
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

type UpdateTableRequest struct {
	Identifier   Identifier         `json:"identifier"`
	Requirements table.Requirements `json:"requirements"`
	Updates      table.Updates      `json:"updates"`
}

type UpdateTableResponse struct {
	MetadataLoc string          `json:"metadata-location"`
	Metadata    json.RawMessage `json:"metadata"`
}

type RenameTableRequest struct {
	Source      Identifier `json:"source"`
	Destination Identifier `json:"destination"`
}
