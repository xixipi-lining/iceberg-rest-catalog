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
	Identifier   Identifier          `json:"identifier"`
	Requirements []table.Requirement `json:"requirements"`
	Updates      []table.Update      `json:"updates"`
}

type UpdateTableResponse struct {
	MetadataLoc string          `json:"metadata-location"`
	Metadata    json.RawMessage `json:"metadata"`
}

type RenameTableRequest struct {
	Source      Identifier `json:"source"`
	Destination Identifier `json:"destination"`
}
