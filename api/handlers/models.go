package handlers

import (
	"encoding/json"

	icetbl "github.com/apache/iceberg-go/table"
)

const namespaceSeparator = "\x1F"

type Namespace []string

type Identifier struct {
	Namespace Namespace `json:"namespace"`
	Name      string    `json:"name"`
}

type ListTablesResponse struct {
	Identifiers   []Identifier `json:"identifiers"`
	NextPageToken string       `json:"next-page-token,omitempty"`
}

type UpdateTableRequest struct {
	Identifier   Identifier           `json:"identifier"`
	Requirements []icetbl.Requirement `json:"requirements"`
	Updates      []icetbl.Update      `json:"updates"`
}

type UpdateTableResponse struct {
	MetadataLoc string          `json:"metadata-location"`
	Metadata    json.RawMessage `json:"metadata"`
}
