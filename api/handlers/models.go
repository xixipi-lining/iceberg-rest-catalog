package handlers

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
