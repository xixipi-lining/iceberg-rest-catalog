package handlers

import "net/http"

type ErrorResponse struct {
	Error ErrorModel `json:"error"`
}

type ErrorModel struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`
}

var ErrInternalServerError = ErrorModel{
	Message: "Internal Server Error",
	Type:    "InternalServerError",
	Code:    http.StatusInternalServerError,
}

var ErrBadRequest = ErrorModel{
	Message: "Malformed request",
	Type:    "BadRequestException",
	Code:    http.StatusBadRequest,
}

var ErrNamespaceNotFound = ErrorModel{
	Message: "The given namespace does not exist",
	Type:    "NoSuchNamespaceException",
	Code:    http.StatusNotFound,
}

var ErrNamespaceAlreadyExists = ErrorModel{
	Message: "The given namespace already exists",
	Type:    "AlreadyExistsException",
	Code:    http.StatusConflict,
}

var ErrNamespaceNotEmpty = ErrorModel{
	Message: "The given namespace is not empty",
	Type:    "NamespaceNotEmptyException",
	Code:    http.StatusConflict,
}

var ErrUnprocessableEntityDuplicateKey = ErrorModel{
	Message: "The request cannot be processed as there is a key present multiple times",
	Type:    "UnprocessableEntityException",
	Code:    http.StatusUnprocessableEntity,
}

var ErrNotImplemented = ErrorModel{
	Message: "Not Implemented",
	Type:    "NotImplementedException",
	Code:    http.StatusNotImplemented,
}

var ErrTableAlreadyExists = ErrorModel{
	Message: "The given table already exists",
	Type:    "AlreadyExistsException",
	Code:    http.StatusConflict,
}

var ErrTableNotFound = ErrorModel{
	Message: "The given table does not exist",
	Type:    "NoSuchTableException",
	Code:    http.StatusNotFound,
}
