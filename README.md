# Iceberg REST Catalog Server

A Golang implementation of Apache Iceberg REST Catalog Server, compatible with the iceberg-go REST catalog client.

## Overview

This project provides a REST API server that implements the Apache Iceberg Catalog specification. It's built on top of the [iceberg-go](https://github.com/apache/iceberg-go) library and maintains compatibility with the Iceberg REST specification version 0.14.1.

## Features

- **Full REST API Support**: Complete implementation of Iceberg REST Catalog API v1
- **Namespace Management**: Create, list, update, and delete namespaces
- **Table Operations**: Create, read, update, delete, and rename tables
- **Multiple Catalog Backends**: Support for SQL, REST, and AWS Glue catalogs
- **Configuration Management**: Flexible YAML-based configuration
- **Health Monitoring**: Built-in health check endpoint
- **Logging**: Structured logging with configurable levels
- **CORS Support**: Cross-origin resource sharing enabled
- **Docker Ready**: Containerized deployment support

## API Endpoints

### Configuration

- `GET /v1/config` - Get catalog configuration

### Namespaces

- `GET /v1/namespaces` - List all namespaces
- `POST /v1/namespaces` - Create a new namespace
- `GET /v1/namespaces/{namespace}` - Load namespace metadata
- `HEAD /v1/namespaces/{namespace}` - Check if namespace exists
- `DELETE /v1/namespaces/{namespace}` - Drop a namespace
- `POST /v1/namespaces/{namespace}/properties` - Update namespace properties

### Tables

- `GET /v1/namespaces/{namespace}/tables` - List tables in namespace
- `POST /v1/namespaces/{namespace}/tables` - Create a new table
- `GET /v1/namespaces/{namespace}/tables/{table}` - Load table metadata
- `POST /v1/namespaces/{namespace}/tables/{table}` - Update table
- `DELETE /v1/namespaces/{namespace}/tables/{table}` - Drop table
- `HEAD /v1/namespaces/{namespace}/tables/{table}` - Check if table exists
- `POST /v1/tables/rename` - Rename a table

### Health

- `GET /health` - Health check endpoint

## Quick Start

### Prerequisites

- Go 1.24.4 or later

### Installation

1. Clone the repository:

```bash
git clone https://github.com/xixipi-lining/iceberg-rest-catalog.git
cd iceberg-rest-catalog
```

2. Install dependencies:

```bash
make deps
```

3. Build the application:

```bash
make build
```

### Configuration

Create a configuration file `.iceberg-go.yaml` in your home directory or set `GOICEBERG_HOME` environment variable:

```yaml
default-catalog: "default"
catalog:
  default:
    type: "sql"
    uri: "sqlite3://./catalog.db"

server:
  defaults: {}
  overrides: {}

log:
  debug: true
  max_size: 100
  compress: false

port: 8080
host: "127.0.0.1"
```

### Running the Server

#### Local Development

```bash
make run
```

#### Using Docker

```bash
# Build Docker image
make docker-build

# Run container
make docker-run
```

The server will start on `http://127.0.0.1:8080` by default.
