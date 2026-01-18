# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build the CLI (requires CGO for SQLite support)
CGO_ENABLED=1 go build -o dbmask ./cmd/dbmask

# Run tests
go test ./...

# Run a specific test
go test -run TestFunctionName ./internal/anonymiser/

# Download dependencies
go mod tidy
```

## Usage

```bash
# Basic export
./dbmask -c config.yaml -o output.sql

# Dry run (shows what would happen)
./dbmask -c config.yaml --dry-run

# Verbose output
./dbmask -c config.yaml -o output.sql -v

# Sync config with database (add missing tables)
./dbmask sync -c config.yaml --dry-run
./dbmask sync -c config.yaml
./dbmask sync -c config.yaml --truncate
```

## Architecture

The tool exports database schemas and data while anonymising sensitive information. Data flows through these stages:

1. **Config** (`internal/config/`) - Parses YAML/JSON config defining connection and table rules
2. **Driver** (`internal/database/`) - Database-agnostic interface with MySQL, PostgreSQL, SQLite implementations
3. **Schema** (`internal/schema/`) - Extracts table schemas and topologically sorts by FK dependencies
4. **Anonymiser** (`internal/anonymiser/`) - Applies column transformations using faker templates or static values
5. **Exporter** (`internal/exporter/`) - Streams rows in batches and generates SQL dump

### Key Interfaces

The `Driver` interface (`internal/database/driver.go`) is the abstraction for all database operations. Each database type implements: `GetTables`, `GetTableSchema`, `GetForeignKeys`, `StreamRows`, etc.

### Configuration Rules

Tables can have three operations:
- `truncate: true` - Export schema only, no data
- `retain: N` - Limit to N rows
- `columns:` - Anonymisation rules per column

Column values support:
- `{{faker.functionName}}` - Generate fake data (see `internal/anonymiser/faker.go` for available functions)
- `"static string"` - Replace with literal value
- `null` - Set to NULL

### Consistency Mapping

The anonymiser maintains a consistency map (`column:originalValue` → `anonymisedValue`) to preserve referential integrity when the same value appears multiple times.
