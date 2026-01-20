package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
)

// StreamOptions contains options for streaming rows from a table.
type StreamOptions struct {
	Limit      int       // Maximum number of rows to fetch (0 = unlimited)
	ColumnName string    // Column name for date-based filtering
	AfterDate  time.Time // Only fetch rows where ColumnName > AfterDate
}

// ForeignKey represents a foreign key relationship.
type ForeignKey struct {
	Table            string // Table containing the foreign key
	Column           string // Column that is the foreign key
	ReferencedTable  string // Table being referenced
	ReferencedColumn string // Column being referenced
}

// ColumnInfo holds metadata about a table column.
type ColumnInfo struct {
	Name       string
	DataType   string
	IsNullable bool
	Default    sql.NullString
}

// RowCallback is called for each batch of rows during streaming.
type RowCallback func(rows []map[string]any) error

// Driver defines the interface for database operations.
type Driver interface {
	// Connect establishes a connection to the database.
	Connect(cfg *config.Connection) error

	// Close closes the database connection.
	Close() error

	// GetTables returns a list of all table names in the database.
	GetTables() ([]string, error)

	// GetTableSchema returns the CREATE TABLE statement for a table.
	GetTableSchema(table string) (string, error)

	// GetColumns returns column information for a table.
	GetColumns(table string) ([]ColumnInfo, error)

	// GetForeignKeys returns all foreign key relationships in the database.
	GetForeignKeys() ([]ForeignKey, error)

	// StreamRows streams rows from a table in batches.
	// The opts parameter controls row filtering (by count or date).
	StreamRows(table string, opts StreamOptions, batchSize int, callback RowCallback) error

	// GetRowCount returns the number of rows in a table.
	GetRowCount(table string) (int64, error)

	// QuoteIdentifier quotes an identifier (table/column name) for safe use in SQL.
	QuoteIdentifier(name string) string

	// GetDatabaseType returns the database type (mysql, postgres, sqlite).
	GetDatabaseType() string
}

// NewDriver creates a new database driver based on the connection type.
func NewDriver(dbType string) (Driver, error) {
	switch dbType {
	case "mysql":
		return &MySQLDriver{}, nil
	case "postgres":
		return &PostgresDriver{}, nil
	case "sqlite":
		return &SQLiteDriver{}, nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}
