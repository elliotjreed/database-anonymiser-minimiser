package database

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
)

// SQLiteDriver implements the Driver interface for SQLite databases.
type SQLiteDriver struct {
	db *sql.DB
}

// Connect establishes a connection to the SQLite database.
func (d *SQLiteDriver) Connect(cfg *config.Connection) error {
	db, err := sql.Open("sqlite3", cfg.DSN())
	if err != nil {
		return fmt.Errorf("failed to open SQLite connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping SQLite: %w", err)
	}

	d.db = db
	return nil
}

// Close closes the database connection.
func (d *SQLiteDriver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// GetTables returns all table names in the database.
func (d *SQLiteDriver) GetTables() ([]string, error) {
	query := `SELECT name FROM sqlite_master
              WHERE type='table' AND name NOT LIKE 'sqlite_%'
              ORDER BY name`

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, name)
	}

	return tables, rows.Err()
}

// GetTableSchema returns the CREATE TABLE statement for a table.
func (d *SQLiteDriver) GetTableSchema(table string) (string, error) {
	var createStmt string
	query := `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`

	err := d.db.QueryRow(query, table).Scan(&createStmt)
	if err != nil {
		return "", fmt.Errorf("failed to get schema for table %s: %w", table, err)
	}

	return createStmt + ";", nil
}

// GetColumns returns column information for a table.
func (d *SQLiteDriver) GetColumns(table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", d.QuoteIdentifier(table))

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull int
		var dfltValue sql.NullString
		var pk int

		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		col := ColumnInfo{
			Name:       name,
			DataType:   dataType,
			IsNullable: notNull == 0,
			Default:    dfltValue,
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// GetForeignKeys returns all foreign key relationships in the database.
func (d *SQLiteDriver) GetForeignKeys() ([]ForeignKey, error) {
	// Get all tables first
	tables, err := d.GetTables()
	if err != nil {
		return nil, err
	}

	var fks []ForeignKey
	for _, table := range tables {
		query := fmt.Sprintf("PRAGMA foreign_key_list(%s)", d.QuoteIdentifier(table))
		rows, err := d.db.Query(query)
		if err != nil {
			continue // Skip tables with no foreign keys
		}

		for rows.Next() {
			var id, seq int
			var refTable, from, to, onUpdate, onDelete, match string

			if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
				rows.Close()
				continue
			}

			fk := ForeignKey{
				Table:            table,
				Column:           from,
				ReferencedTable:  refTable,
				ReferencedColumn: to,
			}
			fks = append(fks, fk)
		}
		rows.Close()
	}

	return fks, nil
}

// StreamRows streams rows from a table in batches.
func (d *SQLiteDriver) StreamRows(table string, opts StreamOptions, batchSize int, callback RowCallback) error {
	// Get column names first
	columns, err := d.GetColumns(table)
	if err != nil {
		return err
	}

	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = d.QuoteIdentifier(col.Name)
	}

	// Build query
	query := fmt.Sprintf("SELECT %s FROM %s",
		strings.Join(columnNames, ", "),
		d.QuoteIdentifier(table))

	var args []any

	// Add date-based WHERE clause if specified
	if opts.ColumnName != "" && !opts.AfterDate.IsZero() {
		query += fmt.Sprintf(" WHERE %s > ?", d.QuoteIdentifier(opts.ColumnName))
		args = append(args, opts.AfterDate.Format("2006-01-02 15:04:05"))
	}

	// Add LIMIT clause if specified
	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}
	defer rows.Close()

	// Prepare scan destinations
	colNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %w", err)
	}

	batch := make([]map[string]any, 0, batchSize)

	for rows.Next() {
		// Create scan destinations
		values := make([]any, len(colNames))
		valuePtrs := make([]any, len(colNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to map
		row := make(map[string]any)
		for i, col := range colNames {
			val := values[i]
			// Convert []byte to string for readability
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		batch = append(batch, row)

		// Process batch when full
		if len(batch) >= batchSize {
			if err := callback(batch); err != nil {
				return err
			}
			batch = make([]map[string]any, 0, batchSize)
		}
	}

	// Process remaining rows
	if len(batch) > 0 {
		if err := callback(batch); err != nil {
			return err
		}
	}

	return rows.Err()
}

// GetRowCount returns the number of rows in a table.
func (d *SQLiteDriver) GetRowCount(table string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", d.QuoteIdentifier(table))
	err := d.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return count, nil
}

// QuoteIdentifier quotes an identifier for SQLite.
func (d *SQLiteDriver) QuoteIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

// GetDatabaseType returns "sqlite".
func (d *SQLiteDriver) GetDatabaseType() string {
	return "sqlite"
}
