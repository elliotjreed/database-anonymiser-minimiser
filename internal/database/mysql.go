package database

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
)

// MySQLDriver implements the Driver interface for MySQL databases.
type MySQLDriver struct {
	db       *sql.DB
	database string
}

// Connect establishes a connection to the MySQL database.
func (d *MySQLDriver) Connect(cfg *config.Connection) error {
	db, err := sql.Open("mysql", cfg.DSN())
	if err != nil {
		return fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping MySQL: %w", err)
	}

	d.db = db
	d.database = cfg.DatabaseName
	return nil
}

// Close closes the database connection.
func (d *MySQLDriver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// GetTables returns all table names in the database.
func (d *MySQLDriver) GetTables() ([]string, error) {
	query := `SELECT table_name FROM information_schema.tables
              WHERE table_schema = ? AND table_type = 'BASE TABLE'
              ORDER BY table_name`

	rows, err := d.db.Query(query, d.database)
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
func (d *MySQLDriver) GetTableSchema(table string) (string, error) {
	var tableName, createStmt string
	query := fmt.Sprintf("SHOW CREATE TABLE %s", d.QuoteIdentifier(table))

	err := d.db.QueryRow(query).Scan(&tableName, &createStmt)
	if err != nil {
		return "", fmt.Errorf("failed to get schema for table %s: %w", table, err)
	}

	return createStmt + ";", nil
}

// GetColumns returns column information for a table.
func (d *MySQLDriver) GetColumns(table string) ([]ColumnInfo, error) {
	query := `SELECT column_name, data_type, is_nullable, column_default
              FROM information_schema.columns
              WHERE table_schema = ? AND table_name = ?
              ORDER BY ordinal_position`

	rows, err := d.db.Query(query, d.database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var isNullable string
		if err := rows.Scan(&col.Name, &col.DataType, &isNullable, &col.Default); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		col.IsNullable = isNullable == "YES"
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// GetForeignKeys returns all foreign key relationships in the database.
func (d *MySQLDriver) GetForeignKeys() ([]ForeignKey, error) {
	query := `SELECT
                kcu.table_name,
                kcu.column_name,
                kcu.referenced_table_name,
                kcu.referenced_column_name
              FROM information_schema.key_column_usage kcu
              WHERE kcu.table_schema = ?
                AND kcu.referenced_table_name IS NOT NULL
              ORDER BY kcu.table_name, kcu.ordinal_position`

	rows, err := d.db.Query(query, d.database)
	if err != nil {
		return nil, fmt.Errorf("failed to query foreign keys: %w", err)
	}
	defer rows.Close()

	var fks []ForeignKey
	for rows.Next() {
		var fk ForeignKey
		if err := rows.Scan(&fk.Table, &fk.Column, &fk.ReferencedTable, &fk.ReferencedColumn); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key: %w", err)
		}
		fks = append(fks, fk)
	}

	return fks, rows.Err()
}

// StreamRows streams rows from a table in batches.
func (d *MySQLDriver) StreamRows(table string, limit int, batchSize int, callback RowCallback) error {
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

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := d.db.Query(query)
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
func (d *MySQLDriver) GetRowCount(table string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", d.QuoteIdentifier(table))
	err := d.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return count, nil
}

// QuoteIdentifier quotes an identifier for MySQL.
func (d *MySQLDriver) QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// GetDatabaseType returns "mysql".
func (d *MySQLDriver) GetDatabaseType() string {
	return "mysql"
}
