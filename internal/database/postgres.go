package database

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
)

// PostgresDriver implements the Driver interface for PostgreSQL databases.
type PostgresDriver struct {
	db       *sql.DB
	database string
}

// Connect establishes a connection to the PostgreSQL database.
func (d *PostgresDriver) Connect(cfg *config.Connection) error {
	db, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		return fmt.Errorf("failed to open PostgreSQL connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	d.db = db
	d.database = cfg.DatabaseName
	return nil
}

// Close closes the database connection.
func (d *PostgresDriver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// GetTables returns all table names in the database.
func (d *PostgresDriver) GetTables() ([]string, error) {
	query := `SELECT table_name FROM information_schema.tables
              WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
              ORDER BY table_name`

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
func (d *PostgresDriver) GetTableSchema(table string) (string, error) {
	// PostgreSQL doesn't have SHOW CREATE TABLE, so we need to construct it
	columns, err := d.GetColumns(table)
	if err != nil {
		return "", err
	}

	// Get column definitions
	var colDefs []string
	for _, col := range columns {
		def := fmt.Sprintf("    %s %s", d.QuoteIdentifier(col.Name), col.DataType)
		if !col.IsNullable {
			def += " NOT NULL"
		}
		if col.Default.Valid {
			def += " DEFAULT " + col.Default.String
		}
		colDefs = append(colDefs, def)
	}

	// Get primary key
	pkQuery := `SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass AND i.indisprimary`

	pkRows, err := d.db.Query(pkQuery, table)
	if err == nil {
		defer pkRows.Close()
		var pkCols []string
		for pkRows.Next() {
			var col string
			if err := pkRows.Scan(&col); err == nil {
				pkCols = append(pkCols, d.QuoteIdentifier(col))
			}
		}
		if len(pkCols) > 0 {
			colDefs = append(colDefs, fmt.Sprintf("    PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
		}
	}

	schema := fmt.Sprintf("CREATE TABLE %s (\n%s\n);",
		d.QuoteIdentifier(table),
		strings.Join(colDefs, ",\n"))

	return schema, nil
}

// GetColumns returns column information for a table.
func (d *PostgresDriver) GetColumns(table string) ([]ColumnInfo, error) {
	query := `SELECT column_name,
                     CASE
                       WHEN character_maximum_length IS NOT NULL
                       THEN data_type || '(' || character_maximum_length || ')'
                       WHEN numeric_precision IS NOT NULL AND data_type NOT IN ('integer', 'bigint', 'smallint')
                       THEN data_type || '(' || numeric_precision || ',' || COALESCE(numeric_scale, 0) || ')'
                       ELSE data_type
                     END as data_type,
                     is_nullable,
                     column_default
              FROM information_schema.columns
              WHERE table_schema = 'public' AND table_name = $1
              ORDER BY ordinal_position`

	rows, err := d.db.Query(query, table)
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
func (d *PostgresDriver) GetForeignKeys() ([]ForeignKey, error) {
	query := `SELECT
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS referenced_table_name,
                ccu.column_name AS referenced_column_name
              FROM information_schema.table_constraints AS tc
              JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
              JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
              WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public'
              ORDER BY tc.table_name`

	rows, err := d.db.Query(query)
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
func (d *PostgresDriver) StreamRows(table string, limit int, batchSize int, callback RowCallback) error {
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
func (d *PostgresDriver) GetRowCount(table string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", d.QuoteIdentifier(table))
	err := d.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return count, nil
}

// QuoteIdentifier quotes an identifier for PostgreSQL.
func (d *PostgresDriver) QuoteIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

// GetDatabaseType returns "postgres".
func (d *PostgresDriver) GetDatabaseType() string {
	return "postgres"
}
