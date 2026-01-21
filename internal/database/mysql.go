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

// GetPrimaryKey returns the primary key column(s) for a table.
func (d *MySQLDriver) GetPrimaryKey(table string) ([]string, error) {
	query := `SELECT column_name
              FROM information_schema.key_column_usage
              WHERE table_schema = ?
                AND table_name = ?
                AND constraint_name = 'PRIMARY'
              ORDER BY ordinal_position`

	rows, err := d.db.Query(query, d.database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// StreamRows streams rows from a table in batches.
func (d *MySQLDriver) StreamRows(table string, opts StreamOptions, batchSize int, callback RowCallback) error {
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
	var whereClauses []string

	// Add date-based WHERE clause if specified
	if opts.ColumnName != "" && !opts.AfterDate.IsZero() {
		whereClauses = append(whereClauses, fmt.Sprintf("%s > ?", d.QuoteIdentifier(opts.ColumnName)))
		args = append(args, opts.AfterDate.Format("2006-01-02 15:04:05"))
	}

	// Add FK filter WHERE clauses
	for _, filter := range opts.FKFilters {
		if len(filter.AllowedValues) == 0 && !filter.AllowNull {
			// No allowed values and NULL not allowed means no rows can match
			return nil
		}

		clause := d.buildFKFilterClause(filter, &args)
		if clause != "" {
			whereClauses = append(whereClauses, clause)
		}
	}

	// Combine WHERE clauses
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
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
func (d *MySQLDriver) GetRowCount(table string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", d.QuoteIdentifier(table))
	err := d.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}
	return count, nil
}

// GetFilteredRowCount returns the number of rows that would be exported given the stream options.
func (d *MySQLDriver) GetFilteredRowCount(table string, opts StreamOptions) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", d.QuoteIdentifier(table))

	var args []any
	var whereClauses []string

	// Add date-based WHERE clause if specified
	if opts.ColumnName != "" && !opts.AfterDate.IsZero() {
		whereClauses = append(whereClauses, fmt.Sprintf("%s > ?", d.QuoteIdentifier(opts.ColumnName)))
		args = append(args, opts.AfterDate.Format("2006-01-02 15:04:05"))
	}

	// Add FK filter WHERE clauses
	for _, filter := range opts.FKFilters {
		if len(filter.AllowedValues) == 0 && !filter.AllowNull {
			// No allowed values and NULL not allowed means no rows can match
			return 0, nil
		}

		clause := d.buildFKFilterClause(filter, &args)
		if clause != "" {
			whereClauses = append(whereClauses, clause)
		}
	}

	// Combine WHERE clauses
	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	var count int64
	err := d.db.QueryRow(query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count filtered rows: %w", err)
	}

	// Apply limit if specified
	if opts.Limit > 0 && count > int64(opts.Limit) {
		return int64(opts.Limit), nil
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

// buildFKFilterClause builds a WHERE clause for a foreign key filter.
func (d *MySQLDriver) buildFKFilterClause(filter FKFilter, args *[]any) string {
	quotedCol := d.QuoteIdentifier(filter.Column)

	if len(filter.AllowedValues) == 0 {
		// Only NULL is allowed
		if filter.AllowNull {
			return fmt.Sprintf("%s IS NULL", quotedCol)
		}
		return "" // No values allowed, will be handled by caller
	}

	// Build IN clause
	placeholders := make([]string, len(filter.AllowedValues))
	for i, v := range filter.AllowedValues {
		placeholders[i] = "?"
		*args = append(*args, v)
	}

	inClause := fmt.Sprintf("%s IN (%s)", quotedCol, strings.Join(placeholders, ", "))

	if filter.AllowNull {
		return fmt.Sprintf("(%s OR %s IS NULL)", inClause, quotedCol)
	}

	return inClause
}
