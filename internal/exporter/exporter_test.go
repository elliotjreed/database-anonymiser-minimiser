package exporter

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/anonymiser"
	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
	"github.com/elliotjreed/database-anonymiser-minimiser/internal/database"
	"github.com/elliotjreed/database-anonymiser-minimiser/internal/schema"
)

// mockDriver implements database.Driver for testing
type mockDriver struct {
	dbType      string
	tables      []string
	columns     map[string][]database.ColumnInfo
	rows        map[string][]map[string]any
	streamErr   error
}

func (m *mockDriver) Connect(cfg *config.Connection) error { return nil }
func (m *mockDriver) Close() error                         { return nil }
func (m *mockDriver) GetTables() ([]string, error)         { return m.tables, nil }
func (m *mockDriver) GetTableSchema(table string) (string, error) {
	return "CREATE TABLE " + table + ";", nil
}
func (m *mockDriver) GetColumns(table string) ([]database.ColumnInfo, error) {
	if cols, ok := m.columns[table]; ok {
		return cols, nil
	}
	return nil, nil
}
func (m *mockDriver) GetForeignKeys() ([]database.ForeignKey, error) {
	return nil, nil
}
func (m *mockDriver) StreamRows(table string, opts database.StreamOptions, batchSize int, callback database.RowCallback) error {
	if m.streamErr != nil {
		return m.streamErr
	}
	if rows, ok := m.rows[table]; ok {
		if opts.Limit > 0 && opts.Limit < len(rows) {
			rows = rows[:opts.Limit]
		}
		// Process in batches
		for i := 0; i < len(rows); i += batchSize {
			end := i + batchSize
			if end > len(rows) {
				end = len(rows)
			}
			if err := callback(rows[i:end]); err != nil {
				return err
			}
		}
	}
	return nil
}
func (m *mockDriver) GetRowCount(table string) (int64, error) {
	if rows, ok := m.rows[table]; ok {
		return int64(len(rows)), nil
	}
	return 0, nil
}
func (m *mockDriver) QuoteIdentifier(name string) string {
	return "\"" + name + "\""
}
func (m *mockDriver) GetDatabaseType() string {
	if m.dbType != "" {
		return m.dbType
	}
	return "sqlite"
}

func TestNew(t *testing.T) {
	driver := &mockDriver{}
	cfg := &config.Config{}
	anon := anonymiser.New(cfg)
	var buf bytes.Buffer

	t.Run("default batch size", func(t *testing.T) {
		exp := New(driver, anon, &buf, Options{})
		if exp == nil {
			t.Fatal("New() returned nil")
		}
		if exp.batchSize != DefaultBatchSize {
			t.Errorf("batchSize = %d, want %d", exp.batchSize, DefaultBatchSize)
		}
	})

	t.Run("custom batch size", func(t *testing.T) {
		exp := New(driver, anon, &buf, Options{BatchSize: 500})
		if exp.batchSize != 500 {
			t.Errorf("batchSize = %d, want 500", exp.batchSize)
		}
	})

	t.Run("zero batch size uses default", func(t *testing.T) {
		exp := New(driver, anon, &buf, Options{BatchSize: 0})
		if exp.batchSize != DefaultBatchSize {
			t.Errorf("batchSize = %d, want %d", exp.batchSize, DefaultBatchSize)
		}
	})

	t.Run("negative batch size uses default", func(t *testing.T) {
		exp := New(driver, anon, &buf, Options{BatchSize: -10})
		if exp.batchSize != DefaultBatchSize {
			t.Errorf("batchSize = %d, want %d", exp.batchSize, DefaultBatchSize)
		}
	})
}

func TestExport(t *testing.T) {
	t.Run("export empty tables", func(t *testing.T) {
		driver := &mockDriver{
			columns: map[string][]database.ColumnInfo{
				"users": {{Name: "id"}, {Name: "name"}},
			},
			rows: map[string][]map[string]any{},
		}
		cfg := &config.Config{}
		anon := anonymiser.New(cfg)
		var buf bytes.Buffer

		exp := New(driver, anon, &buf, Options{BatchSize: 10})

		tables := []schema.TableInfo{
			{
				Name:       "users",
				CreateStmt: "CREATE TABLE users (id INT, name VARCHAR(255));",
				Columns:    []database.ColumnInfo{{Name: "id"}, {Name: "name"}},
			},
		}

		err := exp.Export(tables)
		if err != nil {
			t.Fatalf("Export() error = %v", err)
		}

		output := buf.String()

		// Check header
		if !strings.Contains(output, "-- Database Dump") {
			t.Error("Output missing database dump header")
		}

		// Check table comment
		if !strings.Contains(output, "-- Table: users") {
			t.Error("Output missing table comment")
		}

		// Check DROP TABLE
		if !strings.Contains(output, "DROP TABLE IF EXISTS") {
			t.Error("Output missing DROP TABLE statement")
		}

		// Check CREATE TABLE
		if !strings.Contains(output, "CREATE TABLE users") {
			t.Error("Output missing CREATE TABLE statement")
		}
	})

	t.Run("export with data", func(t *testing.T) {
		driver := &mockDriver{
			columns: map[string][]database.ColumnInfo{
				"users": {{Name: "id"}, {Name: "name"}},
			},
			rows: map[string][]map[string]any{
				"users": {
					{"id": int64(1), "name": "John"},
					{"id": int64(2), "name": "Jane"},
				},
			},
		}
		cfg := &config.Config{}
		anon := anonymiser.New(cfg)
		var buf bytes.Buffer

		exp := New(driver, anon, &buf, Options{BatchSize: 10})

		tables := []schema.TableInfo{
			{
				Name:       "users",
				CreateStmt: "CREATE TABLE users (id INT, name VARCHAR(255));",
				Columns:    []database.ColumnInfo{{Name: "id"}, {Name: "name"}},
			},
		}

		err := exp.Export(tables)
		if err != nil {
			t.Fatalf("Export() error = %v", err)
		}

		output := buf.String()

		// Check INSERT statement
		if !strings.Contains(output, "INSERT INTO") {
			t.Error("Output missing INSERT statement")
		}

		// Check data values
		if !strings.Contains(output, "'John'") {
			t.Error("Output missing 'John' value")
		}
		if !strings.Contains(output, "'Jane'") {
			t.Error("Output missing 'Jane' value")
		}
	})

	t.Run("export with truncation", func(t *testing.T) {
		driver := &mockDriver{
			columns: map[string][]database.ColumnInfo{
				"logs": {{Name: "id"}, {Name: "message"}},
			},
			rows: map[string][]map[string]any{
				"logs": {
					{"id": int64(1), "message": "log1"},
				},
			},
		}
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"logs": {Truncate: true},
			},
		}
		anon := anonymiser.New(cfg)
		var buf bytes.Buffer

		exp := New(driver, anon, &buf, Options{BatchSize: 10})

		tables := []schema.TableInfo{
			{
				Name:       "logs",
				CreateStmt: "CREATE TABLE logs (id INT);",
				Columns:    []database.ColumnInfo{{Name: "id"}, {Name: "message"}},
			},
		}

		err := exp.Export(tables)
		if err != nil {
			t.Fatalf("Export() error = %v", err)
		}

		output := buf.String()

		// Should NOT contain INSERT (truncated)
		if strings.Contains(output, "INSERT INTO") {
			t.Error("Truncated table should not have INSERT statements")
		}

		// Check stats
		stats := exp.GetStats()
		if stats.TablesTruncated != 1 {
			t.Errorf("TablesTruncated = %d, want 1", stats.TablesTruncated)
		}
	})

	t.Run("export with retain limit", func(t *testing.T) {
		driver := &mockDriver{
			columns: map[string][]database.ColumnInfo{
				"users": {{Name: "id"}},
			},
			rows: map[string][]map[string]any{
				"users": {
					{"id": int64(1)},
					{"id": int64(2)},
					{"id": int64(3)},
					{"id": int64(4)},
					{"id": int64(5)},
				},
			},
		}
		cfg := &config.Config{
			Configuration: map[string]*config.TableConfig{
				"users": {Retain: config.RetainConfig{Count: 2}},
			},
		}
		anon := anonymiser.New(cfg)
		var buf bytes.Buffer

		exp := New(driver, anon, &buf, Options{BatchSize: 10})

		tables := []schema.TableInfo{
			{
				Name:       "users",
				CreateStmt: "CREATE TABLE users (id INT);",
				Columns:    []database.ColumnInfo{{Name: "id"}},
			},
		}

		err := exp.Export(tables)
		if err != nil {
			t.Fatalf("Export() error = %v", err)
		}

		stats := exp.GetStats()
		if stats.RowsExported != 2 {
			t.Errorf("RowsExported = %d, want 2", stats.RowsExported)
		}
	})
}

func TestExport_DatabaseHeaders(t *testing.T) {
	tests := []struct {
		dbType   string
		contains []string
	}{
		{
			dbType:   "mysql",
			contains: []string{"SET NAMES utf8mb4", "SET FOREIGN_KEY_CHECKS = 0", "START TRANSACTION", "COMMIT"},
		},
		{
			dbType:   "postgres",
			contains: []string{"SET client_encoding = 'UTF8'", "-- End of dump"},
		},
		{
			dbType:   "sqlite",
			contains: []string{"PRAGMA foreign_keys = OFF", "PRAGMA foreign_keys = ON"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			driver := &mockDriver{dbType: tt.dbType}
			cfg := &config.Config{}
			anon := anonymiser.New(cfg)
			var buf bytes.Buffer

			exp := New(driver, anon, &buf, Options{})
			err := exp.Export([]schema.TableInfo{})

			if err != nil {
				t.Fatalf("Export() error = %v", err)
			}

			output := buf.String()
			for _, s := range tt.contains {
				if !strings.Contains(output, s) {
					t.Errorf("Output missing %q for %s", s, tt.dbType)
				}
			}
		})
	}
}

func TestExport_StreamError(t *testing.T) {
	testErr := errors.New("stream error")
	driver := &mockDriver{
		columns: map[string][]database.ColumnInfo{
			"users": {{Name: "id"}},
		},
		streamErr: testErr,
	}
	cfg := &config.Config{}
	anon := anonymiser.New(cfg)
	var buf bytes.Buffer

	exp := New(driver, anon, &buf, Options{})

	tables := []schema.TableInfo{
		{
			Name:       "users",
			CreateStmt: "CREATE TABLE users;",
			Columns:    []database.ColumnInfo{{Name: "id"}},
		},
	}

	err := exp.Export(tables)
	if err == nil {
		t.Error("Export() expected error from StreamRows")
	}
}

func TestFormatValue(t *testing.T) {
	exp := &Exporter{}

	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"nil", nil, "NULL"},
		{"true", true, "1"},
		{"false", false, "0"},
		{"int", 42, "42"},
		{"int64", int64(123), "123"},
		{"int32", int32(-5), "-5"},
		{"uint", uint(100), "100"},
		{"float64", 3.14, "3.14"},
		{"float32", float32(2.5), "2.5"},
		{"string", "hello", "'hello'"},
		{"string with quote", "it's", "'it''s'"},
		{"string with backslash", "a\\b", "'a\\\\b'"},
		{"string with newline", "line1\nline2", "'line1\\nline2'"},
		{"string with carriage return", "a\rb", "'a\\rb'"},
		{"bytes", []byte("binary"), "'binary'"},
		{"time", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), "'2024-01-15 10:30:00'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exp.formatValue(tt.value)
			if got != tt.want {
				t.Errorf("formatValue(%v) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestEscapeString(t *testing.T) {
	exp := &Exporter{}

	tests := []struct {
		input string
		want  string
	}{
		{"hello", "'hello'"},
		{"it's a test", "'it''s a test'"},
		{"back\\slash", "'back\\\\slash'"},
		{"new\nline", "'new\\nline'"},
		{"carriage\rreturn", "'carriage\\rreturn'"},
		{"null\x00char", "'null\\0char'"},
		{"ctrl-z\x1achar", "'ctrl-z\\Zchar'"},
		{"", "''"},
		{"multiple''quotes", "'multiple''''quotes'"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := exp.escapeString(tt.input)
			if got != tt.want {
				t.Errorf("escapeString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetDropTableStatement(t *testing.T) {
	tests := []struct {
		dbType string
		table  string
		want   string
	}{
		{"mysql", "users", `DROP TABLE IF EXISTS "users";`},
		{"postgres", "users", `DROP TABLE IF EXISTS "users" CASCADE;`},
		{"sqlite", "users", `DROP TABLE IF EXISTS "users";`},
		{"unknown", "users", `DROP TABLE IF EXISTS "users";`},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			driver := &mockDriver{dbType: tt.dbType}
			exp := &Exporter{driver: driver, dbType: tt.dbType}

			got := exp.getDropTableStatement(tt.table)
			if got != tt.want {
				t.Errorf("getDropTableStatement(%q) = %q, want %q", tt.table, got, tt.want)
			}
		})
	}
}

func TestWriteBatchInsert(t *testing.T) {
	t.Run("single row", func(t *testing.T) {
		driver := &mockDriver{}
		var buf bytes.Buffer
		exp := &Exporter{
			driver: driver,
			writer: bufio.NewWriter(&buf),
		}

		columns := []string{"id", "name"}
		rows := []map[string]any{
			{"id": int64(1), "name": "John"},
		}

		err := exp.writeBatchInsert("users", columns, rows)
		if err != nil {
			t.Fatalf("writeBatchInsert() error = %v", err)
		}
		exp.writer.Flush()

		output := buf.String()
		if !strings.Contains(output, `INSERT INTO "users"`) {
			t.Error("Output missing INSERT INTO statement")
		}
		if !strings.Contains(output, "1, 'John'") {
			t.Error("Output missing row values")
		}
	})

	t.Run("multiple rows", func(t *testing.T) {
		driver := &mockDriver{}
		var buf bytes.Buffer
		exp := &Exporter{
			driver: driver,
			writer: bufio.NewWriter(&buf),
		}

		columns := []string{"id", "name"}
		rows := []map[string]any{
			{"id": int64(1), "name": "John"},
			{"id": int64(2), "name": "Jane"},
		}

		err := exp.writeBatchInsert("users", columns, rows)
		if err != nil {
			t.Fatalf("writeBatchInsert() error = %v", err)
		}
		exp.writer.Flush()

		output := buf.String()
		// Should have comma between rows
		if !strings.Contains(output, "),\n(") {
			t.Error("Output missing comma separator between rows")
		}
	})

	t.Run("empty rows", func(t *testing.T) {
		driver := &mockDriver{}
		var buf bytes.Buffer
		exp := &Exporter{
			driver: driver,
			writer: bufio.NewWriter(&buf),
		}

		err := exp.writeBatchInsert("users", []string{"id"}, []map[string]any{})
		if err != nil {
			t.Fatalf("writeBatchInsert() error = %v", err)
		}
		exp.writer.Flush()

		if buf.Len() != 0 {
			t.Error("Empty rows should produce no output")
		}
	})
}

func TestGetStats(t *testing.T) {
	driver := &mockDriver{
		columns: map[string][]database.ColumnInfo{
			"users":    {{Name: "id"}},
			"orders":   {{Name: "id"}},
			"products": {{Name: "id"}},
		},
		rows: map[string][]map[string]any{
			"users":    {{"id": int64(1)}, {"id": int64(2)}},
			"orders":   {{"id": int64(1)}},
			"products": {},
		},
	}
	cfg := &config.Config{
		Configuration: map[string]*config.TableConfig{
			"products": {Truncate: true},
		},
	}
	anon := anonymiser.New(cfg)
	var buf bytes.Buffer

	exp := New(driver, anon, &buf, Options{BatchSize: 10})

	tables := []schema.TableInfo{
		{Name: "users", CreateStmt: "CREATE TABLE users;", Columns: []database.ColumnInfo{{Name: "id"}}},
		{Name: "orders", CreateStmt: "CREATE TABLE orders;", Columns: []database.ColumnInfo{{Name: "id"}}},
		{Name: "products", CreateStmt: "CREATE TABLE products;", Columns: []database.ColumnInfo{{Name: "id"}}},
	}

	err := exp.Export(tables)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	stats := exp.GetStats()

	if stats.TablesExported != 3 {
		t.Errorf("TablesExported = %d, want 3", stats.TablesExported)
	}
	if stats.TablesTruncated != 1 {
		t.Errorf("TablesTruncated = %d, want 1", stats.TablesTruncated)
	}
	if stats.RowsExported != 3 {
		t.Errorf("RowsExported = %d, want 3", stats.RowsExported)
	}
}

func TestExport_WithAnonymisation(t *testing.T) {
	driver := &mockDriver{
		columns: map[string][]database.ColumnInfo{
			"users": {{Name: "id"}, {Name: "email"}},
		},
		rows: map[string][]map[string]any{
			"users": {
				{"id": int64(1), "email": "john@example.com"},
			},
		},
	}
	cfg := &config.Config{
		Configuration: map[string]*config.TableConfig{
			"users": {
				Columns: map[string]string{
					"email": "redacted@example.com",
				},
			},
		},
	}
	anon := anonymiser.New(cfg)
	var buf bytes.Buffer

	exp := New(driver, anon, &buf, Options{BatchSize: 10})

	tables := []schema.TableInfo{
		{
			Name:       "users",
			CreateStmt: "CREATE TABLE users;",
			Columns:    []database.ColumnInfo{{Name: "id"}, {Name: "email"}},
		},
	}

	err := exp.Export(tables)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}

	output := buf.String()

	// Should have anonymised email
	if !strings.Contains(output, "redacted@example.com") {
		t.Error("Output should contain anonymised email")
	}
	// Should NOT have original email
	if strings.Contains(output, "john@example.com") {
		t.Error("Output should not contain original email")
	}
}

func TestStatsStruct(t *testing.T) {
	stats := Stats{
		TablesExported:  10,
		TablesTruncated: 2,
		RowsExported:    1000,
	}

	if stats.TablesExported != 10 {
		t.Errorf("TablesExported = %d, want 10", stats.TablesExported)
	}
	if stats.TablesTruncated != 2 {
		t.Errorf("TablesTruncated = %d, want 2", stats.TablesTruncated)
	}
	if stats.RowsExported != 1000 {
		t.Errorf("RowsExported = %d, want 1000", stats.RowsExported)
	}
}

func TestOptionsStruct(t *testing.T) {
	opts := Options{
		Verbose:   true,
		BatchSize: 500,
	}

	if !opts.Verbose {
		t.Error("Verbose = false, want true")
	}
	if opts.BatchSize != 500 {
		t.Errorf("BatchSize = %d, want 500", opts.BatchSize)
	}
}

func TestConstants(t *testing.T) {
	if DefaultBatchSize != 1000 {
		t.Errorf("DefaultBatchSize = %d, want 1000", DefaultBatchSize)
	}
	if BufferSize != 64*1024 {
		t.Errorf("BufferSize = %d, want %d", BufferSize, 64*1024)
	}
}
