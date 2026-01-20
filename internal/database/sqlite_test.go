package database

import (
	"errors"
	"testing"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
)

func createTestDB(t *testing.T) *SQLiteDriver {
	t.Helper()

	driver := &SQLiteDriver{}
	cfg := &config.Connection{
		Type: "sqlite",
		File: ":memory:",
	}

	if err := driver.Connect(cfg); err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}

	return driver
}

func setupTestTables(t *testing.T, driver *SQLiteDriver) {
	t.Helper()

	// Create test tables
	queries := []string{
		`CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT UNIQUE,
			age INTEGER DEFAULT 0,
			active INTEGER DEFAULT 1
		)`,
		`CREATE TABLE orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			amount REAL NOT NULL,
			created_at TEXT,
			FOREIGN KEY (user_id) REFERENCES users(id)
		)`,
		`CREATE TABLE products (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			price REAL
		)`,
	}

	for _, q := range queries {
		if _, err := driver.db.Exec(q); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}
	}
}

func TestSQLiteDriver_Connect(t *testing.T) {
	t.Run("successful connection", func(t *testing.T) {
		driver := &SQLiteDriver{}
		cfg := &config.Connection{
			Type: "sqlite",
			File: ":memory:",
		}

		err := driver.Connect(cfg)
		if err != nil {
			t.Errorf("Connect() error = %v", err)
		}
		defer driver.Close()
	})

	t.Run("invalid database path", func(t *testing.T) {
		driver := &SQLiteDriver{}
		cfg := &config.Connection{
			Type: "sqlite",
			File: "/nonexistent/path/to/db.sqlite",
		}

		err := driver.Connect(cfg)
		// SQLite will create the file, but we can't write to a nonexistent directory
		if err == nil {
			driver.Close()
			// This might succeed on some systems if the path is writable
			t.Log("Connection succeeded unexpectedly - path may be writable")
		}
	})
}

func TestSQLiteDriver_Close(t *testing.T) {
	t.Run("close open connection", func(t *testing.T) {
		driver := createTestDB(t)
		err := driver.Close()
		if err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	t.Run("close nil connection", func(t *testing.T) {
		driver := &SQLiteDriver{}
		err := driver.Close()
		if err != nil {
			t.Errorf("Close() on nil connection error = %v", err)
		}
	})
}

func TestSQLiteDriver_GetTables(t *testing.T) {
	driver := createTestDB(t)
	defer driver.Close()
	setupTestTables(t, driver)

	tables, err := driver.GetTables()
	if err != nil {
		t.Fatalf("GetTables() error = %v", err)
	}

	if len(tables) != 3 {
		t.Errorf("GetTables() returned %d tables, want 3", len(tables))
	}

	// Tables should be sorted alphabetically
	expected := []string{"orders", "products", "users"}
	for i, name := range expected {
		if tables[i] != name {
			t.Errorf("tables[%d] = %q, want %q", i, tables[i], name)
		}
	}
}

func TestSQLiteDriver_GetTables_Empty(t *testing.T) {
	driver := createTestDB(t)
	defer driver.Close()

	tables, err := driver.GetTables()
	if err != nil {
		t.Fatalf("GetTables() error = %v", err)
	}

	if len(tables) != 0 {
		t.Errorf("GetTables() on empty database returned %d tables, want 0", len(tables))
	}
}

func TestSQLiteDriver_GetTableSchema(t *testing.T) {
	driver := createTestDB(t)
	defer driver.Close()
	setupTestTables(t, driver)

	t.Run("existing table", func(t *testing.T) {
		schema, err := driver.GetTableSchema("users")
		if err != nil {
			t.Fatalf("GetTableSchema() error = %v", err)
		}

		// Should contain CREATE TABLE
		if schema == "" {
			t.Error("GetTableSchema() returned empty string")
		}

		// Should end with semicolon
		if schema[len(schema)-1] != ';' {
			t.Error("GetTableSchema() should end with semicolon")
		}
	})

	t.Run("nonexistent table", func(t *testing.T) {
		_, err := driver.GetTableSchema("nonexistent")
		if err == nil {
			t.Error("GetTableSchema() expected error for nonexistent table")
		}
	})
}

func TestSQLiteDriver_GetColumns(t *testing.T) {
	driver := createTestDB(t)
	defer driver.Close()
	setupTestTables(t, driver)

	columns, err := driver.GetColumns("users")
	if err != nil {
		t.Fatalf("GetColumns() error = %v", err)
	}

	if len(columns) != 5 {
		t.Errorf("GetColumns() returned %d columns, want 5", len(columns))
	}

	// Check specific columns
	colMap := make(map[string]ColumnInfo)
	for _, col := range columns {
		colMap[col.Name] = col
	}

	// Check id column
	if id, ok := colMap["id"]; ok {
		if id.DataType != "INTEGER" {
			t.Errorf("id.DataType = %q, want INTEGER", id.DataType)
		}
	} else {
		t.Error("id column not found")
	}

	// Check name column (NOT NULL)
	if name, ok := colMap["name"]; ok {
		if name.IsNullable {
			t.Error("name.IsNullable = true, want false")
		}
	} else {
		t.Error("name column not found")
	}

	// Check email column (nullable)
	if email, ok := colMap["email"]; ok {
		if !email.IsNullable {
			t.Error("email.IsNullable = false, want true")
		}
	} else {
		t.Error("email column not found")
	}
}

func TestSQLiteDriver_GetForeignKeys(t *testing.T) {
	driver := createTestDB(t)
	defer driver.Close()
	setupTestTables(t, driver)

	fks, err := driver.GetForeignKeys()
	if err != nil {
		t.Fatalf("GetForeignKeys() error = %v", err)
	}

	if len(fks) != 1 {
		t.Errorf("GetForeignKeys() returned %d FKs, want 1", len(fks))
	}

	if len(fks) > 0 {
		fk := fks[0]
		if fk.Table != "orders" {
			t.Errorf("FK.Table = %q, want %q", fk.Table, "orders")
		}
		if fk.Column != "user_id" {
			t.Errorf("FK.Column = %q, want %q", fk.Column, "user_id")
		}
		if fk.ReferencedTable != "users" {
			t.Errorf("FK.ReferencedTable = %q, want %q", fk.ReferencedTable, "users")
		}
		if fk.ReferencedColumn != "id" {
			t.Errorf("FK.ReferencedColumn = %q, want %q", fk.ReferencedColumn, "id")
		}
	}
}

func TestSQLiteDriver_GetForeignKeys_NoFKs(t *testing.T) {
	driver := createTestDB(t)
	defer driver.Close()

	// Create table without foreign keys
	_, err := driver.db.Exec(`CREATE TABLE simple (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	fks, err := driver.GetForeignKeys()
	if err != nil {
		t.Fatalf("GetForeignKeys() error = %v", err)
	}

	if len(fks) != 0 {
		t.Errorf("GetForeignKeys() returned %d FKs, want 0", len(fks))
	}
}

func TestSQLiteDriver_StreamRows(t *testing.T) {
	driver := createTestDB(t)
	defer driver.Close()
	setupTestTables(t, driver)

	// Insert test data
	for i := 1; i <= 10; i++ {
		_, err := driver.db.Exec(
			"INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
			"User"+string(rune('0'+i)),
			"user"+string(rune('0'+i))+"@example.com",
			20+i,
		)
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	t.Run("stream all rows", func(t *testing.T) {
		var totalRows int
		var batches int

		err := driver.StreamRows("users", 0, 3, func(rows []map[string]any) error {
			totalRows += len(rows)
			batches++
			return nil
		})

		if err != nil {
			t.Fatalf("StreamRows() error = %v", err)
		}

		if totalRows != 10 {
			t.Errorf("StreamRows() processed %d rows, want 10", totalRows)
		}

		// With batch size 3 and 10 rows, we should have 4 batches (3+3+3+1)
		if batches != 4 {
			t.Errorf("StreamRows() called callback %d times, want 4", batches)
		}
	})

	t.Run("stream with limit", func(t *testing.T) {
		var totalRows int

		err := driver.StreamRows("users", 5, 10, func(rows []map[string]any) error {
			totalRows += len(rows)
			return nil
		})

		if err != nil {
			t.Fatalf("StreamRows() error = %v", err)
		}

		if totalRows != 5 {
			t.Errorf("StreamRows() with limit=5 processed %d rows, want 5", totalRows)
		}
	})

	t.Run("callback error propagation", func(t *testing.T) {
		testErr := errors.New("test error")

		err := driver.StreamRows("users", 0, 10, func(rows []map[string]any) error {
			return testErr
		})

		if err != testErr {
			t.Errorf("StreamRows() error = %v, want %v", err, testErr)
		}
	})

	t.Run("empty table", func(t *testing.T) {
		callbackCalled := false

		err := driver.StreamRows("products", 0, 10, func(rows []map[string]any) error {
			callbackCalled = true
			return nil
		})

		if err != nil {
			t.Fatalf("StreamRows() error = %v", err)
		}

		if callbackCalled {
			t.Error("StreamRows() called callback for empty table")
		}
	})

	t.Run("verify row data", func(t *testing.T) {
		var firstRow map[string]any

		err := driver.StreamRows("users", 1, 10, func(rows []map[string]any) error {
			if len(rows) > 0 {
				firstRow = rows[0]
			}
			return nil
		})

		if err != nil {
			t.Fatalf("StreamRows() error = %v", err)
		}

		if firstRow == nil {
			t.Fatal("no rows returned")
		}

		// Check that expected columns are present
		if _, ok := firstRow["id"]; !ok {
			t.Error("row missing 'id' column")
		}
		if _, ok := firstRow["name"]; !ok {
			t.Error("row missing 'name' column")
		}
		if _, ok := firstRow["email"]; !ok {
			t.Error("row missing 'email' column")
		}
	})
}

func TestSQLiteDriver_GetRowCount(t *testing.T) {
	driver := createTestDB(t)
	defer driver.Close()
	setupTestTables(t, driver)

	t.Run("empty table", func(t *testing.T) {
		count, err := driver.GetRowCount("users")
		if err != nil {
			t.Fatalf("GetRowCount() error = %v", err)
		}
		if count != 0 {
			t.Errorf("GetRowCount() = %d, want 0", count)
		}
	})

	// Insert data
	for i := 0; i < 5; i++ {
		_, _ = driver.db.Exec("INSERT INTO users (name) VALUES (?)", "User")
	}

	t.Run("table with rows", func(t *testing.T) {
		count, err := driver.GetRowCount("users")
		if err != nil {
			t.Fatalf("GetRowCount() error = %v", err)
		}
		if count != 5 {
			t.Errorf("GetRowCount() = %d, want 5", count)
		}
	})
}

func TestSQLiteDriver_QuoteIdentifier(t *testing.T) {
	driver := &SQLiteDriver{}

	tests := []struct {
		name string
		want string
	}{
		{"simple", `"simple"`},
		{"with space", `"with space"`},
		{`with"quote`, `"with""quote"`},
		{"", `""`},
		{"MixedCase", `"MixedCase"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := driver.QuoteIdentifier(tt.name)
			if got != tt.want {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

func TestSQLiteDriver_GetDatabaseType(t *testing.T) {
	driver := &SQLiteDriver{}
	if got := driver.GetDatabaseType(); got != "sqlite" {
		t.Errorf("GetDatabaseType() = %q, want %q", got, "sqlite")
	}
}

func TestSQLiteDriver_DataTypes(t *testing.T) {
	driver := createTestDB(t)
	defer driver.Close()

	// Create table with various data types
	_, err := driver.db.Exec(`
		CREATE TABLE types_test (
			id INTEGER PRIMARY KEY,
			int_col INTEGER,
			real_col REAL,
			text_col TEXT,
			blob_col BLOB,
			null_col TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = driver.db.Exec(`
		INSERT INTO types_test (int_col, real_col, text_col, blob_col, null_col)
		VALUES (42, 3.14, 'hello', X'48454C4C4F', NULL)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	var row map[string]any
	err = driver.StreamRows("types_test", 1, 10, func(rows []map[string]any) error {
		if len(rows) > 0 {
			row = rows[0]
		}
		return nil
	})
	if err != nil {
		t.Fatalf("StreamRows() error = %v", err)
	}

	// Verify types are handled correctly
	if row["null_col"] != nil {
		t.Errorf("null_col = %v, want nil", row["null_col"])
	}

	// Integer column
	if intVal, ok := row["int_col"].(int64); ok {
		if intVal != 42 {
			t.Errorf("int_col = %d, want 42", intVal)
		}
	} else {
		t.Errorf("int_col type = %T, want int64", row["int_col"])
	}

	// Real column
	if realVal, ok := row["real_col"].(float64); ok {
		if realVal != 3.14 {
			t.Errorf("real_col = %f, want 3.14", realVal)
		}
	} else {
		t.Errorf("real_col type = %T, want float64", row["real_col"])
	}

	// Text column (may be string or []byte converted to string)
	if textVal, ok := row["text_col"].(string); ok {
		if textVal != "hello" {
			t.Errorf("text_col = %q, want %q", textVal, "hello")
		}
	} else {
		t.Errorf("text_col type = %T, want string", row["text_col"])
	}
}
