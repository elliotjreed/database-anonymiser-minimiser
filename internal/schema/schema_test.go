package schema

import (
	"errors"
	"testing"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
	"github.com/elliotjreed/database-anonymiser-minimiser/internal/database"
)

// mockDriver implements database.Driver for testing
type mockDriver struct {
	tables     []string
	schemas    map[string]string
	columns    map[string][]database.ColumnInfo
	rowCounts  map[string]int64
	foreignKeys []database.ForeignKey

	// Error injection
	getTablesErr     error
	getSchemaErr     error
	getColumnsErr    error
	getRowCountErr   error
	getForeignKeysErr error
}

func (m *mockDriver) Connect(cfg *config.Connection) error { return nil }
func (m *mockDriver) Close() error                         { return nil }

func (m *mockDriver) GetTables() ([]string, error) {
	if m.getTablesErr != nil {
		return nil, m.getTablesErr
	}
	return m.tables, nil
}

func (m *mockDriver) GetTableSchema(table string) (string, error) {
	if m.getSchemaErr != nil {
		return "", m.getSchemaErr
	}
	if schema, ok := m.schemas[table]; ok {
		return schema, nil
	}
	return "", errors.New("table not found")
}

func (m *mockDriver) GetColumns(table string) ([]database.ColumnInfo, error) {
	if m.getColumnsErr != nil {
		return nil, m.getColumnsErr
	}
	if cols, ok := m.columns[table]; ok {
		return cols, nil
	}
	return nil, nil
}

func (m *mockDriver) GetForeignKeys() ([]database.ForeignKey, error) {
	if m.getForeignKeysErr != nil {
		return nil, m.getForeignKeysErr
	}
	return m.foreignKeys, nil
}

func (m *mockDriver) StreamRows(table string, opts database.StreamOptions, batchSize int, callback database.RowCallback) error {
	return nil
}

func (m *mockDriver) GetRowCount(table string) (int64, error) {
	if m.getRowCountErr != nil {
		return 0, m.getRowCountErr
	}
	if count, ok := m.rowCounts[table]; ok {
		return count, nil
	}
	return 0, nil
}

func (m *mockDriver) QuoteIdentifier(name string) string {
	return "\"" + name + "\""
}

func (m *mockDriver) GetDatabaseType() string {
	return "mock"
}

func TestNewAnalyser(t *testing.T) {
	driver := &mockDriver{}
	analyser := NewAnalyser(driver)

	if analyser == nil {
		t.Fatal("NewAnalyser() returned nil")
	}
	if analyser.driver != driver {
		t.Error("NewAnalyser() did not store driver")
	}
}

func TestGetAllTables(t *testing.T) {
	t.Run("successful retrieval", func(t *testing.T) {
		driver := &mockDriver{
			tables: []string{"users", "orders"},
			schemas: map[string]string{
				"users":  "CREATE TABLE users (id INT);",
				"orders": "CREATE TABLE orders (id INT);",
			},
			columns: map[string][]database.ColumnInfo{
				"users": {
					{Name: "id", DataType: "INT"},
					{Name: "name", DataType: "VARCHAR"},
				},
				"orders": {
					{Name: "id", DataType: "INT"},
				},
			},
			rowCounts: map[string]int64{
				"users":  100,
				"orders": 500,
			},
		}

		analyser := NewAnalyser(driver)
		tables, err := analyser.GetAllTables()

		if err != nil {
			t.Fatalf("GetAllTables() error = %v", err)
		}

		if len(tables) != 2 {
			t.Errorf("GetAllTables() returned %d tables, want 2", len(tables))
		}

		// Check users table
		if tables[0].Name != "users" {
			t.Errorf("tables[0].Name = %q, want %q", tables[0].Name, "users")
		}
		if tables[0].RowCount != 100 {
			t.Errorf("tables[0].RowCount = %d, want 100", tables[0].RowCount)
		}
		if len(tables[0].Columns) != 2 {
			t.Errorf("tables[0].Columns length = %d, want 2", len(tables[0].Columns))
		}
	})

	t.Run("empty database", func(t *testing.T) {
		driver := &mockDriver{
			tables:    []string{},
			schemas:   map[string]string{},
			columns:   map[string][]database.ColumnInfo{},
			rowCounts: map[string]int64{},
		}

		analyser := NewAnalyser(driver)
		tables, err := analyser.GetAllTables()

		if err != nil {
			t.Fatalf("GetAllTables() error = %v", err)
		}

		if len(tables) != 0 {
			t.Errorf("GetAllTables() returned %d tables, want 0", len(tables))
		}
	})

	t.Run("GetTables error", func(t *testing.T) {
		driver := &mockDriver{
			getTablesErr: errors.New("connection failed"),
		}

		analyser := NewAnalyser(driver)
		_, err := analyser.GetAllTables()

		if err == nil {
			t.Error("GetAllTables() expected error")
		}
	})

	t.Run("GetTableSchema error", func(t *testing.T) {
		driver := &mockDriver{
			tables:       []string{"users"},
			getSchemaErr: errors.New("schema error"),
		}

		analyser := NewAnalyser(driver)
		_, err := analyser.GetAllTables()

		if err == nil {
			t.Error("GetAllTables() expected error from GetTableSchema")
		}
	})

	t.Run("GetColumns error", func(t *testing.T) {
		driver := &mockDriver{
			tables:  []string{"users"},
			schemas: map[string]string{"users": "CREATE TABLE users;"},
			getColumnsErr: errors.New("columns error"),
		}

		analyser := NewAnalyser(driver)
		_, err := analyser.GetAllTables()

		if err == nil {
			t.Error("GetAllTables() expected error from GetColumns")
		}
	})

	t.Run("GetRowCount error", func(t *testing.T) {
		driver := &mockDriver{
			tables:  []string{"users"},
			schemas: map[string]string{"users": "CREATE TABLE users;"},
			columns: map[string][]database.ColumnInfo{"users": {}},
			getRowCountErr: errors.New("count error"),
		}

		analyser := NewAnalyser(driver)
		_, err := analyser.GetAllTables()

		if err == nil {
			t.Error("GetAllTables() expected error from GetRowCount")
		}
	})
}

func TestSortTablesByDependency(t *testing.T) {
	t.Run("no dependencies", func(t *testing.T) {
		driver := &mockDriver{
			foreignKeys: []database.ForeignKey{},
		}

		tables := []TableInfo{
			{Name: "users"},
			{Name: "products"},
			{Name: "orders"},
		}

		analyser := NewAnalyser(driver)
		sorted, err := analyser.SortTablesByDependency(tables)

		if err != nil {
			t.Fatalf("SortTablesByDependency() error = %v", err)
		}

		if len(sorted) != 3 {
			t.Errorf("SortTablesByDependency() returned %d tables, want 3", len(sorted))
		}
	})

	t.Run("linear dependencies", func(t *testing.T) {
		// orders -> users (orders depends on users)
		driver := &mockDriver{
			foreignKeys: []database.ForeignKey{
				{Table: "orders", Column: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
			},
		}

		tables := []TableInfo{
			{Name: "orders"},
			{Name: "users"},
		}

		analyser := NewAnalyser(driver)
		sorted, err := analyser.SortTablesByDependency(tables)

		if err != nil {
			t.Fatalf("SortTablesByDependency() error = %v", err)
		}

		// users should come before orders
		usersIdx := -1
		ordersIdx := -1
		for i, t := range sorted {
			if t.Name == "users" {
				usersIdx = i
			}
			if t.Name == "orders" {
				ordersIdx = i
			}
		}

		if usersIdx > ordersIdx {
			t.Errorf("users should come before orders: users=%d, orders=%d", usersIdx, ordersIdx)
		}
	})

	t.Run("complex dependencies", func(t *testing.T) {
		// order_items -> orders -> users
		// order_items -> products
		driver := &mockDriver{
			foreignKeys: []database.ForeignKey{
				{Table: "orders", Column: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
				{Table: "order_items", Column: "order_id", ReferencedTable: "orders", ReferencedColumn: "id"},
				{Table: "order_items", Column: "product_id", ReferencedTable: "products", ReferencedColumn: "id"},
			},
		}

		tables := []TableInfo{
			{Name: "order_items"},
			{Name: "orders"},
			{Name: "users"},
			{Name: "products"},
		}

		analyser := NewAnalyser(driver)
		sorted, err := analyser.SortTablesByDependency(tables)

		if err != nil {
			t.Fatalf("SortTablesByDependency() error = %v", err)
		}

		// Build position map
		pos := make(map[string]int)
		for i, t := range sorted {
			pos[t.Name] = i
		}

		// users should come before orders
		if pos["users"] > pos["orders"] {
			t.Errorf("users should come before orders")
		}
		// orders should come before order_items
		if pos["orders"] > pos["order_items"] {
			t.Errorf("orders should come before order_items")
		}
		// products should come before order_items
		if pos["products"] > pos["order_items"] {
			t.Errorf("products should come before order_items")
		}
	})

	t.Run("self-referencing table", func(t *testing.T) {
		// employees references itself (manager_id -> id)
		driver := &mockDriver{
			foreignKeys: []database.ForeignKey{
				{Table: "employees", Column: "manager_id", ReferencedTable: "employees", ReferencedColumn: "id"},
			},
		}

		tables := []TableInfo{
			{Name: "employees"},
		}

		analyser := NewAnalyser(driver)
		sorted, err := analyser.SortTablesByDependency(tables)

		if err != nil {
			t.Fatalf("SortTablesByDependency() error = %v", err)
		}

		if len(sorted) != 1 {
			t.Errorf("SortTablesByDependency() returned %d tables, want 1", len(sorted))
		}
	})

	t.Run("circular dependency", func(t *testing.T) {
		// a -> b -> a (cycle)
		driver := &mockDriver{
			foreignKeys: []database.ForeignKey{
				{Table: "a", Column: "b_id", ReferencedTable: "b", ReferencedColumn: "id"},
				{Table: "b", Column: "a_id", ReferencedTable: "a", ReferencedColumn: "id"},
			},
		}

		tables := []TableInfo{
			{Name: "a"},
			{Name: "b"},
		}

		analyser := NewAnalyser(driver)
		sorted, err := analyser.SortTablesByDependency(tables)

		// Should still return all tables even with cycle
		if err != nil {
			t.Fatalf("SortTablesByDependency() error = %v", err)
		}

		if len(sorted) != 2 {
			t.Errorf("SortTablesByDependency() returned %d tables, want 2", len(sorted))
		}
	})

	t.Run("GetForeignKeys error", func(t *testing.T) {
		driver := &mockDriver{
			getForeignKeysErr: errors.New("fk error"),
		}

		tables := []TableInfo{{Name: "users"}}

		analyser := NewAnalyser(driver)
		_, err := analyser.SortTablesByDependency(tables)

		if err == nil {
			t.Error("SortTablesByDependency() expected error")
		}
	})

	t.Run("FK references table not in list", func(t *testing.T) {
		// orders references users, but users is not in the table list
		driver := &mockDriver{
			foreignKeys: []database.ForeignKey{
				{Table: "orders", Column: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
			},
		}

		tables := []TableInfo{
			{Name: "orders"},
			// users is not in the list
		}

		analyser := NewAnalyser(driver)
		sorted, err := analyser.SortTablesByDependency(tables)

		if err != nil {
			t.Fatalf("SortTablesByDependency() error = %v", err)
		}

		// Should still return the table
		if len(sorted) != 1 {
			t.Errorf("SortTablesByDependency() returned %d tables, want 1", len(sorted))
		}
	})
}

func TestGetForeignKeyMap(t *testing.T) {
	t.Run("successful retrieval", func(t *testing.T) {
		driver := &mockDriver{
			foreignKeys: []database.ForeignKey{
				{Table: "orders", Column: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
				{Table: "orders", Column: "product_id", ReferencedTable: "products", ReferencedColumn: "id"},
				{Table: "reviews", Column: "user_id", ReferencedTable: "users", ReferencedColumn: "id"},
			},
		}

		analyser := NewAnalyser(driver)
		fkMap, err := analyser.GetForeignKeyMap()

		if err != nil {
			t.Fatalf("GetForeignKeyMap() error = %v", err)
		}

		// orders should have 2 FKs
		if len(fkMap["orders"]) != 2 {
			t.Errorf("fkMap[orders] has %d FKs, want 2", len(fkMap["orders"]))
		}

		// reviews should have 1 FK
		if len(fkMap["reviews"]) != 1 {
			t.Errorf("fkMap[reviews] has %d FKs, want 1", len(fkMap["reviews"]))
		}

		// users should have no FKs (as the table itself, not as referenced)
		if len(fkMap["users"]) != 0 {
			t.Errorf("fkMap[users] has %d FKs, want 0", len(fkMap["users"]))
		}
	})

	t.Run("no foreign keys", func(t *testing.T) {
		driver := &mockDriver{
			foreignKeys: []database.ForeignKey{},
		}

		analyser := NewAnalyser(driver)
		fkMap, err := analyser.GetForeignKeyMap()

		if err != nil {
			t.Fatalf("GetForeignKeyMap() error = %v", err)
		}

		if len(fkMap) != 0 {
			t.Errorf("GetForeignKeyMap() returned %d entries, want 0", len(fkMap))
		}
	})

	t.Run("error handling", func(t *testing.T) {
		driver := &mockDriver{
			getForeignKeysErr: errors.New("fk error"),
		}

		analyser := NewAnalyser(driver)
		_, err := analyser.GetForeignKeyMap()

		if err == nil {
			t.Error("GetForeignKeyMap() expected error")
		}
	})
}

func TestTableInfoStruct(t *testing.T) {
	info := TableInfo{
		Name:       "users",
		CreateStmt: "CREATE TABLE users (id INT);",
		Columns: []database.ColumnInfo{
			{Name: "id", DataType: "INT"},
		},
		RowCount: 100,
	}

	if info.Name != "users" {
		t.Errorf("Name = %q, want %q", info.Name, "users")
	}
	if info.RowCount != 100 {
		t.Errorf("RowCount = %d, want 100", info.RowCount)
	}
	if len(info.Columns) != 1 {
		t.Errorf("len(Columns) = %d, want 1", len(info.Columns))
	}
}

func TestTopologicalSort_EdgeCases(t *testing.T) {
	t.Run("empty table list", func(t *testing.T) {
		tables := []TableInfo{}
		deps := map[string][]string{}

		sorted, err := topologicalSort(tables, deps)
		if err != nil {
			t.Fatalf("topologicalSort() error = %v", err)
		}

		if len(sorted) != 0 {
			t.Errorf("topologicalSort() returned %d tables, want 0", len(sorted))
		}
	})

	t.Run("single table no deps", func(t *testing.T) {
		tables := []TableInfo{{Name: "users"}}
		deps := map[string][]string{"users": {}}

		sorted, err := topologicalSort(tables, deps)
		if err != nil {
			t.Fatalf("topologicalSort() error = %v", err)
		}

		if len(sorted) != 1 || sorted[0].Name != "users" {
			t.Errorf("topologicalSort() = %v, want [{users}]", sorted)
		}
	})

	t.Run("diamond dependency", func(t *testing.T) {
		//     A
		//    / \
		//   B   C
		//    \ /
		//     D
		tables := []TableInfo{
			{Name: "D"},
			{Name: "B"},
			{Name: "C"},
			{Name: "A"},
		}
		deps := map[string][]string{
			"A": {},
			"B": {"A"},
			"C": {"A"},
			"D": {"B", "C"},
		}

		sorted, err := topologicalSort(tables, deps)
		if err != nil {
			t.Fatalf("topologicalSort() error = %v", err)
		}

		pos := make(map[string]int)
		for i, t := range sorted {
			pos[t.Name] = i
		}

		// A must come before B and C
		if pos["A"] > pos["B"] || pos["A"] > pos["C"] {
			t.Error("A should come before B and C")
		}
		// B and C must come before D
		if pos["B"] > pos["D"] || pos["C"] > pos["D"] {
			t.Error("B and C should come before D")
		}
	})
}
