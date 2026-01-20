package database

import (
	"testing"
)

func TestNewDriver(t *testing.T) {
	tests := []struct {
		name    string
		dbType  string
		wantErr bool
		wantType string
	}{
		{
			name:     "mysql driver",
			dbType:   "mysql",
			wantErr:  false,
			wantType: "mysql",
		},
		{
			name:     "postgres driver",
			dbType:   "postgres",
			wantErr:  false,
			wantType: "postgres",
		},
		{
			name:     "sqlite driver",
			dbType:   "sqlite",
			wantErr:  false,
			wantType: "sqlite",
		},
		{
			name:    "unsupported driver",
			dbType:  "oracle",
			wantErr: true,
		},
		{
			name:    "empty driver",
			dbType:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver, err := NewDriver(tt.dbType)

			if (err != nil) != tt.wantErr {
				t.Errorf("NewDriver(%q) error = %v, wantErr %v", tt.dbType, err, tt.wantErr)
				return
			}

			if !tt.wantErr && driver.GetDatabaseType() != tt.wantType {
				t.Errorf("GetDatabaseType() = %q, want %q", driver.GetDatabaseType(), tt.wantType)
			}
		})
	}
}

func TestForeignKeyStruct(t *testing.T) {
	fk := ForeignKey{
		Table:            "orders",
		Column:           "user_id",
		ReferencedTable:  "users",
		ReferencedColumn: "id",
	}

	if fk.Table != "orders" {
		t.Errorf("Table = %q, want %q", fk.Table, "orders")
	}
	if fk.Column != "user_id" {
		t.Errorf("Column = %q, want %q", fk.Column, "user_id")
	}
	if fk.ReferencedTable != "users" {
		t.Errorf("ReferencedTable = %q, want %q", fk.ReferencedTable, "users")
	}
	if fk.ReferencedColumn != "id" {
		t.Errorf("ReferencedColumn = %q, want %q", fk.ReferencedColumn, "id")
	}
}

func TestColumnInfoStruct(t *testing.T) {
	col := ColumnInfo{
		Name:       "email",
		DataType:   "VARCHAR(255)",
		IsNullable: true,
	}

	if col.Name != "email" {
		t.Errorf("Name = %q, want %q", col.Name, "email")
	}
	if col.DataType != "VARCHAR(255)" {
		t.Errorf("DataType = %q, want %q", col.DataType, "VARCHAR(255)")
	}
	if !col.IsNullable {
		t.Error("IsNullable = false, want true")
	}
}
