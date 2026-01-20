package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_YAML(t *testing.T) {
	content := `
connection:
  type: mysql
  host: localhost
  port: 3306
  username: root
  password: secret
  database_name: testdb
configuration:
  users:
    truncate: false
    retain: 100
    columns:
      email: "{{faker.email}}"
      name: "{{faker.name}}"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Connection.Type != "mysql" {
		t.Errorf("Connection.Type = %q, want %q", cfg.Connection.Type, "mysql")
	}
	if cfg.Connection.Host != "localhost" {
		t.Errorf("Connection.Host = %q, want %q", cfg.Connection.Host, "localhost")
	}
	if cfg.Connection.Port != 3306 {
		t.Errorf("Connection.Port = %d, want %d", cfg.Connection.Port, 3306)
	}
	if cfg.Connection.DatabaseName != "testdb" {
		t.Errorf("Connection.DatabaseName = %q, want %q", cfg.Connection.DatabaseName, "testdb")
	}

	tableConfig := cfg.GetTableConfig("users")
	if tableConfig == nil {
		t.Fatal("GetTableConfig(users) returned nil")
	}
	if tableConfig.Retain != 100 {
		t.Errorf("tableConfig.Retain = %d, want %d", tableConfig.Retain, 100)
	}
	if tableConfig.Columns["email"] != "{{faker.email}}" {
		t.Errorf("tableConfig.Columns[email] = %q, want %q", tableConfig.Columns["email"], "{{faker.email}}")
	}
}

func TestLoad_JSON(t *testing.T) {
	content := `{
  "connection": {
    "type": "postgres",
    "host": "localhost",
    "port": 5432,
    "username": "postgres",
    "password": "secret",
    "database_name": "testdb"
  },
  "configuration": {
    "orders": {
      "truncate": true
    }
  }
}`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Connection.Type != "postgres" {
		t.Errorf("Connection.Type = %q, want %q", cfg.Connection.Type, "postgres")
	}

	tableConfig := cfg.GetTableConfig("orders")
	if tableConfig == nil {
		t.Fatal("GetTableConfig(orders) returned nil")
	}
	if !tableConfig.Truncate {
		t.Error("tableConfig.Truncate = false, want true")
	}
}

func TestLoad_SQLite(t *testing.T) {
	content := `
connection:
  type: sqlite
  file: /tmp/test.db
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Connection.Type != "sqlite" {
		t.Errorf("Connection.Type = %q, want %q", cfg.Connection.Type, "sqlite")
	}
	if cfg.Connection.File != "/tmp/test.db" {
		t.Errorf("Connection.File = %q, want %q", cfg.Connection.File, "/tmp/test.db")
	}
}

func TestLoad_UnknownExtension_YAML(t *testing.T) {
	content := `
connection:
  type: mysql
  host: localhost
  database_name: testdb
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.txt")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Connection.Type != "mysql" {
		t.Errorf("Connection.Type = %q, want %q", cfg.Connection.Type, "mysql")
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("Load() expected error for non-existent file, got nil")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	content := `
connection:
  type: mysql
  host: localhost
  database_name: testdb
  invalid yaml: [
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	_, err := Load(configPath)
	if err == nil {
		t.Error("Load() expected error for invalid YAML, got nil")
	}
}

func TestLoad_InvalidJSON(t *testing.T) {
	content := `{"connection": invalid}`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	_, err := Load(configPath)
	if err == nil {
		t.Error("Load() expected error for invalid JSON, got nil")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid mysql config",
			config: Config{
				Connection: Connection{
					Type:         "mysql",
					Host:         "localhost",
					DatabaseName: "testdb",
				},
			},
			wantErr: false,
		},
		{
			name: "valid postgres config",
			config: Config{
				Connection: Connection{
					Type:         "postgres",
					Host:         "localhost",
					DatabaseName: "testdb",
				},
			},
			wantErr: false,
		},
		{
			name: "valid sqlite config",
			config: Config{
				Connection: Connection{
					Type: "sqlite",
					File: "/tmp/test.db",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid connection type",
			config: Config{
				Connection: Connection{
					Type: "oracle",
					Host: "localhost",
				},
			},
			wantErr: true,
		},
		{
			name: "mysql missing host",
			config: Config{
				Connection: Connection{
					Type:         "mysql",
					DatabaseName: "testdb",
				},
			},
			wantErr: true,
		},
		{
			name: "mysql missing database_name",
			config: Config{
				Connection: Connection{
					Type: "mysql",
					Host: "localhost",
				},
			},
			wantErr: true,
		},
		{
			name: "sqlite missing file",
			config: Config{
				Connection: Connection{
					Type: "sqlite",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDSN(t *testing.T) {
	tests := []struct {
		name string
		conn Connection
		want string
	}{
		{
			name: "mysql with default port",
			conn: Connection{
				Type:         "mysql",
				Host:         "localhost",
				Username:     "root",
				Password:     "secret",
				DatabaseName: "testdb",
			},
			want: "root:secret@tcp(localhost:3306)/testdb?parseTime=true&multiStatements=true",
		},
		{
			name: "mysql with custom port",
			conn: Connection{
				Type:         "mysql",
				Host:         "localhost",
				Port:         3307,
				Username:     "root",
				Password:     "secret",
				DatabaseName: "testdb",
			},
			want: "root:secret@tcp(localhost:3307)/testdb?parseTime=true&multiStatements=true",
		},
		{
			name: "postgres with default port",
			conn: Connection{
				Type:         "postgres",
				Host:         "localhost",
				Username:     "postgres",
				Password:     "secret",
				DatabaseName: "testdb",
			},
			want: "host=localhost port=5432 user=postgres password=secret dbname=testdb sslmode=disable",
		},
		{
			name: "postgres with custom port",
			conn: Connection{
				Type:         "postgres",
				Host:         "localhost",
				Port:         5433,
				Username:     "postgres",
				Password:     "secret",
				DatabaseName: "testdb",
			},
			want: "host=localhost port=5433 user=postgres password=secret dbname=testdb sslmode=disable",
		},
		{
			name: "sqlite",
			conn: Connection{
				Type: "sqlite",
				File: "/tmp/test.db",
			},
			want: "/tmp/test.db",
		},
		{
			name: "unknown type",
			conn: Connection{
				Type: "oracle",
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.conn.DSN()
			if got != tt.want {
				t.Errorf("DSN() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetTableConfig(t *testing.T) {
	cfg := &Config{
		Configuration: map[string]*TableConfig{
			"users": {
				Truncate: false,
				Retain:   100,
			},
			"orders": {
				Truncate: true,
			},
		},
	}

	t.Run("existing table", func(t *testing.T) {
		tc := cfg.GetTableConfig("users")
		if tc == nil {
			t.Fatal("GetTableConfig(users) returned nil")
		}
		if tc.Retain != 100 {
			t.Errorf("Retain = %d, want %d", tc.Retain, 100)
		}
	})

	t.Run("non-existent table", func(t *testing.T) {
		tc := cfg.GetTableConfig("products")
		if tc != nil {
			t.Error("GetTableConfig(products) should return nil")
		}
	})

	t.Run("nil configuration", func(t *testing.T) {
		emptyCfg := &Config{}
		tc := emptyCfg.GetTableConfig("users")
		if tc != nil {
			t.Error("GetTableConfig on nil Configuration should return nil")
		}
	})
}

func TestSave(t *testing.T) {
	cfg := &Config{
		Connection: Connection{
			Type:         "mysql",
			Host:         "localhost",
			Port:         3306,
			Username:     "root",
			Password:     "secret",
			DatabaseName: "testdb",
		},
		Configuration: map[string]*TableConfig{
			"users": {
				Truncate: false,
				Retain:   100,
				Columns: map[string]string{
					"email": "{{faker.email}}",
				},
			},
		},
	}

	t.Run("save as YAML", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "config.yaml")

		if err := cfg.Save(path); err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Load it back
		loaded, err := Load(path)
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}

		if loaded.Connection.Type != cfg.Connection.Type {
			t.Errorf("Connection.Type = %q, want %q", loaded.Connection.Type, cfg.Connection.Type)
		}
		if loaded.Connection.Host != cfg.Connection.Host {
			t.Errorf("Connection.Host = %q, want %q", loaded.Connection.Host, cfg.Connection.Host)
		}
	})

	t.Run("save as JSON", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "config.json")

		if err := cfg.Save(path); err != nil {
			t.Fatalf("Save() error = %v", err)
		}

		// Load it back
		loaded, err := Load(path)
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}

		if loaded.Connection.Type != cfg.Connection.Type {
			t.Errorf("Connection.Type = %q, want %q", loaded.Connection.Type, cfg.Connection.Type)
		}
	})
}

func TestAddTable(t *testing.T) {
	t.Run("add to nil configuration", func(t *testing.T) {
		cfg := &Config{}
		added := cfg.AddTable("users", &TableConfig{Truncate: true})

		if !added {
			t.Error("AddTable() returned false, want true")
		}
		if cfg.Configuration == nil {
			t.Error("Configuration should be initialized")
		}
		if cfg.Configuration["users"] == nil {
			t.Error("Table 'users' should exist")
		}
	})

	t.Run("add new table", func(t *testing.T) {
		cfg := &Config{
			Configuration: map[string]*TableConfig{
				"users": {Truncate: false},
			},
		}

		added := cfg.AddTable("orders", &TableConfig{Truncate: true})
		if !added {
			t.Error("AddTable() returned false, want true")
		}
		if cfg.Configuration["orders"] == nil {
			t.Error("Table 'orders' should exist")
		}
	})

	t.Run("add existing table", func(t *testing.T) {
		cfg := &Config{
			Configuration: map[string]*TableConfig{
				"users": {Truncate: false},
			},
		}

		added := cfg.AddTable("users", &TableConfig{Truncate: true})
		if added {
			t.Error("AddTable() returned true for existing table, want false")
		}
		// Original config should be unchanged
		if cfg.Configuration["users"].Truncate {
			t.Error("Existing table config should not be modified")
		}
	})
}

func TestHasTable(t *testing.T) {
	cfg := &Config{
		Configuration: map[string]*TableConfig{
			"users": {Truncate: false},
		},
	}

	t.Run("existing table", func(t *testing.T) {
		if !cfg.HasTable("users") {
			t.Error("HasTable(users) = false, want true")
		}
	})

	t.Run("non-existent table", func(t *testing.T) {
		if cfg.HasTable("orders") {
			t.Error("HasTable(orders) = true, want false")
		}
	})

	t.Run("nil configuration", func(t *testing.T) {
		emptyCfg := &Config{}
		if emptyCfg.HasTable("users") {
			t.Error("HasTable on nil Configuration should return false")
		}
	})
}

func TestListTables(t *testing.T) {
	t.Run("with tables", func(t *testing.T) {
		cfg := &Config{
			Configuration: map[string]*TableConfig{
				"users":  {Truncate: false},
				"orders": {Truncate: true},
			},
		}

		tables := cfg.ListTables()
		if len(tables) != 2 {
			t.Errorf("len(ListTables()) = %d, want 2", len(tables))
		}

		hasUsers := false
		hasOrders := false
		for _, t := range tables {
			if t == "users" {
				hasUsers = true
			}
			if t == "orders" {
				hasOrders = true
			}
		}
		if !hasUsers {
			t.Error("ListTables() should contain 'users'")
		}
		if !hasOrders {
			t.Error("ListTables() should contain 'orders'")
		}
	})

	t.Run("empty configuration", func(t *testing.T) {
		cfg := &Config{
			Configuration: map[string]*TableConfig{},
		}

		tables := cfg.ListTables()
		if len(tables) != 0 {
			t.Errorf("len(ListTables()) = %d, want 0", len(tables))
		}
	})

	t.Run("nil configuration", func(t *testing.T) {
		cfg := &Config{}

		tables := cfg.ListTables()
		if tables != nil {
			t.Error("ListTables() on nil Configuration should return nil")
		}
	})
}
