package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the full configuration file structure.
type Config struct {
	Connection    Connection              `yaml:"connection" json:"connection"`
	Configuration map[string]*TableConfig `yaml:"configuration" json:"configuration"`
}

// Connection holds database connection parameters.
type Connection struct {
	Type         string `yaml:"type" json:"type"`                                       // mysql, postgres, sqlite
	Host         string `yaml:"host,omitempty" json:"host,omitempty"`                   // Database host
	Port         int    `yaml:"port,omitempty" json:"port,omitempty"`                   // Database port
	Username     string `yaml:"username,omitempty" json:"username,omitempty"`           // Database username
	Password     string `yaml:"password,omitempty" json:"password,omitempty"`           // Database password
	DatabaseName string `yaml:"database_name,omitempty" json:"database_name,omitempty"` // Database name
	File         string `yaml:"file,omitempty" json:"file,omitempty"`                   // SQLite file path
}

// TableConfig defines how a table should be processed.
type TableConfig struct {
	Truncate bool              `yaml:"truncate,omitempty" json:"truncate,omitempty"` // If true, export schema only
	Retain   int               `yaml:"retain,omitempty" json:"retain,omitempty"`     // Limit number of rows (0 = all)
	Columns  map[string]string `yaml:"columns,omitempty" json:"columns,omitempty"`   // Column anonymisation rules
}

// Load reads and parses a configuration file (YAML or JSON).
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	default:
		// Try YAML first, then JSON
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			if err := json.Unmarshal(data, &cfg); err != nil {
				return nil, fmt.Errorf("failed to parse config (tried YAML and JSON)")
			}
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

// Validate checks that the configuration is valid.
func (c *Config) Validate() error {
	validTypes := map[string]bool{"mysql": true, "postgres": true, "sqlite": true}
	if !validTypes[c.Connection.Type] {
		return fmt.Errorf("invalid connection type %q, must be mysql, postgres, or sqlite", c.Connection.Type)
	}

	if c.Connection.Type == "sqlite" {
		if c.Connection.File == "" {
			return fmt.Errorf("sqlite connection requires 'file' parameter")
		}
	} else {
		if c.Connection.Host == "" {
			return fmt.Errorf("connection requires 'host' parameter")
		}
		if c.Connection.DatabaseName == "" {
			return fmt.Errorf("connection requires 'database_name' parameter")
		}
	}

	return nil
}

// GetTableConfig returns the configuration for a specific table.
// Returns nil if no specific config exists (full export).
func (c *Config) GetTableConfig(tableName string) *TableConfig {
	if c.Configuration == nil {
		return nil
	}
	return c.Configuration[tableName]
}

// DSN returns the connection string for the database.
func (c *Connection) DSN() string {
	switch c.Type {
	case "mysql":
		port := c.Port
		if port == 0 {
			port = 3306
		}
		// user:password@tcp(host:port)/database
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
			c.Username, c.Password, c.Host, port, c.DatabaseName)
	case "postgres":
		port := c.Port
		if port == 0 {
			port = 5432
		}
		// postgres://user:password@host:port/database?sslmode=disable
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			c.Host, port, c.Username, c.Password, c.DatabaseName)
	case "sqlite":
		return c.File
	default:
		return ""
	}
}

// Save writes the configuration to a file in YAML or JSON format.
// The format is determined by the file extension.
func (c *Config) Save(path string) error {
	ext := strings.ToLower(filepath.Ext(path))

	var data []byte
	var err error

	switch ext {
	case ".json":
		data, err = json.MarshalIndent(c, "", "  ")
	default:
		// Default to YAML
		data, err = yaml.Marshal(c)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// AddTable adds a new table to the configuration if it doesn't already exist.
// Returns true if the table was added, false if it already existed.
func (c *Config) AddTable(tableName string, tableConfig *TableConfig) bool {
	if c.Configuration == nil {
		c.Configuration = make(map[string]*TableConfig)
	}

	if _, exists := c.Configuration[tableName]; exists {
		return false
	}

	c.Configuration[tableName] = tableConfig
	return true
}

// HasTable checks if a table exists in the configuration.
func (c *Config) HasTable(tableName string) bool {
	if c.Configuration == nil {
		return false
	}
	_, exists := c.Configuration[tableName]
	return exists
}

// ListTables returns all table names in the configuration.
func (c *Config) ListTables() []string {
	if c.Configuration == nil {
		return nil
	}

	tables := make([]string, 0, len(c.Configuration))
	for name := range c.Configuration {
		tables = append(tables, name)
	}
	return tables
}
