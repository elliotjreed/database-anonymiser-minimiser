package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the full configuration file structure.
type Config struct {
	Connection          Connection              `yaml:"connection" json:"connection"`
	ForeignKeyIntegrity *bool                   `yaml:"foreign_key_integrity,omitempty" json:"foreign_key_integrity,omitempty"`
	Configuration       map[string]*TableConfig `yaml:"configuration" json:"configuration"`
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

// RetainConfig defines how rows should be retained during export.
// It supports two modes:
// 1. Count-based: retain a specific number of rows (e.g., retain: 100)
// 2. Date-based: retain rows after a specific date (e.g., retain: {column_name: "created_at", after_date: "2024-01-01"})
type RetainConfig struct {
	Count      int       // Number of rows to retain (0 = all rows)
	ColumnName string    // Column name for date-based filtering
	AfterDate  time.Time // Only retain rows after this date
}

// IsDateBased returns true if the retain config uses date-based filtering.
func (r *RetainConfig) IsDateBased() bool {
	return r.ColumnName != "" && !r.AfterDate.IsZero()
}

// IsCountBased returns true if the retain config uses count-based limiting.
func (r *RetainConfig) IsCountBased() bool {
	return r.Count > 0
}

// IsEmpty returns true if no retain configuration is set.
func (r *RetainConfig) IsEmpty() bool {
	return r.Count == 0 && r.ColumnName == "" && r.AfterDate.IsZero()
}

// retainConfigRaw is used for parsing the flexible retain format.
type retainConfigRaw struct {
	ColumnName string `yaml:"column_name" json:"column_name"`
	AfterDate  string `yaml:"after_date" json:"after_date"`
}

// UnmarshalYAML implements custom YAML unmarshaling for RetainConfig.
// It supports both integer values and object format.
func (r *RetainConfig) UnmarshalYAML(value *yaml.Node) error {
	// Try to unmarshal as an integer first
	var intVal int
	if err := value.Decode(&intVal); err == nil {
		r.Count = intVal
		return nil
	}

	// Try to unmarshal as an object
	var raw retainConfigRaw
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("retain must be an integer or an object with column_name and after_date: %w", err)
	}

	if raw.ColumnName == "" {
		return fmt.Errorf("retain object requires column_name")
	}
	if raw.AfterDate == "" {
		return fmt.Errorf("retain object requires after_date")
	}

	// Parse the date - support multiple formats
	parsedDate, err := parseDate(raw.AfterDate)
	if err != nil {
		return fmt.Errorf("invalid after_date format %q: %w", raw.AfterDate, err)
	}

	r.ColumnName = raw.ColumnName
	r.AfterDate = parsedDate
	return nil
}

// UnmarshalJSON implements custom JSON unmarshaling for RetainConfig.
func (r *RetainConfig) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as an integer first
	var intVal int
	if err := json.Unmarshal(data, &intVal); err == nil {
		r.Count = intVal
		return nil
	}

	// Try to unmarshal as an object
	var raw retainConfigRaw
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("retain must be an integer or an object with column_name and after_date: %w", err)
	}

	if raw.ColumnName == "" {
		return fmt.Errorf("retain object requires column_name")
	}
	if raw.AfterDate == "" {
		return fmt.Errorf("retain object requires after_date")
	}

	parsedDate, err := parseDate(raw.AfterDate)
	if err != nil {
		return fmt.Errorf("invalid after_date format %q: %w", raw.AfterDate, err)
	}

	r.ColumnName = raw.ColumnName
	r.AfterDate = parsedDate
	return nil
}

// MarshalYAML implements custom YAML marshaling for RetainConfig.
func (r RetainConfig) MarshalYAML() (interface{}, error) {
	if r.IsDateBased() {
		return map[string]string{
			"column_name": r.ColumnName,
			"after_date":  r.AfterDate.Format("2006-01-02"),
		}, nil
	}
	if r.Count > 0 {
		return r.Count, nil
	}
	return nil, nil
}

// MarshalJSON implements custom JSON marshaling for RetainConfig.
func (r RetainConfig) MarshalJSON() ([]byte, error) {
	if r.IsDateBased() {
		return json.Marshal(map[string]string{
			"column_name": r.ColumnName,
			"after_date":  r.AfterDate.Format("2006-01-02"),
		})
	}
	if r.Count > 0 {
		return json.Marshal(r.Count)
	}
	return []byte("null"), nil
}

// parseDate attempts to parse a date string in various formats.
func parseDate(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		time.RFC3339,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("could not parse date, supported formats: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS")
}

// TableConfig defines how a table should be processed.
type TableConfig struct {
	Truncate            bool              `yaml:"truncate,omitempty" json:"truncate,omitempty"`                           // If true, export schema only
	ForeignKeyIntegrity *bool             `yaml:"foreign_key_integrity,omitempty" json:"foreign_key_integrity,omitempty"` // Override global FK integrity setting
	Retain              RetainConfig      `yaml:"retain,omitempty" json:"retain,omitempty"`                               // Row retention config (count or date-based)
	Columns             map[string]string `yaml:"columns,omitempty" json:"columns,omitempty"`                             // Column anonymisation rules
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

// ShouldEnforceFKIntegrity returns whether foreign key integrity should be enforced for a table.
// Table-level settings override the global setting.
func (c *Config) ShouldEnforceFKIntegrity(tableName string) bool {
	if tc := c.GetTableConfig(tableName); tc != nil && tc.ForeignKeyIntegrity != nil {
		return *tc.ForeignKeyIntegrity
	}
	if c.ForeignKeyIntegrity != nil {
		return *c.ForeignKeyIntegrity
	}
	return false
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
