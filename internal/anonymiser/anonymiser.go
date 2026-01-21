package anonymiser

import (
	"regexp"
	"sync"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
)

var (
	// fakerPattern matches {{faker.funcName}} templates.
	fakerPattern = regexp.MustCompile(`\{\{faker\.(\w+)\}\}`)
)

// Anonymiser handles data anonymisation based on configuration.
type Anonymiser struct {
	config *config.Config

	// consistencyMap maintains value mappings for referential integrity.
	// Key format: "column:originalValue" -> anonymised value
	consistencyMap map[string]string
	mu             sync.RWMutex
}

// New creates a new Anonymiser instance.
func New(cfg *config.Config) *Anonymiser {
	return &Anonymiser{
		config:         cfg,
		consistencyMap: make(map[string]string),
	}
}

// AnonymiseRow applies anonymisation rules to a row of data.
func (a *Anonymiser) AnonymiseRow(tableName string, row map[string]any) map[string]any {
	tableConfig := a.config.GetTableConfig(tableName)
	if tableConfig == nil || tableConfig.Columns == nil {
		return row
	}

	result := make(map[string]any, len(row))
	for col, val := range row {
		result[col] = val
	}

	for col, rule := range tableConfig.Columns {
		if _, exists := result[col]; !exists {
			continue
		}

		// Handle null rule (set to NULL)
		if rule == "null" || rule == "" {
			result[col] = nil
			continue
		}

		// Get original value for consistency mapping
		originalVal := result[col]
		var originalStr string
		if originalVal != nil {
			switch v := originalVal.(type) {
			case string:
				originalStr = v
			default:
				// For non-string types, convert to string for mapping
				originalStr = ""
			}
		}

		// Check for faker template
		if matches := fakerPattern.FindStringSubmatch(rule); matches != nil {
			funcName := matches[1]

			// Check consistency map first
			a.mu.RLock()
			key := col + ":" + originalStr
			if cached, ok := a.consistencyMap[key]; ok {
				a.mu.RUnlock()
				result[col] = cached
				continue
			}
			a.mu.RUnlock()

			// Generate new value
			newVal := GenerateFakeValue(funcName)

			// Store in consistency map
			if originalStr != "" {
				a.mu.Lock()
				a.consistencyMap[key] = newVal
				a.mu.Unlock()
			}

			result[col] = newVal
		} else {
			// Static replacement value
			result[col] = rule
		}
	}

	return result
}

// ShouldTruncate returns true if the table should be truncated (schema only).
func (a *Anonymiser) ShouldTruncate(tableName string) bool {
	tableConfig := a.config.GetTableConfig(tableName)
	if tableConfig == nil {
		return false
	}
	return tableConfig.Truncate
}

// GetRetainConfig returns the retain configuration for a table.
func (a *Anonymiser) GetRetainConfig(tableName string) config.RetainConfig {
	tableConfig := a.config.GetTableConfig(tableName)
	if tableConfig == nil {
		return config.RetainConfig{}
	}
	return tableConfig.Retain
}

// HasAnonymisation returns true if the table has any anonymisation rules.
func (a *Anonymiser) HasAnonymisation(tableName string) bool {
	tableConfig := a.config.GetTableConfig(tableName)
	if tableConfig == nil {
		return false
	}
	return len(tableConfig.Columns) > 0
}

// ParseFakerTemplate extracts the faker function name from a template.
// Returns the function name and true if it's a faker template, otherwise empty string and false.
func ParseFakerTemplate(template string) (string, bool) {
	matches := fakerPattern.FindStringSubmatch(template)
	if matches == nil {
		return "", false
	}
	return matches[1], true
}

// IsFakerTemplate checks if a string is a faker template.
func IsFakerTemplate(s string) bool {
	return fakerPattern.MatchString(s)
}

// GetAnonymisedColumns returns the list of columns that will be anonymised for a table.
func (a *Anonymiser) GetAnonymisedColumns(tableName string) []string {
	tableConfig := a.config.GetTableConfig(tableName)
	if tableConfig == nil || tableConfig.Columns == nil {
		return nil
	}

	columns := make([]string, 0, len(tableConfig.Columns))
	for col := range tableConfig.Columns {
		columns = append(columns, col)
	}
	return columns
}

// ClearConsistencyMap clears the consistency map (useful for testing).
func (a *Anonymiser) ClearConsistencyMap() {
	a.mu.Lock()
	a.consistencyMap = make(map[string]string)
	a.mu.Unlock()
}

// ValidateRules validates anonymisation rules for known faker functions.
func (a *Anonymiser) ValidateRules() []string {
	var errors []string

	if a.config.Configuration == nil {
		return errors
	}

	for tableName, tableConfig := range a.config.Configuration {
		if tableConfig == nil || tableConfig.Columns == nil {
			continue
		}

		for col, rule := range tableConfig.Columns {
			if funcName, isFaker := ParseFakerTemplate(rule); isFaker {
				if GetFakerFunc(funcName) == nil {
					errors = append(errors, "unknown faker function '"+funcName+"' for "+tableName+"."+col)
				}
			}
		}
	}

	return errors
}
