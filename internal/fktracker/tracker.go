package fktracker

import (
	"fmt"
	"sync"
)

// Tracker tracks exported primary key values to enable foreign key integrity filtering.
// It maintains a map of table.column -> set of exported values, allowing child tables
// to be filtered to only include rows with valid foreign key references.
type Tracker struct {
	mu           sync.RWMutex
	exportedKeys map[string]map[any]struct{} // "table.column" -> set of values
}

// NewTracker creates a new FK value tracker.
func NewTracker() *Tracker {
	return &Tracker{
		exportedKeys: make(map[string]map[any]struct{}),
	}
}

// makeKey creates a lookup key from table and column names.
func makeKey(table, column string) string {
	return fmt.Sprintf("%s.%s", table, column)
}

// RecordValue records that a value was exported for a given table.column.
func (t *Tracker) RecordValue(table, column string, value any) {
	if value == nil {
		return // Don't track NULL values
	}

	key := makeKey(table, column)

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.exportedKeys[key] == nil {
		t.exportedKeys[key] = make(map[any]struct{})
	}

	// Normalize the value for consistent comparison
	normalised := normalizeValue(value)
	t.exportedKeys[key][normalised] = struct{}{}
}

// HasValue checks if a value exists in the exported set for a table.column.
func (t *Tracker) HasValue(table, column string, value any) bool {
	if value == nil {
		return true // NULL values are always allowed (valid in SQL)
	}

	key := makeKey(table, column)

	t.mu.RLock()
	defer t.mu.RUnlock()

	values, exists := t.exportedKeys[key]
	if !exists {
		return false
	}

	normalised := normalizeValue(value)
	_, found := values[normalised]
	return found
}

// GetExportedValues returns all exported values for a table.column.
// Returns nil if no values have been recorded for this table.column.
func (t *Tracker) GetExportedValues(table, column string) []any {
	key := makeKey(table, column)

	t.mu.RLock()
	defer t.mu.RUnlock()

	values, exists := t.exportedKeys[key]
	if !exists || len(values) == 0 {
		return nil
	}

	result := make([]any, 0, len(values))
	for v := range values {
		result = append(result, v)
	}
	return result
}

// GetExportedCount returns the count of exported values for a table.column.
func (t *Tracker) GetExportedCount(table, column string) int {
	key := makeKey(table, column)

	t.mu.RLock()
	defer t.mu.RUnlock()

	values, exists := t.exportedKeys[key]
	if !exists {
		return 0
	}
	return len(values)
}

// normalizeValue normalizes values for consistent comparison.
// Different database drivers may return the same value as different types
// (e.g., int vs int64), so we normalize to comparable types.
func normalizeValue(value any) any {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		// Handle potential overflow, but this is rare for PKs
		return int64(v)
	case float32:
		return float64(v)
	case []byte:
		return string(v)
	default:
		return v
	}
}
