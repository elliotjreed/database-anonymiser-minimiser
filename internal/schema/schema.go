package schema

import (
	"fmt"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/database"
)

// TableInfo holds schema information for a table.
type TableInfo struct {
	Name       string
	CreateStmt string
	Columns    []database.ColumnInfo
	RowCount   int64
}

// Analyser handles schema extraction and analysis.
type Analyser struct {
	driver database.Driver
}

// NewAnalyser creates a new schema analyser.
func NewAnalyser(driver database.Driver) *Analyser {
	return &Analyser{driver: driver}
}

// GetAllTables returns information about all tables in the database.
func (a *Analyser) GetAllTables() ([]TableInfo, error) {
	tables, err := a.driver.GetTables()
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}

	var tableInfos []TableInfo
	for _, table := range tables {
		schema, err := a.driver.GetTableSchema(table)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema for %s: %w", table, err)
		}

		columns, err := a.driver.GetColumns(table)
		if err != nil {
			return nil, fmt.Errorf("failed to get columns for %s: %w", table, err)
		}

		rowCount, err := a.driver.GetRowCount(table)
		if err != nil {
			return nil, fmt.Errorf("failed to get row count for %s: %w", table, err)
		}

		tableInfos = append(tableInfos, TableInfo{
			Name:       table,
			CreateStmt: schema,
			Columns:    columns,
			RowCount:   rowCount,
		})
	}

	return tableInfos, nil
}

// SortTablesByDependency returns tables sorted by foreign key dependencies.
// Tables with no dependencies come first, then tables that depend on them, etc.
func (a *Analyser) SortTablesByDependency(tables []TableInfo) ([]TableInfo, error) {
	fks, err := a.driver.GetForeignKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign keys: %w", err)
	}

	// Build adjacency list: table -> tables it depends on
	dependencies := make(map[string][]string)
	tableSet := make(map[string]bool)

	for _, t := range tables {
		tableSet[t.Name] = true
		if dependencies[t.Name] == nil {
			dependencies[t.Name] = []string{}
		}
	}

	for _, fk := range fks {
		// Only add dependency if both tables exist in our set
		if tableSet[fk.Table] && tableSet[fk.ReferencedTable] {
			// Skip self-references
			if fk.Table != fk.ReferencedTable {
				dependencies[fk.Table] = append(dependencies[fk.Table], fk.ReferencedTable)
			}
		}
	}

	// Topological sort using Kahn's algorithm
	sorted, err := topologicalSort(tables, dependencies)
	if err != nil {
		return nil, err
	}

	return sorted, nil
}

// topologicalSort performs a topological sort on tables based on dependencies.
func topologicalSort(tables []TableInfo, dependencies map[string][]string) ([]TableInfo, error) {
	// Build in-degree map
	inDegree := make(map[string]int)
	for _, t := range tables {
		inDegree[t.Name] = 0
	}

	// Build reverse adjacency list (who depends on me)
	dependents := make(map[string][]string)
	for table, deps := range dependencies {
		for _, dep := range deps {
			dependents[dep] = append(dependents[dep], table)
			inDegree[table]++
		}
	}

	// Find all tables with no dependencies
	var queue []string
	for _, t := range tables {
		if inDegree[t.Name] == 0 {
			queue = append(queue, t.Name)
		}
	}

	// Build table lookup map
	tableMap := make(map[string]TableInfo)
	for _, t := range tables {
		tableMap[t.Name] = t
	}

	// Process queue
	var sorted []TableInfo
	for len(queue) > 0 {
		// Pop from queue
		current := queue[0]
		queue = queue[1:]

		sorted = append(sorted, tableMap[current])

		// Reduce in-degree of dependents
		for _, dependent := range dependents[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	// Check for cycles
	if len(sorted) != len(tables) {
		// There's a cycle, but we still need to return something
		// Add remaining tables at the end
		sortedSet := make(map[string]bool)
		for _, t := range sorted {
			sortedSet[t.Name] = true
		}

		for _, t := range tables {
			if !sortedSet[t.Name] {
				sorted = append(sorted, t)
			}
		}
	}

	return sorted, nil
}

// GetForeignKeyMap returns a map of table -> []ForeignKey for quick lookup.
func (a *Analyser) GetForeignKeyMap() (map[string][]database.ForeignKey, error) {
	fks, err := a.driver.GetForeignKeys()
	if err != nil {
		return nil, err
	}

	fkMap := make(map[string][]database.ForeignKey)
	for _, fk := range fks {
		fkMap[fk.Table] = append(fkMap[fk.Table], fk)
	}

	return fkMap, nil
}
