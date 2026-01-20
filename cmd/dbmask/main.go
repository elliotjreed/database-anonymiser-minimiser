package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"

	"github.com/elliotjreed/database-anonymiser-minimiser/internal/anonymiser"
	"github.com/elliotjreed/database-anonymiser-minimiser/internal/config"
	"github.com/elliotjreed/database-anonymiser-minimiser/internal/database"
	"github.com/elliotjreed/database-anonymiser-minimiser/internal/exporter"
	"github.com/elliotjreed/database-anonymiser-minimiser/internal/schema"
)

var (
	version = "0.1.0"

	configPath   string
	outputPath   string
	verbose      bool
	dryRun       bool
	syncTruncate bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "dbmask",
		Short: "Database anonymiser and minimiser tool",
		Long: `dbmask is a CLI tool that exports database schemas and data while
anonymising sensitive information based on a YAML/JSON configuration file.

Supports MySQL, PostgreSQL, and SQLite databases.`,
		RunE: runExport,
	}

	rootCmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file (required)")
	rootCmd.Flags().StringVarP(&outputPath, "output", "o", "", "Output file path (default: stdout)")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be done without executing")

	rootCmd.MarkFlagRequired("config")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("dbmask version %s\n", version)
		},
	}
	rootCmd.AddCommand(versionCmd)

	syncCmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync config file with database tables",
		Long: `Connects to the database and adds any tables that are not
currently in the configuration file. Existing table configurations
are preserved.

New tables are added with an empty configuration (full export).
Use --truncate to add new tables with truncate: true instead.`,
		RunE: runSync,
	}
	syncCmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file (required)")
	syncCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging")
	syncCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be added without modifying the file")
	syncCmd.Flags().BoolVar(&syncTruncate, "truncate", false, "Add new tables with truncate: true")
	syncCmd.MarkFlagRequired("config")
	rootCmd.AddCommand(syncCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runExport(cmd *cobra.Command, args []string) error {
	startTime := time.Now()

	// Get initial memory stats
	var memStatsBefore runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	// Load configuration
	if verbose {
		fmt.Printf("Loading configuration from: %s\n", configPath)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create anonymiser and validate rules
	anon := anonymiser.New(cfg)
	if errors := anon.ValidateRules(); len(errors) > 0 {
		for _, e := range errors {
			fmt.Fprintf(os.Stderr, "Warning: %s\n", e)
		}
	}

	// Create database driver
	if verbose {
		fmt.Printf("Connecting to %s database...\n", cfg.Connection.Type)
	}

	driver, err := database.NewDriver(cfg.Connection.Type)
	if err != nil {
		return err
	}

	if err := driver.Connect(&cfg.Connection); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer driver.Close()

	// Analyze schema
	if verbose {
		fmt.Println("Analyzing database schema...")
	}

	analyzer := schema.NewAnalyser(driver)
	tables, err := analyzer.GetAllTables()
	if err != nil {
		return fmt.Errorf("failed to analyze schema: %w", err)
	}

	// Sort tables by dependencies
	if verbose {
		fmt.Println("Sorting tables by foreign key dependencies...")
	}

	sortedTables, err := analyzer.SortTablesByDependency(tables)
	if err != nil {
		return fmt.Errorf("failed to sort tables: %w", err)
	}

	// Dry run mode
	if dryRun {
		return printDryRun(sortedTables, anon)
	}

	// Determine output
	var output *os.File
	if outputPath != "" {
		output, err = os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer output.Close()

		if verbose {
			fmt.Printf("Writing output to: %s\n", outputPath)
		}
	} else {
		output = os.Stdout
	}

	// Export
	if verbose {
		fmt.Printf("Exporting %d tables...\n", len(sortedTables))
	}

	exp := exporter.New(driver, anon, output, exporter.Options{
		Verbose:   verbose,
		BatchSize: 1000,
	})

	if err := exp.Export(sortedTables); err != nil {
		return fmt.Errorf("export failed: %w", err)
	}

	// Collect final statistics
	elapsed := time.Since(startTime)
	var memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsAfter)
	stats := exp.GetStats()

	// Print statistics
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "=== Export Statistics ===")
	fmt.Fprintf(os.Stderr, "Tables exported:   %d\n", stats.TablesExported)
	fmt.Fprintf(os.Stderr, "Tables truncated:  %d\n", stats.TablesTruncated)
	fmt.Fprintf(os.Stderr, "Rows exported:     %d\n", stats.RowsExported)
	fmt.Fprintf(os.Stderr, "Run time:          %s\n", elapsed.Round(time.Millisecond))
	fmt.Fprintf(os.Stderr, "Memory used:       %s\n", formatBytes(memStatsAfter.TotalAlloc-memStatsBefore.TotalAlloc))
	fmt.Fprintf(os.Stderr, "Peak memory:       %s\n", formatBytes(memStatsAfter.HeapAlloc))
	fmt.Fprintf(os.Stderr, "CPU cores used:    %d\n", runtime.NumCPU())

	if verbose {
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Export completed successfully!")
	}

	return nil
}

func printDryRun(tables []schema.TableInfo, anon *anonymiser.Anonymiser) error {
	fmt.Println("=== DRY RUN MODE ===")
	fmt.Printf("Found %d tables\n\n", len(tables))

	for _, table := range tables {
		fmt.Printf("Table: %s\n", table.Name)
		fmt.Printf("  Rows: %d\n", table.RowCount)

		if anon.ShouldTruncate(table.Name) {
			fmt.Println("  Action: TRUNCATE (no data will be exported)")
		} else if limit := anon.GetRetainLimit(table.Name); limit > 0 {
			fmt.Printf("  Action: RETAIN %d rows\n", limit)
		} else {
			fmt.Println("  Action: FULL EXPORT")
		}

		if cols := anon.GetAnonymisedColumns(table.Name); len(cols) > 0 {
			fmt.Printf("  Anonymised columns: %v\n", cols)
		}

		fmt.Println()
	}

	return nil
}

func runSync(cmd *cobra.Command, args []string) error {
	// Load configuration
	if verbose {
		fmt.Printf("Loading configuration from: %s\n", configPath)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create database driver
	if verbose {
		fmt.Printf("Connecting to %s database...\n", cfg.Connection.Type)
	}

	driver, err := database.NewDriver(cfg.Connection.Type)
	if err != nil {
		return err
	}

	if err := driver.Connect(&cfg.Connection); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer driver.Close()

	// Get all tables from database
	if verbose {
		fmt.Println("Fetching tables from database...")
	}

	dbTables, err := driver.GetTables()
	if err != nil {
		return fmt.Errorf("failed to get tables: %w", err)
	}

	// Find tables not in config
	var newTables []string
	for _, table := range dbTables {
		if !cfg.HasTable(table) {
			newTables = append(newTables, table)
		}
	}

	if len(newTables) == 0 {
		fmt.Println("All database tables are already in the configuration.")
		return nil
	}

	// Report what will be added
	fmt.Printf("Found %d new table(s) not in configuration:\n", len(newTables))
	for _, table := range newTables {
		if syncTruncate {
			fmt.Printf("  + %s (truncate: true)\n", table)
		} else {
			fmt.Printf("  + %s (full export)\n", table)
		}
	}

	// Dry run mode - don't modify the file
	if dryRun {
		fmt.Println("\nDry run mode - no changes made to config file.")
		return nil
	}

	// Add new tables to config
	for _, table := range newTables {
		var tableConfig *config.TableConfig
		if syncTruncate {
			tableConfig = &config.TableConfig{Truncate: true}
		} else {
			tableConfig = &config.TableConfig{}
		}
		cfg.AddTable(table, tableConfig)
	}

	// Save the updated config
	if err := cfg.Save(configPath); err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	fmt.Printf("\nConfiguration updated: %s\n", configPath)
	fmt.Printf("Added %d table(s).\n", len(newTables))

	return nil
}

// formatBytes formats bytes into a human-readable string.
func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
