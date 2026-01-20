# dbmask

A CLI tool for exporting database schemas and data while anonymising sensitive information. Perfect for creating safe development or testing datasets from production databases.

## Features

- **Multi-database support**: MySQL, PostgreSQL, and SQLite
- **Data anonymisation**: Replace sensitive data with realistic fake values using faker templates
- **Data minimisation**: Truncate tables, retain a row count, or filter by date
- **Foreign key aware**: Automatically orders tables by dependencies for valid imports
- **Memory efficient**: Streams data in configurable batches (default: 1000 rows)
- **Flexible configuration**: YAML or JSON config files
- **Dry run mode**: Preview what will happen before executing

## Installation

### From Source

Requires Go 1.21+ and CGO (for SQLite support).

```bash
git clone https://github.com/elliotxx/dbmask.git
cd dbmask
CGO_ENABLED=1 go build -o dbmask ./cmd/dbmask
```

### Binary

Move the compiled binary to your PATH:

```bash
sudo mv dbmask /usr/local/bin/
```

## Quick Start

1. Create a configuration file (`config.yaml`):

```yaml
connection:
  type: mysql
  host: localhost
  port: 3306
  username: root
  password: secret
  database_name: production_db

configuration:
  users:
    columns:
      email: "{{faker.email}}"
      name: "{{faker.name}}"
      phone: "{{faker.phone}}"

  sessions:
    truncate: true

  audit_logs:
    retain: 100  # Keep first 100 rows

  orders:
    retain:
      column_name: "created_at"
      after_date: "2024-01-01"  # Keep only orders since this date
```

2. Run the export:

```bash
dbmask -c config.yaml -o anonymised_dump.sql
```

3. Import into your development database:

```bash
mysql -u root -p dev_db < anonymised_dump.sql
```

## Usage

```
dbmask [flags]

Flags:
  -c, --config string   Path to config file (required)
  -o, --output string   Output file path (default: stdout)
  -v, --verbose         Enable verbose logging
      --dry-run         Show what would be done without executing
  -h, --help            Help for dbmask

Commands:
  sync        Sync config file with database tables
  version     Print version information
```

### Examples

```bash
# Export to stdout
dbmask -c config.yaml

# Export to file with verbose output
dbmask -c config.yaml -o dump.sql -v

# Preview without executing
dbmask -c config.yaml --dry-run

# Using JSON config
dbmask -c config.json -o dump.sql
```

### Sync Command

The `sync` command connects to your database and adds any tables that are missing from your configuration file. This is useful when:

- Setting up a new configuration
- The database schema has changed and new tables were added
- You want to ensure all tables are accounted for

```bash
# Preview what tables would be added (dry run)
dbmask sync -c config.yaml --dry-run

# Add missing tables with default settings (full export)
dbmask sync -c config.yaml

# Add missing tables with truncate: true (schema only, no data)
dbmask sync -c config.yaml --truncate

# Verbose output
dbmask sync -c config.yaml -v
```

**Sync Flags:**

| Flag | Description |
|------|-------------|
| `-c, --config` | Path to config file (required) |
| `--dry-run` | Show what would be added without modifying the file |
| `--truncate` | Add new tables with `truncate: true` instead of full export |
| `-v, --verbose` | Enable verbose logging |

**Example output:**

```
Found 3 new table(s) not in configuration:
  + audit_logs (full export)
  + api_tokens (full export)
  + webhooks (full export)

Configuration updated: config.yaml
Added 3 table(s).
```

## Configuration

### Connection Settings

#### MySQL

```yaml
connection:
  type: mysql
  host: localhost
  port: 3306          # optional, defaults to 3306
  username: user
  password: pass
  database_name: mydb
```

#### PostgreSQL

```yaml
connection:
  type: postgres
  host: localhost
  port: 5432          # optional, defaults to 5432
  username: user
  password: pass
  database_name: mydb
```

#### SQLite

```yaml
connection:
  type: sqlite
  file: /path/to/database.db
```

### Table Configuration

Tables not listed in `configuration` are exported in full with no modifications.

#### Truncate (Schema Only)

Export the table structure but no data. Useful for session tables, temporary data, or sensitive logs.

```yaml
configuration:
  sessions:
    truncate: true

  password_reset_tokens:
    truncate: true
```

#### Retain (Limit Rows)

The `retain` option supports two modes for limiting exported rows:

**Count-based**: Keep only a specified number of rows. Useful for large tables where you only need sample data.

```yaml
configuration:
  audit_logs:
    retain: 1000    # Keep first 1000 rows

  orders:
    retain: 500
```

**Date-based**: Keep only rows after a specified date. Useful for time-series data where you want recent records.

```yaml
configuration:
  orders:
    retain:
      column_name: "created_at"    # The date/datetime column to filter on
      after_date: "2024-01-01"     # Keep rows where column > this date

  events:
    retain:
      column_name: "event_time"
      after_date: "2024-06-01"
```

Supported date formats:
- `YYYY-MM-DD` (e.g., `2024-01-01`)
- `YYYY-MM-DDTHH:MM:SS` (e.g., `2024-01-01T00:00:00`)
- `YYYY-MM-DD HH:MM:SS` (e.g., `2024-01-01 00:00:00`)
- RFC3339 (e.g., `2024-01-01T00:00:00Z`)

#### Column Anonymisation

Replace column values with fake data or static values.

```yaml
configuration:
  users:
    columns:
      email: "{{faker.email}}"           # Fake email
      first_name: "{{faker.firstName}}"  # Fake first name
      last_name: "{{faker.lastName}}"    # Fake last name
      phone: "{{faker.phone}}"           # Fake phone
      ssn: null                          # Set to NULL
      notes: "REDACTED"                  # Static value
```

#### Combined Operations

You can combine `retain` (count-based or date-based) with column anonymisation:

```yaml
configuration:
  # Count-based retain with anonymisation
  audit_logs:
    retain: 100
    columns:
      user_email: "{{faker.email}}"
      ip_address: "{{faker.ipv4}}"

  # Date-based retain with anonymisation
  orders:
    retain:
      column_name: "order_date"
      after_date: "2024-01-01"
    columns:
      customer_email: "{{faker.email}}"
      shipping_address: "{{faker.address}}"
      notes: "Order notes redacted"
```

### Available Faker Functions

| Function | Description | Example Output |
|----------|-------------|----------------|
| `{{faker.name}}` | Full name | John Smith |
| `{{faker.firstName}}` | First name | John |
| `{{faker.lastName}}` | Last name | Smith |
| `{{faker.email}}` | Email address | john.smith@example.com |
| `{{faker.phone}}` | Phone number | 555-123-4567 |
| `{{faker.address}}` | Street address | 123 Main St |
| `{{faker.city}}` | City name | New York |
| `{{faker.country}}` | Country name | United States |
| `{{faker.company}}` | Company name | Acme Corp |
| `{{faker.uuid}}` | UUID v4 | 550e8400-e29b-41d4-a716-446655440000 |
| `{{faker.username}}` | Username | johnsmith42 |
| `{{faker.password}}` | Random password (32 chars) | xK9#mP2$vL... |
| `{{faker.ipv4}}` | IPv4 address | 192.168.1.100 |
| `{{faker.date}}` | Date (YYYY-MM-DD) | 2024-03-15 |
| `{{faker.text}}` | Lorem ipsum sentence | Lorem ipsum dolor sit... |
| `{{faker.number}}` | 8-digit number | 12345678 |

### Referential Integrity

The anonymiser maintains a consistency map to preserve referential integrity. If the same original value appears in multiple rows, it will be replaced with the same anonymised value. This ensures that foreign key relationships remain valid after anonymization.

## Complete Example

Here's a comprehensive configuration for a typical web application:

```yaml
connection:
  type: mysql
  host: production-db.example.com
  port: 3306
  username: readonly_user
  password: ${DB_PASSWORD}  # Use environment variable
  database_name: webapp

configuration:
  # Security: Remove all session data
  sessions:
    truncate: true

  password_resets:
    truncate: true

  oauth_tokens:
    truncate: true

  # Minimise: Keep limited audit history (count-based)
  audit_logs:
    retain: 5000

  # Minimise: Keep recent events only (date-based)
  events:
    retain:
      column_name: "event_time"
      after_date: "2024-01-01"

  # Anonymise: User PII
  users:
    columns:
      email: "{{faker.email}}"
      first_name: "{{faker.firstName}}"
      last_name: "{{faker.lastName}}"
      phone: "{{faker.phone}}"
      password_hash: "{{faker.password}}"
      ip_address: "{{faker.ipv4}}"
      date_of_birth: "{{faker.date}}"

  # Anonymise: Customer data
  customers:
    columns:
      email: "{{faker.email}}"
      name: "{{faker.name}}"
      company: "{{faker.company}}"
      phone: "{{faker.phone}}"

  # Anonymise: Address data
  addresses:
    columns:
      street: "{{faker.address}}"
      city: "{{faker.city}}"
      country: "{{faker.country}}"

  # Minimise + Anonymise: Orders (date-based retain)
  orders:
    retain:
      column_name: "created_at"
      after_date: "2024-01-01"
    columns:
      customer_email: "{{faker.email}}"
      shipping_address: "{{faker.address}}"
      billing_address: "{{faker.address}}"

  # Anonymise: Payment info (critical!)
  payments:
    columns:
      card_number: null
      card_holder: "{{faker.name}}"
      billing_address: "{{faker.address}}"

  # Full export with no changes:
  # - products
  # - categories
  # - settings
  # - migrations
```

## Output Format

The tool generates standard SQL dump files with:

- Database-specific headers (charset, foreign key settings)
- `DROP TABLE IF EXISTS` statements
- `CREATE TABLE` statements (original schema)
- Multi-row `INSERT` statements (batched for efficiency)
- Proper escaping for special characters
- Tables ordered by foreign key dependencies

### Example Output

```sql
-- Database Dump
-- Generated by dbmask
-- Date: 2024-03-15T10:30:00Z
-- Database Type: mysql

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;
START TRANSACTION;

--
-- Table: users
--

DROP TABLE IF EXISTS `users`;

CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `email` varchar(255) NOT NULL,
  `name` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
);

INSERT INTO `users` (`id`, `email`, `name`) VALUES
(1, 'jessica.wilson@gmail.com', 'Jessica Wilson'),
(2, 'mike.johnson@yahoo.com', 'Mike Johnson');

COMMIT;
SET FOREIGN_KEY_CHECKS = 1;
```

## Development

### Prerequisites

- Go 1.21 or later
- GCC (for SQLite CGO compilation)
- Access to MySQL, PostgreSQL, or SQLite for testing

### Building

```bash
# Clone the repository
git clone https://github.com/elliotxx/dbmask.git
cd dbmask

# Download dependencies
go mod tidy

# Build with CGO (required for SQLite)
CGO_ENABLED=1 go build -o dbmask ./cmd/dbmask

# Build with version information
CGO_ENABLED=1 go build -ldflags="-s -w -X main.version=v1.0.0" -o dbmask ./cmd/dbmask

# Build without CGO (no SQLite support, but fully cross-platform)
CGO_ENABLED=0 go build -o dbmask ./cmd/dbmask
```

### Testing

The project has comprehensive unit tests covering all modules.

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests with race detection (recommended)
go test -race ./...

# Run tests with coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

# Run tests with race detection and coverage
go test -v -race -coverprofile=coverage.out ./...

# Run a specific test
go test -run TestAnonymiseRow ./internal/anonymiser/

# Run tests for a specific package
go test -v ./internal/config/
```

### CI/CD

The project uses GitHub Actions for continuous integration and releases:

- **CI Workflow** (`.github/workflows/ci.yml`): Runs on every push/PR to main
  - Runs all tests with race detection and coverage
  - Builds binaries for Linux, macOS, and Windows (amd64, arm64)
  - Runs golangci-lint for code quality

- **Release Workflow** (`.github/workflows/release.yml`): Triggers on version tags
  - Runs full test suite
  - Builds release binaries for all platforms
  - Creates GitHub release with changelog and downloadable assets

### Creating a Release

The project uses semantic versioning (semver). To create a new release:

```bash
# Ensure all changes are committed
git status

# Create a version tag (must start with 'v')
git tag v1.0.0

# Or for pre-release versions
git tag v1.0.0-beta.1
git tag v1.0.0-rc.1

# Push the tag to trigger the release workflow
git push origin v1.0.0
```

The release workflow will automatically:
1. Run all tests
2. Build binaries for Linux, macOS, and Windows
3. Create archives (.tar.gz for Linux/macOS, .zip for Windows)
4. Generate a changelog from commit messages
5. Create a GitHub release with all assets

**Version format examples:**
- `v1.0.0` - Stable release
- `v1.0.1` - Patch release (bug fixes)
- `v1.1.0` - Minor release (new features, backwards compatible)
- `v2.0.0` - Major release (breaking changes)
- `v1.0.0-alpha.1` - Alpha pre-release
- `v1.0.0-beta.1` - Beta pre-release
- `v1.0.0-rc.1` - Release candidate

### Project Structure

```
.
├── cmd/dbmask/
│   └── main.go              # CLI entry point
├── internal/
│   ├── config/
│   │   └── config.go        # YAML/JSON configuration parsing
│   ├── database/
│   │   ├── driver.go        # Database driver interface
│   │   ├── mysql.go         # MySQL implementation
│   │   ├── postgres.go      # PostgreSQL implementation
│   │   └── sqlite.go        # SQLite implementation
│   ├── schema/
│   │   └── schema.go        # Schema extraction & FK sorting
│   ├── anonymiser/
│   │   ├── anonymiser.go    # Anonymisation logic
│   │   └── faker.go         # Faker function registry
│   └── exporter/
│       └── exporter.go      # SQL dump generation
├── config.example.yaml      # Example configuration
├── go.mod
└── go.sum
```

### Architecture

The tool follows a pipeline architecture:

1. **Config** - Parses YAML/JSON configuration defining database connection and table rules
2. **Driver** - Connects to the database using the appropriate driver (MySQL, PostgreSQL, or SQLite)
3. **Schema** - Extracts table schemas and analyzes foreign key relationships
4. **Analyser** - Topologically sorts tables by FK dependencies to ensure valid import order
5. **Anonymiser** - Applies transformation rules to each row (faker templates, static values, NULL)
6. **Exporter** - Streams rows in batches and generates SQL statements

### Adding a New Faker Function

Edit `internal/anonymiser/faker.go`:

```go
var fakerFunctions = map[string]FakerFunc{
    // ... existing functions ...
    "creditCard": func() string { return gofakeit.CreditCardNumber(nil) },
    "ssn":        func() string { return gofakeit.SSN() },
}
```

### Adding a New Database Driver

1. Create a new file in `internal/database/` (e.g., `mssql.go`)
2. Implement the `Driver` interface
3. Register the driver in `NewDriver()` in `driver.go`
4. Add connection DSN logic in `internal/config/config.go`

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
