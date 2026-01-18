# dbmask

A CLI tool for exporting database schemas and data while anonymising sensitive information. Perfect for creating safe development or testing datasets from production databases.

## Features

- **Multi-database support**: MySQL, PostgreSQL, and SQLite
- **Data anonymisation**: Replace sensitive data with realistic fake values using faker templates
- **Data minimisation**: Truncate tables or retain only a subset of rows
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

  orders:
    retain: 100
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

Keep only a specified number of rows. Useful for large tables where you only need sample data.

```yaml
configuration:
  audit_logs:
    retain: 1000    # Keep first 1000 rows

  orders:
    retain: 500
```

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

You can combine `retain` with column anonymisation:

```yaml
configuration:
  orders:
    retain: 100
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

  # Minimise: Keep limited audit history
  audit_logs:
    retain: 5000

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

  # Minimise + Anonymise: Orders
  orders:
    retain: 200
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

# Run tests
go test ./...

# Run a specific test
go test -run TestAnonymiseRow ./internal/anonymiser/
```

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
