# SQL Column Extractor

Extract table.column references from SQL files and output to CSV/Excel format. Processes folders recursively and tracks which file each column reference comes from.

## Features

- Extracts fully qualified table.column references from SQL queries
- Resolves table aliases to actual table names
- Handles unqualified columns by inferring table names from context
- Processes folders recursively (searches all subdirectories)
- Supports complex SQL features:
  - CTEs (Common Table Expressions)
  - Subqueries and derived tables
  - JOINs (INNER, LEFT, RIGHT, FULL OUTER)
  - Window functions
  - WHERE, HAVING, and JOIN conditions
- Case-insensitive alias resolution
- Preprocesses SQL to handle edge cases:
  - Removes SQL comments
  - Removes DDL statements (CREATE, ALTER, DROP)
  - Removes DECLARE and SET statements
  - Removes WITH (NOLOCK) hints
  - Removes USE statements and GO statements
  - Removes isolation levels and SET NOCOUNT
  - Removes TOP clauses
  - Normalizes whitespace
  - Handles HTML entities and escape codes

## Installation

```bash
pip install sqlglot pandas openpyxl
```

Or install from requirements.txt:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
# Process all SQL files in current directory (recursive)
python extract_columns.py

# Process a specific folder (recursive)
python extract_columns.py my_folder --output output.xlsx

# Process specific files
python extract_columns.py file1.sql file2.sql --output columns.csv

# Process folder and output to Excel
python extract_columns.py sql_queries/ --output results.xlsx
```

### Command Line Options

- `files`: SQL files or directories to process (default: current directory)
- `--output`, `-o`: Output file path (default: `columns.csv`)
- `--dialect`, `-d`: SQL dialect (postgres, mysql, tsql, snowflake, etc.)
- `--dataset`: Dataset name (default: "SQL_Parser", not used in current output format)

### Examples

```bash
# Process current directory recursively
python extract_columns.py

# Process a folder and save to Excel
python extract_columns.py ./sql_files --output results.xlsx

# Process with specific SQL dialect
python extract_columns.py queries/ --dialect tsql --output output.csv

# Process multiple folders/files
python extract_columns.py folder1/ folder2/ file.sql --output combined.xlsx
```

## Output Format

The script generates a CSV or Excel file with two columns:

- **Filename**: Relative path to the SQL file where the column was found
- **ColumnName**: Fully qualified table.column reference (e.g., `employees.employee_id`)

### Example Output

| Filename | ColumnName |
|----------|------------|
| queries/users.sql | users.user_id |
| queries/users.sql | users.email |
| queries/orders.sql | orders.order_id |
| queries/orders.sql | orders.user_id |
| queries/orders.sql | orders.total |

Each row represents one column reference occurrence. If the same column appears multiple times in a file, it will appear on multiple rows.

## How It Works

1. **Preprocessing**: Removes comments, DDL statements, and other non-query SQL
2. **Parsing**: Uses sqlglot to parse SQL into an Abstract Syntax Tree (AST)
3. **Alias Resolution**: Builds a map of table aliases to actual table names
4. **Column Extraction**: Traverses the AST to find all column references
5. **Qualification**: Resolves unqualified columns to their source tables
6. **Output**: Writes results to CSV or Excel format

## Supported SQL Features

- SELECT statements with multiple tables
- JOINs (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
- CTEs (WITH clauses)
- Subqueries (scalar, correlated, EXISTS, IN)
- Derived tables
- Window functions
- WHERE, HAVING, GROUP BY, ORDER BY clauses
- UNION, INTERSECT, EXCEPT operations

## Limitations

- Unqualified columns in multi-table queries may not be resolved if the table cannot be determined from context
- Some SQL dialects may require explicit `--dialect` specification
- Very large SQL files may take longer to process

## Requirements

- Python 3.7+
- sqlglot >= 24.0.0
- pandas >= 2.0.0 (for Excel output)
- openpyxl >= 3.0.0 (for Excel output)

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
