# SQL Column Extractor

Extract table.column references from SQL files and output to CSV/Excel format. Processes folders recursively and tracks which file each column reference comes from.

## Features

- Extracts fully qualified table.column references from SQL queries
- Resolves table aliases to actual table names
- Handles unqualified columns by inferring table names from context
- Processes folders recursively (searches all subdirectories)
- Supports complex SQL features:
  - CTEs (Common Table Expressions), including recursive CTEs
  - Subqueries and derived tables
  - JOINs (INNER, LEFT, RIGHT, FULL OUTER, LATERAL, CROSS APPLY)
  - Window functions with PARTITION BY
  - PIVOT and UNPIVOT operations
  - Table-valued functions
  - Correlated subqueries
  - WHERE, HAVING, and JOIN conditions
  - Set operations (UNION, INTERSECT, EXCEPT)
  - NATURAL JOINs and USING clauses
- Case-insensitive alias resolution
- Advanced unqualified column resolution (checks JOIN conditions, WHERE clauses)
- Multi-dialect support with automatic fallback
- Preprocesses SQL to handle edge cases:
  - Removes SQL comments
  - Removes DDL statements (CREATE, ALTER, DROP)
  - Removes DECLARE and SET statements
  - Removes WITH (NOLOCK) hints
  - Removes USE statements and GO statements
  - Removes isolation levels and SET NOCOUNT
  - Removes TOP clauses
  - Normalizes whitespace
  - Decodes HTML entities (`&gt;`, `&lt;`, `&#60;`, `&#62;`, etc.) to actual symbols
  - Decodes URL-encoded characters (`%3E`, `%3C`, `%26`, etc.)
  - Removes ANSI escape codes and Unicode control characters
- Hybrid parsing strategy:
  - Primary: Strict AST parsing using sqlglot for accurate column extraction
  - Fallback: Pure regex-based extraction (no sqlparse dependency) when AST parsing fails
  - Fallback is bracket/quote/backtick-aware: handles `[schema].[table]`, `"table"."column"`, `` `table`.`column` ``
  - Case-preserving: maintains original identifier case for accurate extraction
  - Automatic fallback ensures maximum column extraction even from malformed SQL
- RDL-aware filename filtering: Automatically skips files matching RDL patterns (SOR_DATA, SOR_REFRESH, etc.)
- Default dialect: Uses `tsql` (SQL Server) as default if no dialect specified

## Installation

```bash
pip install sqlglot pandas openpyxl sqlparse
```

Or install from requirements.txt:

```bash
pip install -r requirements.txt
```

**Note**: `sqlparse` is optional. The fallback parser now uses pure regex patterns that are bracket/quote/backtick-aware and does not require sqlparse.

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

- `files`: SQL files or directories to process (default: current directory, searches recursively)
- `--output`, `-o`: Output file path or directory (default: `output/columns.csv`)
  - If a file path: Creates parent directory and uses that filename
  - If a directory: Creates CSV file named `columns.csv` in that directory
  - If not specified: Creates `output/` directory in current location
- `--dialect`, `-d`: SQL dialect (default: `tsql` for SQL Server)
  - Supported: `tsql`/`mssql` (SQL Server), `postgres`, `mysql`, `snowflake`, `oracle`, `bigquery`
  - Note: `mssql`, `sqlserver`, `sql-server`, `t-sql` are automatically normalized to `tsql`
- `--try-multiple-dialects`: Try multiple dialects on parse failure (default: only try specified dialect or tsql)
  - When enabled, attempts: `tsql`, `postgres`, `mysql`, `snowflake`, `oracle`, `bigquery`
- `--no-unqualified-resolution`: Disable inference of table names for unqualified columns
  - By default, the script attempts to resolve unqualified columns (e.g., `id`) to their source tables
  - Use this flag to disable this behavior and only extract fully qualified columns
- `--dataset`: Dataset name (legacy option, not used - Dataset is extracted from filename)

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

# Try multiple dialects if parsing fails
python extract_columns.py queries/ --try-multiple-dialects --output output.csv

# Disable unqualified column resolution
python extract_columns.py queries/ --no-unqualified-resolution --output output.csv
```

## Output Structure

The script creates an `output/` folder (or uses the specified output directory) containing:

- **CSV/Excel file**: The main output with extracted columns
- **Log file**: Detailed processing log with timestamps
- **errors.txt**: Comprehensive error report
- **Error_Reports/**: Subfolder containing copies of files that had errors or found 0 columns

### Error Handling

The script uses a hybrid parsing approach with status codes:

- **SUCCESS**: AST parsing succeeded and columns were extracted
- **PARTIAL_OK**: AST parsing failed, but fallback parsing successfully extracted columns (treated as success, file not copied to Error_Reports)
- **PARSE_ERROR**: Both AST and fallback parsing failed (file copied to Error_Reports)
- **ZERO_COLUMNS**: Parsing succeeded but no columns were found (file copied to Error_Reports)

Files with `PARSE_ERROR` or `ZERO_COLUMNS` status are:
- Copied (not moved) to `Error_Reports/` subfolder
- Logged with full details
- Added to `errors.txt` report with detailed error information

Files with `PARTIAL_OK` status are treated as successful and are not copied to Error_Reports.

The `errors.txt` file includes comprehensive error details:
- **Parse errors** with line numbers and column positions
- **SQL context** showing 3 lines before/after the error location
- **Dialect information** showing which SQL dialect was attempted
- **Actionable suggestions** for fixing common SQL syntax issues
- **Statement numbers** when processing multiple statements
- **Full tracebacks** for non-parse errors

Example error details in `errors.txt`:
```
File: query.sql
Error: Failed to parse SQL

Detailed Parse Errors:

  Statement #1
  Dialect: postgres
  Error: Invalid expression / Unexpected token. Line 1, Col: 40.

  Detailed Error Information:
    Parse Error (ParseError)
    Statement #1
    Dialect: postgres
    
    Error: Invalid expression / Unexpected token. Line 1, Col: 40.
    Line: 1
    
    Context:
       1: SELECT * FROM table WHERE invalid
    >>>   2: syntax error here
    
    Suggestions:
      - Check for missing commas, parentheses, or quotes
      - Verify SQL syntax matches the specified dialect
      - Try specifying a dialect: --dialect postgres|mysql|tsql|snowflake
```

## Output Format

The script generates a CSV or Excel file with three columns:

- **ReportName**: The report name extracted from the SQL filename (part before `__`)
- **Dataset**: The dataset name extracted from the SQL filename (part after `__`, or "Default" if no `__` found)
- **ColumnName**: Fully qualified table.column reference (e.g., `employees.employee_id`)

### Filename Parsing

The script parses SQL filenames to extract Report Name and Dataset:
- Format: `<report_name>__<dataset>.sql`
- Example: `115_Hr_Inactive_Users_with_Active_accounts__AppNames.sql`
  - ReportName: `115_Hr_Inactive_Users_with_Active_accounts`
  - Dataset: `AppNames`
- If no double underscore (`__`) is found, Dataset defaults to `"Default"`

### Example Output

| ReportName | Dataset | ColumnName |
|------------|---------|------------|
| 115_Hr_Inactive_Users_with_Active_accounts | AppNames | users.user_id |
| 115_Hr_Inactive_Users_with_Active_accounts | AppNames | users.email |
| 200_Customer_Orders | Default | orders.order_id |
| 200_Customer_Orders | Default | orders.user_id |

Each row represents one unique column reference per file. Duplicate columns within the same file are removed, but the same column can appear in multiple rows if it exists in different files.

### Detailed Example

**Input SQL File**: `115_Hr_Inactive_Users_with_Active_accounts__AppNames.sql`
```sql
SELECT employee_id, first_name, email
FROM employees
WHERE status = 'active';
```

**Output CSV**:
```csv
ReportName,Dataset,ColumnName
115_Hr_Inactive_Users_with_Active_accounts,AppNames,employees.employee_id
115_Hr_Inactive_Users_with_Active_accounts,AppNames,employees.first_name
115_Hr_Inactive_Users_with_Active_accounts,AppNames,employees.email
115_Hr_Inactive_Users_with_Active_accounts,AppNames,employees.status
```

**Input SQL File** (no double underscore): `simple_query.sql`
```sql
SELECT id, name FROM users;
```

**Output CSV**:
```csv
ReportName,Dataset,ColumnName
simple_query,Default,users.id
simple_query,Default,users.name
```

## How It Works

1. **Preprocessing**: 
   - Decodes HTML entities (`&gt;` → `>`, `&lt;` → `<`, etc.)
   - Decodes URL-encoded characters (`%3E` → `>`, etc.)
   - Removes ANSI escape codes and Unicode control characters
   - Removes comments, DDL statements, and other non-query SQL
   - Normalizes whitespace
2. **RDL File Filtering**: Skips files matching RDL patterns (SOR_DATA, SOR_REFRESH, etc.)
3. **Hybrid Parsing**:
   - **Primary**: Attempts strict AST parsing using sqlglot
   - **Fallback**: If AST parsing fails or finds 0 columns, uses tolerant sqlparse + regex parsing
4. **Alias Resolution**: Builds a map of table aliases to actual table names (case-insensitive)
5. **Column Extraction**: Traverses the AST to find all column references
6. **Qualification**: Resolves unqualified columns to their source tables (unless `--no-unqualified-resolution` is used)
7. **Filename Parsing**: Extracts Report Name and Dataset from SQL filename
8. **Deduplication**: Removes duplicate columns within each file (keeps unique per file)
9. **Error Tracking**: Copies problematic files (`PARSE_ERROR`, `ZERO_COLUMNS`) to `Error_Reports/` and logs details
10. **Output**: Writes results to CSV or Excel format with ReportName, Dataset, ColumnName columns
11. **Error Reporting**: Generates comprehensive error report in `errors.txt`

## Supported SQL Features

- SELECT statements with multiple tables
- JOINs (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
- CTEs (WITH clauses)
- Subqueries (scalar, correlated, EXISTS, IN)
- Derived tables
- Window functions
- WHERE, HAVING, GROUP BY, ORDER BY clauses
- UNION, INTERSECT, EXCEPT operations

## Output Files

When you run the script, it creates an output directory structure:

```
output/
├── columns.csv          # Main output file (or specified filename)
├── columns.log          # Detailed processing log
├── errors.txt           # Error report with all problematic files
└── Error_Reports/      # Copies of files with errors or 0 columns
    ├── error_file1.sql
    ├── zero_columns.sql
    └── ...
```

The `errors.txt` file contains:
- List of files with processing errors (with full tracebacks)
- List of files with 0 columns found (with reasons)
- Total counts and summary statistics

## Error Reporting

The script provides detailed error reporting to help diagnose SQL parsing issues:

- **Enhanced error messages** with line numbers, column positions, and SQL context
- **Multi-dialect fallback** automatically tries different SQL dialects
- **Per-statement error handling** continues processing other statements if one fails
- **Comprehensive error log** in `errors.txt` with all error details
- **Error file copies** in `Error_Reports/` subfolder for easy review

When a SQL file fails to parse, the error message includes:
- Which statement failed (if multiple statements)
- Which dialect was attempted
- Exact line and column where the error occurred
- SQL context around the error (3 lines before/after)
- Specific suggestions for fixing the issue

## Limitations

- Unqualified columns in multi-table queries may not be resolved if the table cannot be determined from context (use `--no-unqualified-resolution` to disable this feature)
- Some SQL dialects may require explicit `--dialect` specification (defaults to `tsql`)
- Very large SQL files may take longer to process
- Files with `PARSE_ERROR` or `ZERO_COLUMNS` status are copied (not moved) to Error_Reports for review
- Dynamic SQL (EXEC with string literals) cannot be statically analyzed
- Fallback parsing (PARTIAL_OK) may extract columns with less accuracy than AST parsing, but provides better coverage for malformed SQL

## Supported Identifier Formats

The extractor handles all common SQL identifier quoting styles:

| Style | Example | Dialect |
|-------|---------|---------|
| Brackets | `[schema].[table].[column]` | SQL Server (TSQL) |
| Double quotes | `"schema"."table"."column"` | ANSI SQL, PostgreSQL, Oracle |
| Backticks | `` `schema`.`table`.`column` `` | MySQL |
| Unquoted | `schema.table.column` | All dialects |
| Mixed | `[schema]."table".column` | Cross-dialect |

## Requirements

- Python 3.7+
- sqlglot >= 24.0.0 (required)
- pandas >= 2.0.0 (for Excel output)
- openpyxl >= 3.0.0 (for Excel output)
- sqlparse >= 0.4.0 (optional, no longer required for fallback)

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
