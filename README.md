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

- `files`: SQL files or directories to process (default: current directory, searches recursively)
- `--output`, `-o`: Output file path or directory (default: `output/columns.csv`)
  - If a file path: Creates parent directory and uses that filename
  - If a directory: Creates CSV file named `columns.csv` in that directory
  - If not specified: Creates `output/` directory in current location
- `--dialect`, `-d`: SQL dialect (postgres, mysql, tsql, snowflake, oracle, etc.)
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
```

## Output Structure

The script creates an `output/` folder (or uses the specified output directory) containing:

- **CSV/Excel file**: The main output with extracted columns
- **Log file**: Detailed processing log with timestamps
- **errors.txt**: Comprehensive error report
- **Error_Reports/**: Subfolder containing copies of files that had errors or found 0 columns

### Error Handling

Files that cannot be parsed correctly or find 0 columns are:
- Copied (not moved) to `Error_Reports/` subfolder
- Logged with full details
- Added to `errors.txt` report

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

1. **Preprocessing**: Removes comments, DDL statements, and other non-query SQL
2. **Parsing**: Uses sqlglot to parse SQL into an Abstract Syntax Tree (AST)
3. **Alias Resolution**: Builds a map of table aliases to actual table names
4. **Column Extraction**: Traverses the AST to find all column references
5. **Qualification**: Resolves unqualified columns to their source tables
6. **Filename Parsing**: Extracts Report Name and Dataset from SQL filename
7. **Deduplication**: Removes duplicate columns within each file (keeps unique per file)
8. **Error Tracking**: Copies problematic files to `Error_Reports/` and logs details
9. **Output**: Writes results to CSV or Excel format with ReportName, Dataset, ColumnName columns
10. **Error Reporting**: Generates comprehensive error report in `errors.txt`

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

## Limitations

- Unqualified columns in multi-table queries may not be resolved if the table cannot be determined from context
- Some SQL dialects may require explicit `--dialect` specification
- Very large SQL files may take longer to process
- Files with errors are copied (not moved) to Error_Reports for review

## Requirements

- Python 3.7+
- sqlglot >= 24.0.0
- pandas >= 2.0.0 (for Excel output)
- openpyxl >= 3.0.0 (for Excel output)

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
