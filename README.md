# SQL Column Extractor

Extract table.column references from SQL files and output to CSV/Excel format. Processes folders recursively and tracks which file each column reference comes from.

## Features

- Extracts fully qualified table.column references from SQL queries
- **Guaranteed no aliases in output** - All table aliases are resolved to actual table names
- **Scope-aware alias resolution** - Correctly handles alias shadowing in nested queries (e.g., same alias `[t]` in outer query vs subquery resolves to different tables)
- **Enhanced alias lookup** - Multiple fallback strategies ensure maximum resolution success (case-insensitive, bracket-aware, scope-aware)
- **Strict validation** - Unresolved aliases are filtered out to prevent aliases from appearing in output
- Handles unqualified columns by inferring table names from context with fallback
- Processes folders recursively (searches all subdirectories)
- Filters out datasets matching exclusion patterns (SOR%, Tablix, EndDate, EvidenceTab, EvidenceTablix)
- Output sorted by ReportName, Dataset, ColumnName
- Excel output with auto-formatted column widths and filters
- Robust error handling for None/empty/invalid input
- Supports complex SQL features:
  - CTEs (Common Table Expressions), including recursive CTEs
  - Subqueries and derived tables
  - JOINs (INNER, LEFT, RIGHT, FULL OUTER)
  - CROSS APPLY / OUTER APPLY (SQL Server lateral joins)
  - Window functions with PARTITION BY and ORDER BY
  - PIVOT and UNPIVOT operations
  - Correlated subqueries
  - WHERE, HAVING, and JOIN conditions
  - UNION, INTERSECT, EXCEPT operations
- Case-insensitive alias resolution
- Scope-aware alias resolution (handles alias shadowing in nested queries)
- Enhanced alias lookup with multiple fallback strategies
- Strict validation prevents unresolved aliases from appearing in output
- Handles bracketed aliases correctly (e.g., `SELECT [u].id FROM users [u]` resolves to `users.id`)
- Strips brackets from identifiers for clean output (e.g., `[dbo].[users].[id]` → `dbo.users.id`)
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
  - Preserves bracketed identifiers (e.g., [schema].[table])

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

## Output Format

The script generates a CSV or Excel file with three columns:

- **ReportName**: The report name extracted from the SQL filename (part before `__`)
- **Dataset**: The dataset name extracted from the SQL filename (part after `__`, or "Default" if no `__` found)
- **ColumnName**: Fully qualified table.column reference (e.g., `dbo.employees.employee_id`)

The output is automatically sorted by ReportName, Dataset, and ColumnName. Excel files include:
- Auto-formatted column widths (capped at 50 characters)
- Auto-filter enabled on the header row for easy filtering

### Filename Parsing

The script parses SQL filenames to extract Report Name and Dataset:
- Format: `<report_name>__<dataset>.sql`
- Example: `115_Hr_Inactive_Users_with_Active_accounts__AppNames.sql`
  - ReportName: `115_Hr_Inactive_Users_with_Active_accounts`
  - Dataset: `AppNames`
- If no double underscore (`__`) is found, Dataset defaults to `"Default"`

### Dataset Filtering

Files with datasets matching the following patterns are automatically skipped:
- SOR% (e.g., SOR_DATA, SOR_REFRESH, SORDATA)
- Tablix
- EndDate
- EvidenceTab
- EvidenceTablix

Filtering is case-insensitive and occurs before SQL processing.

### Example Output

| ReportName | Dataset | ColumnName |
|------------|---------|------------|
| 115_Hr_Inactive_Users_with_Active_accounts | AppNames | dbo.users.user_id |
| 115_Hr_Inactive_Users_with_Active_accounts | AppNames | dbo.users.email |
| 200_Customer_Orders | Default | dbo.orders.order_id |
| 200_Customer_Orders | Default | dbo.orders.user_id |

Each row represents one unique column reference per file. Duplicate columns within the same file are removed, but the same column can appear in multiple rows if it exists in different files. Column names are output in clean format without brackets (e.g., `dbo.users.id` instead of `[dbo].[users].[id]`).

### Detailed Example

**Input SQL File**: `115_Hr_Inactive_Users_with_Active_accounts__AppNames.sql`
```sql
SELECT employee_id, first_name, email
FROM employees
WHERE status = 'active';
```

**Output CSV** (sorted by ReportName, Dataset, ColumnName):
```csv
ReportName,Dataset,ColumnName
115_Hr_Inactive_Users_with_Active_accounts,AppNames,dbo.employees.employee_id
115_Hr_Inactive_Users_with_Active_accounts,AppNames,dbo.employees.email
115_Hr_Inactive_Users_with_Active_accounts,AppNames,dbo.employees.first_name
115_Hr_Inactive_Users_with_Active_accounts,AppNames,dbo.employees.status
```

**Input SQL File** (no double underscore): `simple_query.sql`
```sql
SELECT id, name FROM users;
```

**Output CSV** (sorted by ReportName, Dataset, ColumnName):
```csv
ReportName,Dataset,ColumnName
simple_query,Default,dbo.users.id
simple_query,Default,dbo.users.name
```

**Input SQL File** (with bracketed aliases): `bracketed_aliases.sql`
```sql
SELECT [u].id, [u].email, [d].dept_name
FROM [dbo].[users] [u]
JOIN [dbo].[departments] [d] ON [u].dept_id = [d].id;
```

**Output CSV** (brackets stripped, aliases resolved):
```csv
ReportName,Dataset,ColumnName
bracketed_aliases,Default,dbo.departments.dept_name
bracketed_aliases,Default,dbo.departments.id
bracketed_aliases,Default,dbo.users.dept_id
bracketed_aliases,Default,dbo.users.email
bracketed_aliases,Default,dbo.users.id
```

**Input SQL File** (alias shadowing - same alias in different scopes): `alias_shadowing.sql`
```sql
SELECT [t].[Column1], [t].[Column2]
FROM [Schema1].[Table1] [t]
WHERE EXISTS (
    SELECT 1
    FROM [Schema2].[Table2] [t]
    WHERE [t].[Column3] = 'test'
);
```

**Output CSV** (scope-aware resolution - each `[t]` resolves to its own scope's table):
```csv
ReportName,Dataset,ColumnName
alias_shadowing,Default,Schema1.Table1.Column1
alias_shadowing,Default,Schema1.Table1.Column2
alias_shadowing,Default,Schema2.Table2.Column3
```

Note: The same alias `[t]` in the outer query resolves to `Schema1.Table1`, while `[t]` in the subquery resolves to `Schema2.Table2`. This is handled correctly by scope-aware alias resolution.

## How It Works

1. **Filename Parsing**: Extracts Report Name and Dataset from SQL filename
2. **Dataset Filtering**: Skips files with excluded dataset names (SOR%, Tablix, EndDate, EvidenceTab, EvidenceTablix)
3. **Input Validation**: Validates SQL input (handles None, empty strings, invalid input gracefully)
4. **Preprocessing**: Removes comments, DDL statements, and other non-query SQL while preserving bracketed identifiers
5. **Parsing**: Uses sqlglot to parse SQL into an Abstract Syntax Tree (AST), with regex fallback for unparseable SQL
6. **Scope-Aware Alias Resolution**: 
   - Builds per-SELECT scope alias maps with parent scope inheritance
   - Each SELECT scope can have its own aliases that shadow parent scope aliases
   - Correctly resolves aliases like `[t]` in outer query vs `[t]` in subquery to different tables
7. **Enhanced Alias Lookup**: 
   - Tries scope-aware lookup first, then global lookup
   - Strips brackets and retries if initial lookup fails
   - Tries case variations (upper, title) for additional matching
   - Handles both bracketed and unbracketed aliases (e.g., `[u]` and `u` both resolve correctly)
8. **Strict Validation**: 
   - Distinguishes between resolved table names and unresolved aliases
   - Only includes columns with valid table references
   - Filters out columns with unresolved aliases to prevent aliases in output
9. **Column Extraction**: Traverses the AST to find all column references
10. **Qualification**: Resolves unqualified columns to their source tables with fallback to first table in FROM clause
11. **Bracket Stripping**: Removes SQL Server brackets from all identifiers (aliases, table names, schema names, column names) for clean output (e.g., `[dbo].[users].[id]` → `dbo.users.id`)
12. **Deduplication**: Removes duplicate columns within each file (keeps unique per file)
13. **Sorting**: Sorts output by ReportName, Dataset, ColumnName
14. **Output**: Writes results to CSV or Excel format with auto-formatting and filters (Excel only)

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

- Unqualified columns in multi-table queries use fallback to first table in FROM clause if table cannot be determined from context
- Columns with unresolvable table references (e.g., nonexistent_table.column) are skipped, except for originally unqualified columns which are included with fallback table
- Derived table references (subqueries, CROSS APPLY results) appear as `derived_alias.column` since there's no underlying physical table (this is correct behavior)
- CTE references appear as `CTE_name.column` (this is correct behavior for CTEs)
- Some SQL dialects may require explicit `--dialect` specification
- Very large SQL files may take longer to process
- CSV format does not support filters or auto-formatting (Excel only)
- Fallback regex parser (used when AST parsing fails) may not handle all bracket/quote patterns as robustly as the main AST parser

## Requirements

- Python 3.7+
- sqlglot >= 24.0.0
- pandas >= 2.0.0 (for Excel output)
- openpyxl >= 3.0.0 (for Excel output)

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
