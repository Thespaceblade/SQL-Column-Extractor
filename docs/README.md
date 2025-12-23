# SQL Column Extractor - Complex Samples

This directory contains an enhanced SQL column extraction script that handles comprehensive edge cases.

## Overview

The `extract_columns.py` script extracts table.column references from SQL files with support for advanced SQL features and edge cases.

## Features

### Core Capabilities
- Extracts fully qualified table.column references from SQL queries
- Resolves table aliases to actual table names
- Handles unqualified columns by inferring table names from context
- Supports multiple SQL dialects with automatic fallback
- Case-insensitive alias resolution

### Edge Cases Handled

#### Critical Priority
- **LATERAL JOINs** (PostgreSQL, MySQL 8.0+)
- **CROSS APPLY / OUTER APPLY** (SQL Server, Oracle)
- **Window functions** with PARTITION BY and ORDER BY
- **Recursive CTEs** with proper scoping
- **Ambiguous column resolution** using JOIN conditions and WHERE clauses
- **Three and four-part names** (database.schema.table, server.database.schema.table)

#### High Priority
- **PIVOT and UNPIVOT** operations
- **Table-valued functions** (SQL Server, PostgreSQL)
- **Correlated subqueries** at multiple nesting levels
- **QUALIFY clause** (Snowflake, BigQuery)
- **Deep nesting** (3+ levels of subqueries/CTEs)

#### Medium Priority
- **Advanced GROUP BY** (ROLLUP, CUBE, GROUPING SETS)
- **Set operations** (INTERSECT, EXCEPT)
- **JSON/XML table functions**
- **Dialect-specific features** (PostgreSQL arrays, SQL Server hints, etc.)

#### Additional Edge Cases
- Derived tables and inline views
- NATURAL JOINs and USING clauses
- Column aliases and expressions
- WHERE and HAVING clause edge cases
- CASE expressions with subqueries
- NULL handling (IS NULL, COALESCE)
- String and date functions
- Aggregate functions with DISTINCT and FILTER
- Multiple statements in one file
- Comments and special formatting
- Views and materialized views
- Table alias edge cases
- Column resolution edge cases

## Usage

### Basic Usage

```bash
# Extract from all SQL files in current directory
python extract_columns.py

# Extract from specific file
python extract_columns.py query.sql

# Specify SQL dialect
python extract_columns.py query.sql --dialect postgres

# Output to Excel
python extract_columns.py query.sql --output results.xlsx
```

### Command Line Options

- `files`: SQL files to process (default: all .sql files in current directory)
- `--dialect`, `-d`: SQL dialect (postgres, mysql, tsql, snowflake, etc.)
- `--output`, `-o`: Output file (default: columns.csv, use .xlsx for Excel)
- `--dataset`: Dataset name for output (default: SQL_Parser)

## How It Works

1. **Parsing**: Uses sqlglot to parse SQL into an Abstract Syntax Tree (AST)
2. **Alias Resolution**: Builds a map of table aliases to actual table names
   - Handles FROM aliases, JOIN aliases, CTE aliases, subquery aliases
   - Case-insensitive resolution for SQL Server/MySQL compatibility
   - Tracks recursive CTE context
3. **Column Extraction**: Recursively finds all Column expressions in the AST
   - Includes columns in SELECT, WHERE, JOIN, HAVING, window functions, etc.
4. **Unqualified Column Resolution**: Maps unqualified columns to source tables
   - Checks if single table in FROM clause
   - Analyzes JOIN conditions for column references
   - Checks WHERE clauses for table-qualified columns
5. **Qualification**: Builds fully qualified names (schema.table.column)
6. **Output**: Writes results to CSV or Excel format

## Technical Details

### Multi-Dialect Support

The script automatically tries multiple dialects if parsing fails:
- Generic SQL (None)
- T-SQL / MSSQL (SQL Server)
- PostgreSQL
- MySQL
- Snowflake

### Error Handling

- Per-statement error handling (continues if one statement fails)
- Graceful handling of parse errors
- Informative error messages

### Limitations

1. **sqlglot Parsing**: Some SQL constructs depend on sqlglot's parser support
   - PIVOT/UNPIVOT: If sqlglot doesn't parse, columns won't be extracted
   - QUALIFY clause: Requires sqlglot support for Snowflake/BigQuery
2. **Ambiguous Columns**: When a column name exists in multiple tables and can't be resolved, it's skipped
3. **Dynamic SQL**: EXEC/EXECUTE statements with string literals cannot be statically analyzed

## Requirements

- Python 3.7+
- sqlglot >= 24.0.0
- pandas >= 2.0.0 (for Excel output)
- openpyxl >= 3.0.0 (for Excel output)

## Installation

```bash
pip install sqlglot pandas openpyxl
```

Or for better performance:
```bash
pip install "sqlglot[rs]" pandas openpyxl
```

## Output Format

The script generates a CSV or Excel file with columns:
- **Dataset**: Dataset name (from --dataset option)
- **ColumnName**: Fully qualified table.column reference (e.g., `employees.employee_id`)

Example output:
```csv
Dataset,ColumnName
SQL_Parser,employees.id
SQL_Parser,employees.name
SQL_Parser,departments.name
```

## Documentation

See the following documentation files for more details:

- **EDGE_CASES.md**: Complete list of all edge cases (30 categories)
- **CRITICAL_EDGE_CASES.md**: Prioritized list of critical edge cases
- **EDGE_CASES_CHECKLIST.md**: Verification checklist of all edge cases
- **EDGE_CASE_COVERAGE_ANALYSIS.md**: Detailed analysis of edge case coverage
- **SCRIPT_ENHANCEMENTS.md**: Summary of enhancements made to handle edge cases

## Examples

### Example 1: Basic Query
```sql
SELECT id, name FROM employees;
```
**Output**: `employees.id`, `employees.name`

### Example 2: With Aliases
```sql
SELECT e.id, d.name 
FROM employees e
JOIN departments d ON e.dept_id = d.id;
```
**Output**: `employees.id`, `departments.name`, `employees.dept_id`, `departments.id`

### Example 3: Window Functions
```sql
SELECT id, name,
       ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn
FROM employees;
```
**Output**: `employees.id`, `employees.name`, `employees.dept_id`, `employees.salary`

### Example 4: Recursive CTE
```sql
WITH RECURSIVE org_tree AS (
  SELECT id, name, parent_id FROM organizations WHERE parent_id IS NULL
  UNION ALL
  SELECT o.id, o.name, o.parent_id
  FROM organizations o
  JOIN org_tree ot ON o.parent_id = ot.id
)
SELECT ot.id, e.name FROM org_tree ot JOIN employees e ON e.org_id = ot.id;
```
**Output**: `organizations.id`, `organizations.name`, `organizations.parent_id`, `employees.name`, `employees.org_id`

## License

MIT License

## Contributing

Contributions welcome! The script is designed to be extensible for handling additional edge cases.

