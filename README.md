# SQL Column Extractor

Extract all `table.column` references from SQL files and output them to CSV or Excel format.

## Features

- ✅ Extracts all table.column references from SQL files
- ✅ Resolves table aliases to full table names (e.g., `e.employee_id` → `employees.employee_id`)
- ✅ Resolves CTE aliases (e.g., `oh.level` → `org_hierarchy.level`)
- ✅ Qualifies unqualified columns (e.g., `employee_id` → `employees.employee_id`)
- ✅ Handles complex SQL: CTEs, subqueries, derived tables, recursive CTEs, UNIONs, JOINs
- ✅ Outputs all occurrences (not just unique) - perfect for data dictionaries
- ✅ Supports CSV and Excel output formats

## Installation

```bash
pip install -r requirements.txt
```

Or install dependencies individually:
```bash
pip install sqlglot pandas openpyxl
```

For better performance, install sqlglot with Rust tokenizer:
```bash
pip install "sqlglot[rs]" pandas openpyxl
```

## Usage

### Basic Usage

```bash
# Extract from all SQL files in current directory
python extract_columns.py

# Extract from specific file(s)
python extract_columns.py file1.sql file2.sql

# Output to Excel
python extract_columns.py --output columns.xlsx

# Custom dataset name
python extract_columns.py --dataset "MyProject" --output columns.xlsx

# Specify SQL dialect
python extract_columns.py --dialect postgres --output columns.csv
```

### Examples

```bash
# Extract from single file
python extract_columns.py queries.sql --output output.xlsx

# Extract from multiple files
python extract_columns.py query1.sql query2.sql --output combined.xlsx

# Use custom dataset name
python extract_columns.py --dataset "Production" --output prod_columns.xlsx
```

## Output Format

The script generates a CSV or Excel file with two columns:

| Dataset | ColumnName |
|---------|------------|
| SQL_Parser | employees.employee_id |
| SQL_Parser | employees.first_name |
| SQL_Parser | customers.customer_id |
| ... | ... |

- **Dataset**: The dataset name (configurable with `--dataset`)
- **ColumnName**: The fully qualified table.column reference

## How It Works

1. **Parses SQL** using sqlglot to build an Abstract Syntax Tree (AST)
2. **Resolves aliases** by mapping table aliases to their actual table/CTE names
3. **Qualifies columns** by resolving unqualified columns to their source tables
4. **Extracts references** by finding all column references in the AST
5. **Outputs results** to CSV or Excel format

## Supported SQL Features

- ✅ Common Table Expressions (CTEs)
- ✅ Recursive CTEs
- ✅ Subqueries (correlated, scalar, EXISTS, IN, etc.)
- ✅ Derived tables (subqueries in FROM clause)
- ✅ Window functions
- ✅ Complex JOINs (INNER, LEFT, RIGHT, FULL OUTER, CROSS, LATERAL)
- ✅ UNION, INTERSECT, EXCEPT
- ✅ Aggregations and GROUP BY
- ✅ CASE expressions
- ✅ Multiple SQL dialects (PostgreSQL, MySQL, Snowflake, BigQuery, etc.)

## Examples

### Input SQL
```sql
WITH employee_data AS (
    SELECT employee_id, first_name, salary
    FROM employees
    WHERE hire_date >= '2020-01-01'
)
SELECT e.employee_id, e.first_name
FROM employee_data e
WHERE e.salary > 50000;
```

### Output (Excel/CSV)
```
Dataset,ColumnName
SQL_Parser,employees.employee_id
SQL_Parser,employees.first_name
SQL_Parser,employees.salary
SQL_Parser,employees.hire_date
SQL_Parser,employee_data.employee_id
SQL_Parser,employee_data.first_name
SQL_Parser,employee_data.salary
```

Notice how:
- `employee_id` (unqualified) → `employees.employee_id`
- `e.employee_id` (alias) → `employee_data.employee_id` (CTE alias resolved)

## License

MIT License - feel free to use this script for your projects!

## Dependencies

- [sqlglot](https://github.com/tobymao/sqlglot) - SQL parser and transpiler
- [pandas](https://pandas.pydata.org/) - Data manipulation (for Excel output)
- [openpyxl](https://openpyxl.readthedocs.io/) - Excel file support

