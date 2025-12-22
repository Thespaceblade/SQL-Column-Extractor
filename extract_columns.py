#!/usr/bin/env python3
"""
Extract table.column references from SQL files and output to CSV/Excel format.

Usage:
    python extract_columns.py                    # Extract from all SQL files
    python extract_columns.py 01_simple_ctes.sql  # Extract from specific file
    python extract_columns.py --output columns.csv  # Output to CSV
    python extract_columns.py --output columns.xlsx  # Output to Excel
"""

import sys
import csv
from pathlib import Path
from typing import Optional
from collections import defaultdict

# Import sqlglot (install with: pip install sqlglot or pip install "sqlglot[rs]")
try:
    import sqlglot
    from sqlglot import expressions as exp
    from sqlglot.errors import ParseError
except ImportError:
    print("Error: sqlglot is not installed.")
    print("Install with: pip install sqlglot")
    print("Or for better performance: pip install 'sqlglot[rs]'")
    sys.exit(1)


def build_alias_map(stmt: exp.Expression) -> dict:
    """
    Build a mapping of table aliases to actual table/CTE names.
    Returns dict mapping alias -> full_table_name or cte_name
    Handles all cases: FROM aliases, JOIN aliases, CTE aliases, subquery aliases
    """
    alias_map = {}
    cte_names = set()
    
    # First, collect all CTE names
    for cte in stmt.find_all(exp.CTE):
        if cte.alias:
            cte_name = cte.alias if isinstance(cte.alias, str) else cte.alias.name if hasattr(cte.alias, 'name') else str(cte.alias)
            if cte_name:
                cte_names.add(cte_name)
                # Map CTE name to itself
                alias_map[cte_name] = cte_name
    
    def extract_table_alias(table_expr, alias_map, cte_names, context_cte_names=None):
        """Extract table name and alias from a table expression."""
        if context_cte_names is None:
            context_cte_names = set()
        
        if isinstance(table_expr, exp.Table):
            # Get actual table name
            table_name = table_expr.name
            if not table_name:
                return
            
            parts = []
            if table_expr.catalog:
                parts.append(table_expr.catalog)
            if table_expr.db:
                parts.append(table_expr.db)
            if table_name:
                parts.append(table_name)
            
            full_table_name = ".".join(parts) if parts else table_name
            
            # Get alias if present
            alias = table_expr.alias
            alias_name = None
            if alias:
                if isinstance(alias, exp.TableAlias):
                    alias_this = alias.this
                    if isinstance(alias_this, exp.Identifier):
                        alias_name = alias_this.name
                    elif isinstance(alias_this, str):
                        alias_name = alias_this
                    else:
                        alias_name = str(alias_this)
                elif isinstance(alias, str):
                    alias_name = alias
                else:
                    alias_name = str(alias)
            
            # Determine what to map the alias/table to
            # Check both global CTE names and context CTE names (for recursive CTEs)
            if table_name in cte_names or table_name in context_cte_names:
                # This is a CTE, use CTE name
                resolved_name = table_name
            else:
                # This is a real table, use full table name
                resolved_name = full_table_name
            
            # Map alias to resolved name
            if alias_name:
                alias_map[alias_name] = resolved_name
            
            # Also map table name to itself (in case it's used without alias)
            alias_map[table_name] = resolved_name
        
        elif isinstance(table_expr, exp.Subquery):
            # Handle subqueries - get alias if present
            alias = table_expr.alias
            if alias:
                if isinstance(alias, exp.TableAlias):
                    alias_this = alias.this
                    if isinstance(alias_this, exp.Identifier):
                        alias_name = alias_this.name
                    elif isinstance(alias_this, str):
                        alias_name = alias_this
                    else:
                        alias_name = str(alias_this)
                elif isinstance(alias, str):
                    alias_name = alias
                else:
                    alias_name = str(alias)
                
                if alias_name:
                    # For subqueries, we can't resolve to a real table, so skip
                    pass
    
    def extract_from_joins(expression, context_cte_names=None):
        """Recursively extract table aliases from FROM and JOIN clauses."""
        if context_cte_names is None:
            context_cte_names = set()
        
        # Collect CTE names in this context (for recursive CTEs)
        local_cte_names = set(context_cte_names)
        for cte in expression.find_all(exp.CTE):
            if cte.alias:
                cte_name = cte.alias if isinstance(cte.alias, str) else cte.alias.name if hasattr(cte.alias, 'name') else str(cte.alias)
                if cte_name:
                    local_cte_names.add(cte_name)
        
        if isinstance(expression, exp.Select):
            # Get the FROM clause (can be 'from' or 'from_')
            from_expr = expression.args.get("from") or expression.args.get("from_")
            if from_expr:
                if isinstance(from_expr, exp.From):
                    extract_table_alias(from_expr.this, alias_map, cte_names, local_cte_names)
                elif isinstance(from_expr, exp.Table):
                    # Sometimes FROM is directly a Table
                    extract_table_alias(from_expr, alias_map, cte_names, local_cte_names)
            
            # Get all JOINs
            for join in expression.args.get("joins", []):
                if isinstance(join, exp.Join):
                    extract_table_alias(join.this, alias_map, cte_names, local_cte_names)
        
        # Recursively process UNION/UNION ALL
        if isinstance(expression, (exp.Union, exp.Except, exp.Intersect)):
            if expression.this:
                extract_from_joins(expression.this, local_cte_names)
            if expression.expression:
                extract_from_joins(expression.expression, local_cte_names)
        
        # Recursively process CTEs
        for cte in expression.find_all(exp.CTE):
            if cte.this:
                extract_from_joins(cte.this, local_cte_names)
        
        # Recursively process subqueries
        for subquery in expression.find_all(exp.Subquery):
            if subquery.this:
                extract_from_joins(subquery.this, local_cte_names)
        
        # Recursively process all SELECT statements
        for select_stmt in expression.find_all(exp.Select):
            if select_stmt != expression:  # Avoid reprocessing current statement
                extract_from_joins(select_stmt, local_cte_names)
    
    extract_from_joins(stmt)
    return alias_map


def resolve_unqualified_columns(stmt: exp.Expression, alias_map: dict) -> dict:
    """
    Build a mapping of unqualified column names to their source tables.
    Returns dict mapping column_name -> table_name for unqualified columns.
    """
    column_to_table = {}
    
    def get_tables_from_from_clause(select_stmt):
        """Get all table names from FROM and JOIN clauses."""
        tables = []
        
        # Get FROM clause
        from_expr = select_stmt.args.get("from") or select_stmt.args.get("from_")
        if from_expr:
            if isinstance(from_expr, exp.From):
                table_expr = from_expr.this
                if isinstance(table_expr, exp.Table):
                    table_name = table_expr.name
                    if table_name:
                        # Resolve alias if present
                        alias = table_expr.alias
                        if alias:
                            if isinstance(alias, exp.TableAlias):
                                alias_name = alias.this.name if isinstance(alias.this, exp.Identifier) else str(alias.this)
                            else:
                                alias_name = str(alias)
                            if alias_name in alias_map:
                                tables.append(alias_map[alias_name])
                            else:
                                tables.append(table_name)
                        else:
                            if table_name in alias_map:
                                tables.append(alias_map[table_name])
                            else:
                                tables.append(table_name)
        
        # Get JOINs
        for join in select_stmt.args.get("joins", []):
            if isinstance(join, exp.Join):
                table_expr = join.this
                if isinstance(table_expr, exp.Table):
                    table_name = table_expr.name
                    if table_name:
                        alias = table_expr.alias
                        if alias:
                            if isinstance(alias, exp.TableAlias):
                                alias_name = alias.this.name if isinstance(alias.this, exp.Identifier) else str(alias.this)
                            else:
                                alias_name = str(alias)
                            if alias_name in alias_map:
                                tables.append(alias_map[alias_name])
                            else:
                                tables.append(table_name)
                        else:
                            if table_name in alias_map:
                                tables.append(alias_map[table_name])
                            else:
                                tables.append(table_name)
        
        return tables
    
    # For each SELECT statement, map unqualified columns to their source tables
    for select_stmt in stmt.find_all(exp.Select):
        tables = get_tables_from_from_clause(select_stmt)
        
        # If there's exactly one table, we can map unqualified columns to it
        if len(tables) == 1:
            source_table = tables[0]
            # Find unqualified columns in this SELECT
            for col in select_stmt.find_all(exp.Column):
                if col.name and not col.table:
                    # This is an unqualified column - map it to the source table
                    column_to_table[col.name] = source_table
    
    return column_to_table


def extract_table_columns(sql: str, dialect: Optional[str] = None) -> list:
    """
    Extract all table.column references from SQL.
    Resolves aliases to full table names and qualifies unqualified columns.
    Returns ALL occurrences (not unique) - every time a table.column is referenced.
    
    Returns:
        List of strings in format "table.column" or "schema.table.column"
        Only includes columns that have a table reference (no standalone columns or tables)
    """
    columns = []
    
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
        
        for stmt in statements:
            if stmt is None:
                continue
            
            # Build alias mapping for this statement
            alias_map = build_alias_map(stmt)
            
            # Resolve unqualified columns to their source tables
            unqualified_map = resolve_unqualified_columns(stmt, alias_map)
            
            # Find all column references
            for col in stmt.find_all(exp.Column):
                if col.name:
                    # Get table name (either from column or from unqualified mapping)
                    table_name = col.table
                    
                    # If column is unqualified, try to resolve it
                    if not table_name and col.name in unqualified_map:
                        table_name = unqualified_map[col.name]
                    
                    # Only include columns that have a table reference
                    if not table_name:
                        # Skip columns without table reference
                        continue
                    
                    # Build fully qualified name
                    parts = []
                    
                    # Add catalog if present
                    if col.catalog:
                        parts.append(col.catalog)
                    
                    # Add database/schema if present
                    if col.db:
                        parts.append(col.db)
                    
                    # Resolve table alias to actual table name
                    if table_name in alias_map:
                        # Replace alias with actual table name
                        actual_table = alias_map[table_name]
                        # Split the actual table name and use its parts
                        table_parts = actual_table.split('.')
                        parts.extend(table_parts)
                    else:
                        # Use table name as-is if not in alias map
                        parts.append(table_name)
                    
                    # Add column name
                    parts.append(col.name)
                    
                    # Join with dots: schema.table.column or table.column
                    # Must have at least table.column (2 parts minimum)
                    if len(parts) >= 2:
                        qualified_name = ".".join(parts)
                        columns.append(qualified_name)
                    # Skip columns without table reference
    
    except ParseError as e:
        print(f"Parse error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Error extracting columns: {e}", file=sys.stderr)
    
    return columns


def process_sql_file(filepath: Path, dialect: Optional[str] = None) -> list:
    """Process a single SQL file and return all table.column references (all occurrences)."""
    with open(filepath, "r") as f:
        content = f.read()
    
    return extract_table_columns(content, dialect)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Extract table.column references from SQL files"
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Specific SQL files to process (default: all .sql files in current directory)"
    )
    parser.add_argument(
        "--dialect",
        "-d",
        help="SQL dialect (postgres, mysql, snowflake, etc.)"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="columns.csv",
        help="Output file (default: columns.csv, use .xlsx for Excel)"
    )
    parser.add_argument(
        "--dataset",
        default="SQL_Parser",
        help="Dataset name to use in output (default: SQL_Parser)"
    )
    
    args = parser.parse_args()
    
    # Determine which files to process
    if args.files:
        # Use provided file paths (can be relative or absolute)
        sql_files = [Path(f) for f in args.files]
    else:
        # Default to current directory
        sql_files = sorted(Path.cwd().glob("*.sql"))
    
    if not sql_files:
        print("No SQL files found to process.")
        return
    
    # Collect all column references (keep all occurrences, not unique)
    all_columns = []
    file_columns = {}
    
    print(f"Processing {len(sql_files)} SQL file(s)...")
    
    for sql_file in sql_files:
        if not sql_file.exists():
            print(f"File not found: {sql_file}", file=sys.stderr)
            continue
        
        print(f"  Processing: {sql_file.name}")
        columns = process_sql_file(sql_file, args.dialect)
        all_columns.extend(columns)
        file_columns[sql_file.name] = columns
    
    # Count unique vs total
    unique_columns = sorted(set(all_columns))
    total_count = len(all_columns)
    unique_count = len(unique_columns)
    
    print(f"\nFound {total_count} total table.column references ({unique_count} unique)")
    
    # Output to CSV or Excel
    output_path = Path(args.output)
    
    if output_path.suffix.lower() == '.xlsx':
        # Excel output
        try:
            import pandas as pd
            
            df = pd.DataFrame({
                'Dataset': [args.dataset] * len(all_columns),
                'ColumnName': all_columns
            })
            
            df.to_excel(output_path, index=False)
            print(f"\n✓ Output written to: {output_path}")
            print(f"  Rows: {len(df)} (all occurrences)")
            
        except ImportError:
            print("\nError: pandas and openpyxl required for Excel output.")
            print("Install with: pip install pandas openpyxl")
            print("\nFalling back to CSV output...")
            output_path = output_path.with_suffix('.csv')
    
    if output_path.suffix.lower() == '.csv':
        # CSV output
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Dataset', 'ColumnName'])
            
            for col in all_columns:
                writer.writerow([args.dataset, col])
        
        print(f"\n✓ Output written to: {output_path}")
        print(f"  Rows: {len(all_columns)} (all occurrences)")
    
    # Show summary by file
    print("\n" + "="*60)
    print("SUMMARY BY FILE")
    print("="*60)
    for filename, cols in sorted(file_columns.items()):
        unique_in_file = len(set(cols))
        print(f"  {filename}: {len(cols)} total references ({unique_in_file} unique)")
    
    # Show sample of extracted columns
    print("\n" + "="*60)
    print("SAMPLE COLUMNS (first 20)")
    print("="*60)
    for col in all_columns[:20]:
        print(f"  {col}")
    if len(all_columns) > 20:
        print(f"  ... and {len(all_columns) - 20} more")


if __name__ == "__main__":
    main()

