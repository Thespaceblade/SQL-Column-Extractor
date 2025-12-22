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
import re
import html
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


def preprocess_sql(sql: str) -> str:
    """
    Preprocess SQL to handle edge cases and remove problematic syntax.
    
    Handles:
    - SQL comments (-- and /* */)
    - DDL statements (CREATE, ALTER, DROP)
    - DECLARE and SET statements
    - WITH (NOLOCK) hints
    - USE statements
    - Isolation levels
    - SET NOCOUNT ON/OFF
    - ASCII control characters
    - Escape codes
    - HTML entities
    - TOP clause (removed, but columns still extracted)
    - Non-printable characters
    - Normalize whitespace
    """
    # Decode HTML entities first (before removing other things)
    sql = html.unescape(sql)
    
    # Remove SQL comments (-- style) - must come before other processing
    sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
    
    # Remove SQL comments (/* */ style)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    
    # Remove GO statements (SQL Server batch separator)
    sql = re.sub(r'(?i)^\s*GO\s*$', '', sql, flags=re.MULTILINE)
    
    # Remove USE statements (can span multiple lines)
    sql = re.sub(r'(?i)^\s*USE\s+[^;]+;?\s*$', '', sql, flags=re.MULTILINE)
    
    # Remove SET NOCOUNT ON/OFF
    sql = re.sub(r'(?i)SET\s+NOCOUNT\s+(ON|OFF)\s*;?', '', sql, flags=re.MULTILINE)
    
    # Remove SET TRANSACTION ISOLATION LEVEL
    sql = re.sub(r'(?i)SET\s+TRANSACTION\s+ISOLATION\s+LEVEL\s+[^;]+;?', '', sql, flags=re.MULTILINE)
    
    # Remove other SET statements (but preserve UPDATE SET)
    # Split by semicolons, process each statement
    statements = sql.split(';')
    cleaned_statements = []
    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue
        # Skip SET statements that aren't part of UPDATE
        if re.match(r'(?i)^\s*SET\s+', stmt) and not re.search(r'(?i)\bUPDATE\s+.*\bSET\b', stmt):
            continue
        cleaned_statements.append(stmt)
    sql = '; '.join(cleaned_statements)
    
    # Remove DECLARE statements
    sql = re.sub(r'(?i)\bDECLARE\s+[^;]+;?', '', sql)
    
    # Remove DDL statements (CREATE, ALTER, DROP) - match entire statement
    sql = re.sub(r'(?i)\b(CREATE|ALTER|DROP)\s+[^;]+;?', '', sql)
    
    # Remove WITH (NOLOCK) hints - multiple patterns
    sql = re.sub(r'(?i)\s+WITH\s*\(\s*NOLOCK\s*\)', '', sql)
    sql = re.sub(r'(?i)\(\s*NOLOCK\s*\)', '', sql)
    sql = re.sub(r'(?i)\s+\(NOLOCK\)', '', sql)
    
    # Remove TOP clause (but keep the rest of the SELECT)
    sql = re.sub(r'(?i)\bTOP\s*\(\s*\d+\s*\)\s+', '', sql)
    sql = re.sub(r'(?i)\bTOP\s+\d+\s+', '', sql)
    
    # Remove escape codes (like \x1b, \033, etc.)
    sql = re.sub(r'\\x[0-9a-fA-F]{2}', '', sql)
    sql = re.sub(r'\\[0-9]{3}', '', sql)
    sql = re.sub(r'\\[a-zA-Z]', '', sql)
    
    # Remove ASCII control characters (except newline, tab, carriage return)
    sql = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', sql)
    
    # Remove non-printable characters (but keep newlines, tabs, spaces, and common punctuation)
    sql = re.sub(r'[^\x20-\x7E\n\r\t]', '', sql)
    
    # Normalize whitespace
    # Replace multiple spaces/tabs with single space
    sql = re.sub(r'[ \t]+', ' ', sql)
    # Replace multiple newlines with single newline
    sql = re.sub(r'\n{2,}', '\n', sql)
    # Remove trailing whitespace from lines
    sql = re.sub(r'[ \t]+$', '', sql, flags=re.MULTILINE)
    # Remove leading/trailing whitespace
    sql = sql.strip()
    
    return sql


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
    
    # If no dialect specified, try common dialects on parse failure
    dialects_to_try = [dialect] if dialect else [None, 'tsql', 'mssql', 'postgres', 'mysql']
    
    last_error = None
    for try_dialect in dialects_to_try:
        try:
            statements = sqlglot.parse(sql, dialect=try_dialect)
            
            # Check if parsing succeeded (at least one non-None statement)
            if not statements or all(s is None for s in statements):
                continue
            
            # Process each statement individually, skip ones that fail
            for i, stmt in enumerate(statements):
                if stmt is None:
                    continue
                
                try:
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
                
                except Exception as e:
                    # Skip this statement but continue with others
                    print(f"Warning: Skipped statement {i+1} due to error: {e}", file=sys.stderr)
                    continue
            
            # If we extracted any columns, return them (even if some statements failed)
            if columns:
                return columns
            
        except ParseError as e:
            last_error = e
            # Try next dialect
            continue
        except Exception as e:
            last_error = e
            # Try next dialect
            continue
    
    # If all dialects failed, print error but don't fail completely
    if last_error:
        print(f"Warning: Parse error (tried dialects: {', '.join(str(d) for d in dialects_to_try if d)}): {last_error}", file=sys.stderr)
        print("Attempting to extract columns from successfully parsed statements...", file=sys.stderr)
    
    return columns


def process_sql_file(filepath: Path, dialect: Optional[str] = None) -> list:
    """Process a single SQL file and return all table.column references (all occurrences)."""
    with open(filepath, "r", encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # Preprocess SQL to handle edge cases
    content = preprocess_sql(content)
    
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

