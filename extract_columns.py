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
    
    # Remove USE statements first (before GO, since GO might follow USE)
    # Match USE at start of line or after semicolon/newline, followed by database name
    sql = re.sub(r'(?i)(?:^|[\n;])\s*USE\s+[^\s;]+(?:\s*;)?\s*(?=[\n;]|$|\s)', '', sql, flags=re.MULTILINE)
    
    # Remove GO statements (SQL Server batch separator) - can be standalone or with semicolon
    # Match GO at start of line or after semicolon/newline/space, optionally followed by semicolon
    sql = re.sub(r'(?i)(?:^|[\n;]|\s)\s*GO\s*;?\s*(?=[\n;]|$|\s)', '', sql, flags=re.MULTILINE)
    
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
                # Map CTE name to itself (store both original case and lowercase)
                alias_map[cte_name] = cte_name
                if isinstance(cte_name, str):
                    alias_map[cte_name.lower()] = cte_name  # Case-insensitive lookup
    
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
            
            # Map alias to resolved name (store both original case and lowercase for case-insensitive lookup)
            if alias_name:
                alias_map[alias_name] = resolved_name
                alias_map[alias_name.lower()] = resolved_name  # Case-insensitive lookup
            
            # Also map table name to itself (in case it's used without alias)
            alias_map[table_name] = resolved_name
            if isinstance(table_name, str):
                alias_map[table_name.lower()] = resolved_name  # Case-insensitive lookup
        
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
    Now handles WHERE clauses and multiple tables by checking column context.
    """
    column_to_table = {}
    
    # Create case-insensitive alias map for lookups
    case_insensitive_alias_map = {}
    for key, value in alias_map.items():
        case_insensitive_alias_map[key.lower()] = value
        case_insensitive_alias_map[key] = value  # Keep original case too
    
    def get_tables_from_from_clause(select_stmt):
        """Get all table names from FROM and JOIN clauses."""
        tables = []
        
        # Get FROM clause
        from_expr = select_stmt.args.get("from") or select_stmt.args.get("from_")
        if from_expr:
            if isinstance(from_expr, exp.From):
                table_expr = from_expr.this
                if isinstance(table_expr, exp.Table):
                    table_name = table_expr.name if isinstance(table_expr.name, str) else str(table_expr.name)
                    if table_name:
                        # Resolve alias if present
                        alias = table_expr.alias
                        if alias:
                            if isinstance(alias, exp.TableAlias):
                                alias_name = alias.this.name if isinstance(alias.this, exp.Identifier) else str(alias.this)
                            else:
                                alias_name = str(alias)
                            # Try case-insensitive lookup
                            resolved = case_insensitive_alias_map.get(alias_name.lower()) or case_insensitive_alias_map.get(alias_name) or alias_map.get(alias_name)
                            if resolved:
                                tables.append(resolved)
                            else:
                                tables.append(table_name)
                        else:
                            resolved = case_insensitive_alias_map.get(table_name.lower()) or alias_map.get(table_name)
                            if resolved:
                                tables.append(resolved)
                            else:
                                tables.append(table_name)
        
        # Get JOINs
        for join in select_stmt.args.get("joins", []):
            if isinstance(join, exp.Join):
                table_expr = join.this
                if isinstance(table_expr, exp.Table):
                    table_name = table_expr.name if isinstance(table_expr.name, str) else str(table_expr.name)
                    if table_name:
                        alias = table_expr.alias
                        if alias:
                            if isinstance(alias, exp.TableAlias):
                                alias_name = alias.this.name if isinstance(alias.this, exp.Identifier) else str(alias.this)
                            else:
                                alias_name = str(alias)
                            resolved = case_insensitive_alias_map.get(alias_name.lower()) or case_insensitive_alias_map.get(alias_name) or alias_map.get(alias_name)
                            if resolved:
                                tables.append(resolved)
                            else:
                                tables.append(table_name)
                        else:
                            resolved = case_insensitive_alias_map.get(table_name.lower()) or alias_map.get(table_name)
                            if resolved:
                                tables.append(resolved)
                            else:
                                tables.append(table_name)
        
        return tables
    
    def find_column_source_table(column_name, select_stmt, alias_map, case_insensitive_alias_map):
        """
        Try to determine which table a column belongs to by checking:
        1. If there's exactly one table, use it
        2. If column appears in JOIN conditions, use the table from that JOIN
        3. If column appears in WHERE with table prefix in same expression, use that table
        """
        tables = get_tables_from_from_clause(select_stmt)
        
        # Case 1: Single table - easy
        if len(tables) == 1:
            return tables[0]
        
        # Case 2: Check JOIN conditions for this column
        for join in select_stmt.args.get("joins", []):
            if isinstance(join, exp.Join):
                # Check if this column appears in the JOIN condition
                join_condition = join.args.get("on") or join.args.get("using")
                if join_condition:
                    for col in join_condition.find_all(exp.Column):
                        if col.name:
                            col_name = col.name if isinstance(col.name, str) else str(col.name)
                            if col_name.lower() == column_name.lower():
                                if col.table:
                                    # Column has table prefix in JOIN
                                    table_ref = col.table if isinstance(col.table, str) else str(col.table)
                                    resolved = case_insensitive_alias_map.get(table_ref.lower()) or alias_map.get(table_ref)
                                    if resolved:
                                        return resolved
                                    return table_ref
        
        # Case 3: Check if column appears with table prefix elsewhere in WHERE/HAVING
        where_clause = select_stmt.args.get("where")
        if where_clause:
            for col in where_clause.find_all(exp.Column):
                if col.name:
                    col_name = col.name if isinstance(col.name, str) else str(col.name)
                    if col_name.lower() == column_name.lower() and col.table:
                        table_ref = col.table if isinstance(col.table, str) else str(col.table)
                        resolved = case_insensitive_alias_map.get(table_ref.lower()) or alias_map.get(table_ref)
                        if resolved:
                            return resolved
                        return table_ref
        
        # Case 4: If we can't determine, return None (will skip this column)
        return None
    
    # Process all SELECT statements
    for select_stmt in stmt.find_all(exp.Select):
        # Find all unqualified columns in this SELECT (including WHERE, JOIN, HAVING, etc.)
        for col in select_stmt.find_all(exp.Column):
            if col.name and not col.table:
                column_name = col.name if isinstance(col.name, str) else str(col.name)
                # Try to find source table using improved logic
                source_table = find_column_source_table(column_name, select_stmt, alias_map, case_insensitive_alias_map)
                if source_table:
                    # Use case-insensitive key for lookup
                    column_to_table[column_name.lower()] = source_table
                    column_to_table[column_name] = source_table  # Keep original case too
    
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
                    
                    # Create case-insensitive alias map
                    case_insensitive_alias_map = {}
                    for key, value in alias_map.items():
                        case_insensitive_alias_map[key.lower()] = value
                    
                    # Resolve unqualified columns to their source tables
                    unqualified_map = resolve_unqualified_columns(stmt, alias_map)
                    
                    # Find all column references (including WHERE, JOIN, HAVING, etc.)
                    for col in stmt.find_all(exp.Column):
                        if col.name:
                            # Get table name (either from column or from unqualified mapping)
                            table_name = col.table
                            if table_name and not isinstance(table_name, str):
                                table_name = str(table_name)
                            
                            # If column is unqualified, try to resolve it (case-insensitive)
                            if not table_name:
                                col_name = col.name if isinstance(col.name, str) else str(col.name)
                                # Try case-insensitive lookup
                                table_name = (unqualified_map.get(col_name) or 
                                            unqualified_map.get(col_name.lower()))
                            
                            # Only include columns that have a table reference
                            if not table_name:
                                # Skip columns without table reference
                                continue
                            
                            # Build fully qualified name
                            parts = []
                            
                            # Add catalog if present
                            if col.catalog:
                                parts.append(col.catalog if isinstance(col.catalog, str) else str(col.catalog))
                            
                            # Add database/schema if present
                            if col.db:
                                parts.append(col.db if isinstance(col.db, str) else str(col.db))
                            
                            # Resolve table alias to actual table name (case-insensitive)
                            table_name_str = table_name if isinstance(table_name, str) else str(table_name)
                            resolved_table = (case_insensitive_alias_map.get(table_name_str.lower()) or 
                                             alias_map.get(table_name_str))
                            
                            if resolved_table:
                                # Replace alias with actual table name
                                actual_table = resolved_table
                                # Split the actual table name and use its parts
                                table_parts = actual_table.split('.')
                                parts.extend(table_parts)
                            else:
                                # Use table name as-is if not in alias map
                                parts.append(table_name_str)
                            
                            # Add column name
                            col_name = col.name if isinstance(col.name, str) else str(col.name)
                            parts.append(col_name)
                            
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
        help="SQL files or directories to process (default: all .sql files in current directory). Directories are searched recursively."
    )
    parser.add_argument(
        "--dialect",
        "-d",
        help="SQL dialect (postgres, mysql, snowflake, etc.)"
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output file (default: columns.csv in current directory, use .xlsx for Excel)"
    )
    parser.add_argument(
        "--dataset",
        default="SQL_Parser",
        help="Dataset name to use in output (default: SQL_Parser). Note: Output format is now Filename, ColumnName"
    )
    
    args = parser.parse_args()
    
    # Determine which files to process
    sql_files = []
    
    if args.files:
        # Process provided files/directories
        for file_path in args.files:
            path = Path(file_path)
            if not path.exists():
                print(f"Warning: Path not found: {path}", file=sys.stderr)
                continue
            
            if path.is_file():
                # Single file
                if path.suffix.lower() == '.sql':
                    sql_files.append(path)
                else:
                    print(f"Warning: Skipping non-SQL file: {path}", file=sys.stderr)
            elif path.is_dir():
                # Directory - find all SQL files recursively
                found_files = sorted(path.rglob("*.sql"))
                sql_files.extend(found_files)
                print(f"Found {len(found_files)} SQL file(s) in {path}")
    else:
        # Default to current directory (recursive)
        sql_files = sorted(Path.cwd().rglob("*.sql"))
    
    if not sql_files:
        print("No SQL files found to process.")
        return
    
    # Collect all column references with file tracking
    # Structure: list of tuples (filename, column_name)
    column_data = []
    file_columns = {}
    
    print(f"\nProcessing {len(sql_files)} SQL file(s)...")
    
    for sql_file in sql_files:
        print(f"  Processing: {sql_file}")
        try:
            columns = process_sql_file(sql_file, args.dialect)
            # Store full relative path or just filename
            file_key = str(sql_file.relative_to(Path.cwd())) if sql_file.is_relative_to(Path.cwd()) else str(sql_file)
            file_columns[file_key] = columns
            
            # Add each column with its source file
            for col in columns:
                column_data.append((file_key, col))
        except Exception as e:
            print(f"  Error processing {sql_file}: {e}", file=sys.stderr)
            continue
    
    # Count unique vs total
    all_columns = [col for _, col in column_data]
    unique_columns = sorted(set(all_columns))
    total_count = len(column_data)
    unique_count = len(unique_columns)
    
    print(f"\nFound {total_count} total table.column references ({unique_count} unique)")
    print(f"Across {len(file_columns)} file(s)")
    
    # Determine output file name
    if args.output:
        output_path = Path(args.output)
    else:
        # Default to columns.csv in current directory
        output_path = Path("columns.csv")
    
    # Output to CSV or Excel
    
    if output_path.suffix.lower() == '.xlsx':
        # Excel output
        try:
            import pandas as pd
            
            # Create DataFrame with Filename and ColumnName
            df = pd.DataFrame({
                'Filename': [filename for filename, _ in column_data],
                'ColumnName': [col for _, col in column_data]
            })
            
            df.to_excel(output_path, index=False)
            print(f"\n✓ Output written to: {output_path}")
            print(f"  Rows: {len(df)} (all occurrences)")
            print(f"  Columns: Filename, ColumnName")
            
        except ImportError:
            print("\nError: pandas and openpyxl required for Excel output.")
            print("Install with: pip install pandas openpyxl")
            print("\nFalling back to CSV output...")
            output_path = output_path.with_suffix('.csv')
    
    if output_path.suffix.lower() == '.csv':
        # CSV output
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Filename', 'ColumnName'])  # Header
            
            for filename, col in column_data:
                writer.writerow([filename, col])
        
        print(f"\n✓ Output written to: {output_path}")
        print(f"  Rows: {len(column_data)} (all occurrences)")
        print(f"  Columns: Filename, ColumnName")
    
    # Show summary by file
    print("\n" + "="*60)
    print("SUMMARY BY FILE")
    print("="*60)
    for filename, cols in sorted(file_columns.items()):
        unique_in_file = len(set(cols))
        print(f"  {filename}: {len(cols)} total references ({unique_in_file} unique)")
    
    # Show sample of extracted columns
    print("\n" + "="*60)
    print("SAMPLE OUTPUT (first 20 rows)")
    print("="*60)
    for filename, col in column_data[:20]:
        print(f"  {filename} | {col}")
    if len(column_data) > 20:
        print(f"  ... and {len(column_data) - 20} more rows")


if __name__ == "__main__":
    main()

