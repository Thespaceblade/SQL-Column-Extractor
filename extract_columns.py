#!/usr/bin/env python3
"""
Extract table.column references from SQL files and output to CSV/Excel format.

EASIEST WAY TO USE:
    1. Open this file and find the FOLDER_PATH variable near the top
    2. Paste your folder path there (e.g., FOLDER_PATH = "/path/to/your/sql/files")
    3. Run: python extract_columns.py

ALTERNATIVE USAGE:
    python extract_columns.py                    # Extract from all SQL files in current directory
    python extract_columns.py 01_simple_ctes.sql  # Extract from specific file
    python extract_columns.py folder/ --output columns.csv  # Process folder, output to CSV
    python extract_columns.py --output columns.xlsx  # Output to Excel
"""

import sys
import csv
import re
import html
import logging
import shutil
from pathlib import Path
from typing import Optional
from collections import defaultdict
from datetime import datetime

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


# ============================================================================
# CONFIGURATION: Paste your folder path here
# ============================================================================
# Paste the full path to your SQL files folder here, or leave empty to use
# command-line arguments or process current directory
# Examples:
#   FOLDER_PATH = "/Users/username/Documents/SQL_Queries"
#   FOLDER_PATH = r"C:\Users\username\Documents\SQL_Queries"  # Windows
#   FOLDER_PATH = ""  # Leave empty to use command-line args or current directory
# ============================================================================
FOLDER_PATH = r""


def parse_filename(filepath: Path) -> tuple[str, str]:
    """
    Parse filename to extract Report Name and Dataset.
    
    Format: <report_name>__<dataset>.sql
    If no double underscore, Dataset defaults to "Default"
    
    Args:
        filepath: Path object to the SQL file
        
    Returns:
        tuple: (report_name, dataset)
    """
    # Get just the filename (no path)
    filename = filepath.name
    
    # Remove .sql extension
    if filename.lower().endswith('.sql'):
        filename = filename[:-4]
    
    # Split by double underscore
    if '__' in filename:
        parts = filename.split('__', 1)  # Split on first occurrence only
        report_name = parts[0]
        dataset = parts[1] if len(parts) > 1 else "Default"
    else:
        # No double underscore - entire filename is report name
        report_name = filename
        dataset = "Default"
    
    return report_name, dataset


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
    
    # Remove ANSI escape sequences comprehensively
    # ANSI escape codes can start with \x1B (ESC), \033 (octal), or \x9B (CSI)
    # Pattern matches: ESC[@-Z\\-_] (single char commands) or ESC[[0-?]*[/]*[@~~] (CSI sequences)
    # This covers SGR codes ([...m), cursor movement, colors, etc.
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[/]*[@-~])')
    sql = ansi_escape.sub('', sql)
    
    # Also handle octal escape sequences (\033)
    ansi_escape_octal = re.compile(r'\033(?:[@-Z\\-_]|\[[0-?]*[/]*[@-~])')
    sql = ansi_escape_octal.sub('', sql)
    
    # Handle CSI sequences starting with \x9B (CSI character)
    # CSI sequences: \x9B followed by parameter bytes (0x30-0x3F), intermediate bytes (0x20-0x2F), final byte (0x40-0x7E)
    ansi_csi = re.compile(r'\x9B[0-?]*[/]*[@-~]')
    sql = ansi_csi.sub('', sql)
    
    # Also handle \x9B sequences that might have lost formatting
    sql = re.sub(r'\x9B[^\x20-\x7E]*', '', sql)
    
    # Remove ANSI escape sequences that lost their escape character (just [...m, [...H, etc.)
    # Match [ followed by optional numbers/semicolons, optional /, and ending character
    sql = re.sub(r'\[[0-9;]*[/]*[@-~]', '', sql)
    
    # Remove standalone escape characters that might be left behind
    sql = re.sub(r'[\x1B\x9B]', '', sql)  # ESC and CSI characters
    
    # Remove string literal escape codes (like \x1b, \033, \n, \t, etc. in string literals)
    # But be careful - we don't want to remove actual SQL escape sequences
    # Only remove if they appear outside of quoted strings (rough heuristic)
    # This is safer - just remove obvious problematic patterns
    sql = re.sub(r'\\x1[bB]', '', sql)  # \x1b or \x1B
    sql = re.sub(r'\\033', '', sql)     # \033
    sql = re.sub(r'\\x9[bB]', '', sql)  # \x9b or \x9B
    
    # Remove other escape sequences that might cause issues
    sql = re.sub(r'\\x[0-9a-fA-F]{2}', '', sql)  # Any \xXX hex escape
    sql = re.sub(r'\\[0-7]{1,3}', '', sql)        # Octal escapes (\0 to \377)
    
    # Remove ASCII control characters (except newline \n=0x0A, tab \t=0x09, carriage return \r=0x0D)
    # Keep: \x09 (tab), \x0A (LF), \x0D (CR)
    # Remove: \x00-\x08, \x0B-\x0C, \x0E-\x1F, \x7F
    sql = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', sql)
    
    # Remove Unicode control characters and zero-width characters
    # Zero-width space, zero-width non-joiner, zero-width joiner, left-to-right mark, right-to-left mark
    sql = re.sub(r'[\u200B-\u200D\uFEFF\u200E\u200F]', '', sql)
    
    # Remove BOM (Byte Order Mark) if present
    if sql.startswith('\ufeff'):
        sql = sql[1:]
    
    # Remove other non-printable Unicode characters (but keep common punctuation and letters)
    # Keep printable ASCII (\x20-\x7E) and common whitespace (\n, \r, \t)
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


def format_parse_error(error: Exception, sql: str, dialect: Optional[str] = None, statement_num: Optional[int] = None) -> str:
    """
    Format a parse error with detailed information.
    
    Args:
        error: The parse error exception
        sql: The SQL string that failed to parse
        dialect: The dialect that was being used
        statement_num: Statement number if processing multiple statements
        
    Returns:
        Formatted error message string
    """
    error_msg = []
    
    # Error type and basic message
    error_type = type(error).__name__
    error_msg.append(f"Parse Error ({error_type})")
    
    if statement_num is not None:
        error_msg.append(f"Statement #{statement_num + 1}")
    
    if dialect:
        error_msg.append(f"Dialect: {dialect}")
    else:
        error_msg.append("Dialect: Generic SQL")
    
    error_msg.append("")
    
    # Get error message - ParseError has better structure
    line_num = None
    col_num = None
    
    if isinstance(error, ParseError):
        # ParseError has an 'errors' attribute with structured error info
        if hasattr(error, 'errors') and error.errors:
            # Use the first error's description (most relevant)
            first_error = error.errors[0]
            error_description = first_error.get('description', str(error))
            # Clean ANSI codes from error description
            error_description = re.sub(r'\x1b\[[0-9;]*m', '', error_description)
            error_description = re.sub(r'\033\[[0-9;]*m', '', error_description)
            error_description = re.sub(r'\[[0-9;]*m', '', error_description)
            error_msg.append(f"Error: {error_description}")
            
            # Extract line and column from error structure
            line_num = first_error.get('line')
            col_num = first_error.get('col')
            
            if line_num:
                error_msg.append(f"Line: {line_num}")
            if col_num:
                error_msg.append(f"Column: {col_num}")
        else:
            # Fallback to string representation
            error_str = str(error)
            # Remove ANSI codes from error message if present
            error_str = re.sub(r'\x1b\[[0-9;]*m', '', error_str)
            error_str = re.sub(r'\033\[[0-9;]*m', '', error_str)
            error_str = re.sub(r'\[[0-9;]*m', '', error_str)
            error_msg.append(f"Error: {error_str}")
            
            # Try to find line number in error message
            line_match = re.search(r'line\s+(\d+)', error_str, re.IGNORECASE)
            col_match = re.search(r'col(umn)?\s+(\d+)', error_str, re.IGNORECASE)
            
            if line_match:
                line_num = int(line_match.group(1))
            if col_match:
                col_num = int(col_match.group(2))
    else:
        # For non-ParseError exceptions
        error_str = str(error)
        # Remove ANSI codes from error message if present
        error_str = re.sub(r'\x1b\[[0-9;]*m', '', error_str)
        error_str = re.sub(r'\033\[[0-9;]*m', '', error_str)
        error_str = re.sub(r'\[[0-9;]*m', '', error_str)
        error_msg.append(f"Error: {error_str}")
        
        # Try to find line number in error message
        line_match = re.search(r'line\s+(\d+)', error_str, re.IGNORECASE)
        col_match = re.search(r'col(umn)?\s+(\d+)', error_str, re.IGNORECASE)
        
        if line_match:
            line_num = int(line_match.group(1))
        if col_match:
            col_num = int(col_match.group(2))
    
    # Show context around error line if we have line number
    if line_num:
        sql_lines = sql.split('\n')
        if 1 <= line_num <= len(sql_lines):
            start_line = max(0, line_num - 3)
            end_line = min(len(sql_lines), line_num + 2)
            
            error_msg.append("\nContext:")
            for i in range(start_line, end_line):
                line_prefix = ">>> " if i == line_num - 1 else "    "
                error_msg.append(f"{line_prefix}{i+1:4d}: {sql_lines[i]}")
    
    # Common error suggestions
    error_msg.append("\nSuggestions:")
    
    # Get error string for suggestions (use description if available, otherwise full error)
    error_str_for_suggestions = ""
    if isinstance(error, ParseError) and hasattr(error, 'errors') and error.errors:
        error_str_for_suggestions = error.errors[0].get('description', str(error)).lower()
    else:
        error_str_for_suggestions = str(error).lower()
    
    if "unexpected" in error_str_for_suggestions or "syntax" in error_str_for_suggestions:
        error_msg.append("  - Check for missing commas, parentheses, or quotes")
        error_msg.append("  - Verify SQL syntax matches the specified dialect")
        error_msg.append("  - Check for unclosed quotes or parentheses")
    
    if "unknown" in error_str_for_suggestions or "invalid" in error_str_for_suggestions:
        error_msg.append("  - Verify table/column names are correct")
        error_msg.append("  - Check for reserved keywords that need quoting")
        error_msg.append("  - Ensure dialect-specific syntax is correct")
    
    if not dialect:
        error_msg.append("  - Try specifying a dialect: --dialect tsql|mssql|postgres|mysql|snowflake|oracle|bigquery")
        error_msg.append("    (Note: 'tsql'/'mssql' is for SQL Server - 'mssql' is automatically converted to 'tsql')")
    
    return "\n".join(error_msg)


def normalize_dialect(dialect: Optional[str]) -> Optional[str]:
    """
    Normalize dialect names to sqlglot-compatible names.
    Converts common aliases to their sqlglot equivalents.
    
    Args:
        dialect: Dialect name (may be None or an alias)
    
    Returns:
        Normalized dialect name or None
    """
    if not dialect:
        return 'tsql'
    
    dialect_lower = dialect.lower().strip()
    
    # Map common aliases to sqlglot dialect names
    dialect_map = {
        'mssql': 'tsql',
        'sqlserver': 'tsql',
        'sql_server': 'tsql',
        'sql-server': 'tsql',
        't-sql': 'tsql',
        'tsql': 'tsql',  # Already correct
        'postgres': 'postgres',
        'postgresql': 'postgres',
        'mysql': 'mysql',
        'snowflake': 'snowflake',
        'oracle': 'oracle',
        'bigquery': 'bigquery',
        'big_query': 'bigquery',
    }
    
    return dialect_map.get(dialect_lower, dialect)  # Return original if not in map


def extract_table_columns(sql: str, dialect: Optional[str] = None, filepath: Optional[str] = None, error_details: Optional[list] = None) -> list:
    """
    Extract all table.column references from SQL.
    Resolves aliases to full table names and qualifies unqualified columns.
    Returns ALL occurrences (not unique) - every time a table.column is referenced.
    
    Args:
        sql: SQL string to parse
        dialect: SQL dialect to use (None = auto-detect)
        filepath: Optional filepath for better error messages
    
    Returns:
        List of strings in format "table.column" or "schema.table.column"
        Only includes columns that have a table reference (no standalone columns or tables)
    """
    columns = []
    
    # Normalize dialect name (convert mssql -> tsql, etc.)
    dialect = normalize_dialect(dialect)
    
    # If no dialect specified, try common dialects on parse failure
    # Note: 'tsql' is the sqlglot dialect name for SQL Server/MSSQL
    dialects_to_try = [dialect] if dialect else [None, 'tsql', 'postgres', 'mysql', 'snowflake', 'oracle', 'bigquery']
    
    last_error = None
    last_dialect = None
    failed_statements = []
    dialects_tried = []  # Track which dialects we actually attempted
    
    for try_dialect in dialects_to_try:
        dialect_name = try_dialect or 'generic'
        dialects_tried.append(dialect_name)
        
        try:
            statements = sqlglot.parse(sql, dialect=try_dialect)
            
            # Check if parsing succeeded (at least one non-None statement)
            if not statements or all(s is None for s in statements):
                # All statements failed to parse
                if len(statements) > 0:
                    # We got statements but they're all None - parsing failed
                    parse_error_msg = f"Failed to parse SQL with dialect '{try_dialect or 'generic'}': All statements returned None"
                    last_error = ParseError(parse_error_msg)
                    last_dialect = try_dialect
                    
                    # Capture this error in error_details
                    if error_details is not None:
                        formatted_error = format_parse_error(last_error, sql, try_dialect)
                        error_details.append({
                            'statement': 'all',
                            'dialect': try_dialect or 'generic',
                            'error': parse_error_msg,
                            'formatted': formatted_error
                        })
                continue
            
            # Process each statement individually, skip ones that fail
            for i, stmt in enumerate(statements):
                if stmt is None:
                    failed_statements.append((i, try_dialect, "Statement parsed as None"))
                    # Capture None statement errors
                    if error_details is not None:
                        error_msg = f"Statement {i+1} failed to parse (returned None) with dialect '{try_dialect or 'generic'}'"
                        # Try to create a formatted error
                        try:
                            # Create a simple error for None statements
                            none_error = ParseError(error_msg)
                            formatted_error = format_parse_error(none_error, sql, try_dialect, i)
                        except:
                            formatted_error = f"Parse Error\nStatement #{i+1}\nDialect: {try_dialect or 'generic'}\n\nError: {error_msg}"
                        
                        error_details.append({
                            'statement': i + 1,
                            'dialect': try_dialect or 'generic',
                            'error': error_msg,
                            'formatted': formatted_error
                        })
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
                                # Filter out wildcard columns (table.*)
                                if qualified_name.endswith('.*'):
                                    continue
                                columns.append(qualified_name)
                            # Skip columns without table reference
                
                except Exception as e:
                    # Skip this statement but continue with others
                    failed_statements.append((i, try_dialect, str(e)))
                    formatted_error = format_parse_error(e, sql, try_dialect, i)
                    file_info = f" in {filepath}" if filepath else ""
                    
                    # Store error details if list provided
                    if error_details is not None:
                        error_details.append({
                            'statement': i + 1,
                            'dialect': try_dialect or 'generic',
                            'error': str(e),
                            'formatted': formatted_error
                        })
                    
                    print(f"\n{'='*80}", file=sys.stderr)
                    print(f"Warning: Failed to process statement {i+1}{file_info}", file=sys.stderr)
                    print(formatted_error, file=sys.stderr)
                    print(f"{'='*80}\n", file=sys.stderr)
                    continue
            
            # If we extracted any columns, return them (even if some statements failed)
            if columns:
                if failed_statements:
                    print(f"Note: Successfully extracted columns despite {len(failed_statements)} failed statement(s)", file=sys.stderr)
                # Even if we got some columns, we should still report failed statements
                # But don't treat this as a complete failure
                return columns
            
            # If we got here, no columns were extracted with this dialect
            # This could mean:
            # 1. All statements failed to parse (handled above with continue)
            # 2. Statements parsed but had no extractable columns
            # 3. All statements failed during processing (all hit exceptions)
            # Continue to next dialect to try parsing with a different dialect
            # Note: We don't return here - we continue the loop to try next dialect
            if failed_statements and error_details is not None:
                # We already captured errors above, but make sure they're in error_details
                # Errors should already be captured in the loop above
                pass
            
            # Reset failed_statements for next dialect attempt
            failed_statements = []
            
        except ParseError as e:
            last_error = e
            last_dialect = try_dialect
            # Store error for this dialect attempt
            if error_details is not None:
                formatted_error = format_parse_error(e, sql, try_dialect)
                # Clean ANSI codes from error string
                error_str = str(e)
                error_str = re.sub(r'\x1b\[[0-9;]*m', '', error_str)
                error_str = re.sub(r'\033\[[0-9;]*m', '', error_str)
                error_str = re.sub(r'\[[0-9;]*m', '', error_str)
                error_details.append({
                    'statement': 'all',
                    'dialect': try_dialect or 'generic',
                    'error': error_str,
                    'formatted': formatted_error
                })
            # Try next dialect
            continue
        except Exception as e:
            last_error = e
            last_dialect = try_dialect
            # Store error for this dialect attempt
            if error_details is not None:
                formatted_error = format_parse_error(e, sql, try_dialect)
                # Clean ANSI codes from error string
                error_str = str(e)
                error_str = re.sub(r'\x1b\[[0-9;]*m', '', error_str)
                error_str = re.sub(r'\033\[[0-9;]*m', '', error_str)
                error_str = re.sub(r'\[[0-9;]*m', '', error_str)
                error_details.append({
                    'statement': 'all',
                    'dialect': try_dialect or 'generic',
                    'error': error_str,
                    'formatted': formatted_error
                })
            # Try next dialect
            continue
    
    # If all dialects failed, print detailed error
    if last_error:
        formatted_error = format_parse_error(last_error, sql, last_dialect)
        file_info = f"\nFile: {filepath}" if filepath else ""
        dialects_tried_str = ', '.join(dialects_tried)
        
        # Store error details if list provided
        if error_details is not None:
            # Check if we already have an error for this dialect
            if not any(ed.get('statement') == 'all' and ed.get('dialect') == (last_dialect or 'generic') for ed in error_details):
                error_details.append({
                    'statement': 'all',
                    'dialect': last_dialect or 'generic',
                    'error': str(last_error),
                    'formatted': formatted_error,
                    'dialects_tried': dialects_tried_str
                })
        
        print(f"\n{'='*80}", file=sys.stderr)
        print(f"ERROR: Failed to parse SQL{file_info}", file=sys.stderr)
        print(f"Dialects tried ({len(dialects_tried)}): {dialects_tried_str}", file=sys.stderr)
        print(formatted_error, file=sys.stderr)
        print(f"{'='*80}\n", file=sys.stderr)
    
    return columns


def process_sql_file(filepath: Path, dialect: Optional[str] = None, error_details: Optional[list] = None) -> list:
    """
    Process a single SQL file and return all table.column references (all occurrences).
    
    Args:
        filepath: Path to SQL file
        dialect: SQL dialect to use (None = auto-detect)
        error_details: Optional list to collect detailed error information
    
    Returns:
        List of table.column references
    """
    try:
        with open(filepath, "r", encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        if not content.strip():
            print(f"Warning: File {filepath} is empty", file=sys.stderr)
            return []
        
        # Preprocess SQL to handle edge cases
        content = preprocess_sql(content)
        
        if not content.strip():
            print(f"Warning: File {filepath} contains only comments/DDL (no query statements)", file=sys.stderr)
            return []
        
        return extract_table_columns(content, dialect, str(filepath), error_details)
    
    except FileNotFoundError:
        print(f"ERROR: File not found: {filepath}", file=sys.stderr)
        return []
    except PermissionError:
        print(f"ERROR: Permission denied reading file: {filepath}", file=sys.stderr)
        return []
    except UnicodeDecodeError as e:
        print(f"ERROR: Unable to decode file {filepath}: {e}", file=sys.stderr)
        print("  Try checking file encoding or use a different encoding", file=sys.stderr)
        return []
    except Exception as e:
        print(f"ERROR: Unexpected error reading file {filepath}: {e}", file=sys.stderr)
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        return []


def setup_logging(log_file: Path):
    """Setup logging to both file and console."""
    # Create log file path
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


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
        help="SQL dialect (tsql/mssql for SQL Server, postgres, mysql, snowflake, oracle, bigquery, etc.). Note: 'mssql' is automatically converted to 'tsql'."
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
    
    # Normalize dialect if provided (convert mssql -> tsql, etc.)
    if args.dialect:
        args.dialect = normalize_dialect(args.dialect)
    
    # Determine output directory and file names
    if args.output:
        output_path = Path(args.output)
        # If output is a directory, use it; otherwise extract directory from file path
        if output_path.suffix:  # It's a file path
            output_dir = output_path.parent
            output_filename = output_path.name
        else:  # It's a directory
            output_dir = output_path
            output_filename = "columns.csv"
    else:
        # Default to output/ directory in current directory
        output_dir = Path("output")
        output_filename = "columns.csv"
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create Error_Reports subdirectory
    error_reports_dir = output_dir / "Error_Reports"
    error_reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Set up file paths
    output_path = output_dir / output_filename
    log_file = output_dir / output_path.with_suffix('.log').name
    errors_file = output_dir / "errors.txt"
    
    # Setup logging (log file will be in output directory)
    logger = setup_logging(log_file)
    
    logger.info("="*80)
    logger.info("SQL Column Extractor - Starting")
    logger.info("="*80)
    logger.info(f"Output file: {output_path.absolute()}")
    logger.info(f"Log file: {log_file.absolute()}")
    
    # Determine which files to process
    sql_files = []
    
    # Check if FOLDER_PATH is set (highest priority)
    if FOLDER_PATH and FOLDER_PATH.strip():
        folder_path = Path(FOLDER_PATH.strip())
        if not folder_path.exists():
            error_msg = f"FOLDER_PATH not found: {folder_path}"
            logger.error(error_msg)
            logger.error("Please check the FOLDER_PATH variable at the top of the script.")
            print(f"Error: {error_msg}", file=sys.stderr)
            print("Please check the FOLDER_PATH variable at the top of the script.", file=sys.stderr)
            return
        
        if folder_path.is_file():
            # Single file
            if folder_path.suffix.lower() == '.sql':
                sql_files.append(folder_path)
                logger.info(f"Using FOLDER_PATH (file): {folder_path}")
                print(f"Using FOLDER_PATH: {folder_path}")
            else:
                error_msg = f"FOLDER_PATH is not a SQL file: {folder_path}"
                logger.error(error_msg)
                print(f"Error: {error_msg}", file=sys.stderr)
                return
        elif folder_path.is_dir():
            # Directory - find all SQL files recursively
            found_files = sorted(folder_path.rglob("*.sql"))
            sql_files.extend(found_files)
            logger.info(f"Using FOLDER_PATH (directory): {folder_path}")
            logger.info(f"Found {len(found_files)} SQL file(s)")
            print(f"Using FOLDER_PATH: {folder_path}")
            print(f"Found {len(found_files)} SQL file(s) in {folder_path}")
        else:
            error_msg = f"FOLDER_PATH is not a valid file or directory: {folder_path}"
            logger.error(error_msg)
            print(f"Error: {error_msg}", file=sys.stderr)
            return
    
    elif args.files:
        # Process provided files/directories from command-line
        for file_path in args.files:
            path = Path(file_path)
            if not path.exists():
                warning_msg = f"Path not found: {path}"
                logger.warning(warning_msg)
                print(f"Warning: {warning_msg}", file=sys.stderr)
                continue
            
            if path.is_file():
                # Single file
                if path.suffix.lower() == '.sql':
                    sql_files.append(path)
                else:
                    warning_msg = f"Skipping non-SQL file: {path}"
                    logger.warning(warning_msg)
                    print(f"Warning: {warning_msg}", file=sys.stderr)
            elif path.is_dir():
                # Directory - find all SQL files recursively
                found_files = sorted(path.rglob("*.sql"))
                sql_files.extend(found_files)
                logger.info(f"Found {len(found_files)} SQL file(s) in {path}")
                print(f"Found {len(found_files)} SQL file(s) in {path}")
    else:
        # Default to current directory (recursive)
        sql_files = sorted(Path.cwd().rglob("*.sql"))
        logger.info(f"Using current directory: {Path.cwd()}")
    
    if not sql_files:
        error_msg = "No SQL files found to process."
        logger.error(error_msg)
        print(error_msg)
        return
    
    # Collect all column references with file tracking
    # Structure: list of tuples (filename, column_name)
    # We'll deduplicate per file (keep unique columns per file)
    column_data = []
    file_columns = {}
    
    # Error tracking
    files_with_errors = []  # Files that threw exceptions
    files_with_zero_columns = []  # Files that processed but found 0 columns
    files_successful = []  # Files that successfully found columns
    
    logger.info(f"\nProcessing {len(sql_files)} SQL file(s)...")
    print(f"\nProcessing {len(sql_files)} SQL file(s)...")
    
    for sql_file in sql_files:
        logger.info(f"Processing: {sql_file}")
        print(f"  Processing: {sql_file}")
        file_error_details = []  # Collect error details for this file
        try:
            columns = process_sql_file(sql_file, args.dialect, file_error_details)
            
            # Parse filename to get report name and dataset
            report_name, dataset = parse_filename(sql_file)
            logger.info(f"  Parsed: Report='{report_name}', Dataset='{dataset}'")
            
            # Store full relative path or just filename for file_columns tracking
            file_key = str(sql_file.relative_to(Path.cwd())) if sql_file.is_relative_to(Path.cwd()) else str(sql_file)
            
            # Filter out wildcard columns (table.*) and deduplicate per file
            # Keep only unique columns per file (can have duplicates across files)
            unique_columns_for_file = []
            seen_in_file = set()
            wildcard_count = 0
            
            for col in columns:
                # Skip wildcard columns (table.*)
                if col.endswith('.*'):
                    wildcard_count += 1
                    logger.debug(f"Skipping wildcard column: {col}")
                    continue
                # Deduplicate within this file
                if col not in seen_in_file:
                    seen_in_file.add(col)
                    unique_columns_for_file.append(col)
            
            file_columns[file_key] = unique_columns_for_file
            
            # Track files with zero columns
            if len(unique_columns_for_file) == 0:
                import shutil
                zero_col_info = {
                    'file': file_key,
                    'report_name': report_name,
                    'dataset': dataset,
                    'total_extracted': len(columns),
                    'wildcards_filtered': wildcard_count
                }
                # Add parse errors if available (file failed to parse)
                if file_error_details:
                    zero_col_info['parse_errors'] = file_error_details
                    # Log parse errors for zero-column files
                    logger.warning(f"  No unique columns found in {report_name} ({dataset}) - Parse errors detected: {len(file_error_details)}")
                    for parse_err in file_error_details:
                        logger.warning(f"    Parse error: Statement {parse_err.get('statement', 'unknown')}, Dialect: {parse_err.get('dialect', 'unknown')}")
                else:
                    logger.warning(f"  No unique columns found in {report_name} ({dataset}) - Total extracted: {len(columns)}, Wildcards filtered: {wildcard_count}")
                files_with_zero_columns.append(zero_col_info)
                
                # Copy file with zero columns to Error_Reports directory
                try:
                    error_report_path = error_reports_dir / sql_file.name
                    shutil.copy2(sql_file, error_report_path)
                    logger.info(f"Copied file with 0 columns to Error_Reports: {error_report_path}")
                except Exception as copy_error:
                    logger.error(f"Failed to copy file {sql_file} to Error_Reports: {copy_error}", exc_info=True)
            else:
                files_successful.append(file_key)
                # Add each unique column with parsed report name and dataset
                for col in unique_columns_for_file:
                    column_data.append((report_name, dataset, col))
                logger.info(f"  Found {len(unique_columns_for_file)} unique columns in {report_name} ({dataset})")
        except Exception as e:
            import traceback
            error_msg = f"Error processing {sql_file}: {e}"
            full_traceback = traceback.format_exc()
            logger.error(error_msg, exc_info=True)
            logger.error(f"Full traceback for {sql_file}:\n{full_traceback}")
            print(f"  {error_msg}", file=sys.stderr)
            
            # Copy failed file to Error_Reports directory
            try:
                error_report_path = error_reports_dir / sql_file.name
                shutil.copy2(sql_file, error_report_path)
                logger.info(f"Copied failed file to Error_Reports: {error_report_path}")
            except Exception as copy_error:
                logger.error(f"Failed to copy error file {sql_file} to Error_Reports: {copy_error}", exc_info=True)
            
            # Combine error details if available
            error_info = {
                'file': str(sql_file),
                'error': str(e),
                'traceback': full_traceback
            }
            
            # Add detailed parse errors if available
            if file_error_details:
                error_info['parse_errors'] = file_error_details
            
            files_with_errors.append(error_info)
            continue
    
    # Count unique vs total
    all_columns = [col for _, _, col in column_data]
    unique_columns = sorted(set(all_columns))
    total_count = len(column_data)
    unique_count = len(unique_columns)
    
    logger.info(f"\nFound {total_count} total table.column references ({unique_count} unique)")
    logger.info(f"Across {len(file_columns)} file(s)")
    print(f"\nFound {total_count} total table.column references ({unique_count} unique)")
    print(f"Across {len(file_columns)} file(s)")
    
    # Output to CSV or Excel
    
    if output_path.suffix.lower() == '.xlsx':
        # Excel output
        try:
            import pandas as pd
            
            # Create DataFrame with ReportName, Dataset, and ColumnName
            df = pd.DataFrame({
                'ReportName': [report for report, _, _ in column_data],
                'Dataset': [dataset for _, dataset, _ in column_data],
                'ColumnName': [col for _, _, col in column_data]
            })
            
            df.to_excel(output_path, index=False)
            abs_path = output_path.absolute()
            logger.info(f"Excel output written successfully: {abs_path}")
            logger.info(f"  Rows: {len(df)} (unique columns per file)")
            logger.info(f"  Columns: ReportName, Dataset, ColumnName")
            print("\n" + "="*80)
            print("OUTPUT FILE LOCATION:")
            print("="*80)
            print(f"  {abs_path}")
            print("="*80)
            print(f"\n✓ Output written to: {abs_path}")
            print(f"  Rows: {len(df)} (unique columns per file)")
            print(f"  Columns: ReportName, Dataset, ColumnName")
            print(f"  Output directory: {output_dir.absolute()}")
            print(f"  Log file: {log_file.absolute()}")
            print(f"  Errors file: {errors_file.absolute()}")
            print(f"  Error reports: {error_reports_dir.absolute()}")
            
        except ImportError:
            error_msg = "pandas and openpyxl required for Excel output"
            logger.warning(error_msg)
            logger.warning("Falling back to CSV output...")
            print("\nError: pandas and openpyxl required for Excel output.")
            print("Install with: pip install pandas openpyxl")
            print("\nFalling back to CSV output...")
            output_path = output_path.with_suffix('.csv')
            # Update log file path if we changed output path
            log_file = output_path.with_suffix('.log')
    
    if output_path.suffix.lower() == '.csv':
        # CSV output
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['ReportName', 'Dataset', 'ColumnName'])  # Header
            
            for report_name, dataset, col in column_data:
                writer.writerow([report_name, dataset, col])
        
        abs_path = output_path.absolute()
        logger.info(f"CSV output written successfully: {abs_path}")
        logger.info(f"  Rows: {len(column_data)} (unique columns per file)")
        logger.info(f"  Columns: ReportName, Dataset, ColumnName")
        print("\n" + "="*80)
        print("OUTPUT FILE LOCATION:")
        print("="*80)
        print(f"  {abs_path}")
        print("="*80)
        print(f"\n✓ Output written to: {abs_path}")
        print(f"  Rows: {len(column_data)} (unique columns per file)")
        print(f"  Columns: ReportName, Dataset, ColumnName")
        print(f"  Output directory: {output_dir.absolute()}")
        print(f"  Log file: {log_file.absolute()}")
        print(f"  Errors file: {errors_file.absolute()}")
        print(f"  Error reports: {error_reports_dir.absolute()}")
    
    # Show summary by file
    logger.info("\n" + "="*60)
    logger.info("SUMMARY BY FILE")
    logger.info("="*60)
    print("\n" + "="*60)
    print("SUMMARY BY FILE")
    print("="*60)
    for file_key, cols in sorted(file_columns.items()):
        # Parse filename for display
        file_path = Path(file_key)
        report_name, dataset = parse_filename(file_path)
        unique_in_file = len(set(cols))
        summary_line = f"  {report_name} ({dataset}): {len(cols)} unique columns"
        logger.info(summary_line)
        print(summary_line)
    
    # Show error summary
    print("\n" + "="*60)
    print("PROCESSING SUMMARY")
    print("="*60)
    total_files = len(sql_files)
    successful_count = len(files_successful)
    zero_columns_count = len(files_with_zero_columns)
    error_count = len(files_with_errors)
    
    logger.info("\n" + "="*60)
    logger.info("PROCESSING SUMMARY")
    logger.info("="*60)
    logger.info(f"Total files processed: {total_files}")
    logger.info(f"Successfully processed with columns: {successful_count}")
    logger.info(f"Files with 0 columns found: {zero_columns_count}")
    logger.info(f"Files with errors: {error_count}")
    
    print(f"Total files processed: {total_files}")
    print(f"Successfully processed with columns: {successful_count}")
    print(f"Files with 0 columns found: {zero_columns_count}")
    print(f"Files with errors: {error_count}")
    
    # Show files with zero columns
    if files_with_zero_columns:
        print(f"\nFiles with 0 columns found ({zero_columns_count}):")
        logger.info(f"\nFiles with 0 columns found ({zero_columns_count}):")
        for file_info in files_with_zero_columns[:20]:  # Show first 20
            msg = f"  {file_info['report_name']} ({file_info['dataset']}): {file_info['file']}"
            if file_info['total_extracted'] > 0:
                msg += f" - {file_info['total_extracted']} columns extracted but filtered (wildcards: {file_info['wildcards_filtered']})"
            else:
                msg += " - No columns extracted (may be DDL-only or parse error)"
            logger.info(msg)
            print(msg)
        if len(files_with_zero_columns) > 20:
            remaining = len(files_with_zero_columns) - 20
            msg = f"  ... and {remaining} more files with 0 columns"
            logger.info(msg)
            print(msg)
    
    # Show files with errors
    if files_with_errors:
        print(f"\nFiles with processing errors ({error_count}):")
        logger.error(f"\nFiles with processing errors ({error_count}):")
        for file_info in files_with_errors[:20]:  # Show first 20
            msg = f"  {file_info['file']}: {file_info['error']}"
            logger.error(msg)
            print(msg)
            # Log full traceback to log file
            if 'traceback' in file_info:
                logger.error(f"Full traceback for {file_info['file']}:\n{file_info['traceback']}")
        if len(files_with_errors) > 20:
            remaining = len(files_with_errors) - 20
            msg = f"  ... and {remaining} more files with errors"
            logger.info(msg)
            print(msg)
            # Log tracebacks for remaining files
            for file_info in files_with_errors[20:]:
                logger.error(f"Error in {file_info['file']}: {file_info['error']}")
                if 'traceback' in file_info:
                    logger.error(f"Full traceback for {file_info['file']}:\n{file_info['traceback']}")
    
    # Write errors to errors.txt file (always write if there are any issues)
    if files_with_errors or files_with_zero_columns:
        try:
            with open(errors_file, 'w', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write("ERROR REPORT\n")
                f.write("="*80 + "\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total files processed: {len(sql_files)}\n")
                f.write(f"Files with errors: {len(files_with_errors)}\n")
                f.write(f"Files with 0 columns: {len(files_with_zero_columns)}\n")
                f.write(f"Files successfully processed: {len(files_successful)}\n\n")
                
                if files_with_errors:
                    f.write(f"FILES WITH PROCESSING ERRORS ({len(files_with_errors)}):\n")
                    f.write("-"*80 + "\n")
                    for file_info in files_with_errors:
                        f.write(f"\nFile: {file_info['file']}\n")
                        f.write(f"Error: {file_info['error']}\n")
                        
                        # Write detailed parse errors if available
                        if 'parse_errors' in file_info and file_info['parse_errors']:
                            f.write(f"\nDetailed Parse Errors:\n")
                            for parse_error in file_info['parse_errors']:
                                f.write(f"\n  Statement #{parse_error.get('statement', 'unknown')}\n")
                                f.write(f"  Dialect: {parse_error.get('dialect', 'unknown')}\n")
                                if 'dialects_tried' in parse_error:
                                    f.write(f"  Dialects tried: {parse_error['dialects_tried']}\n")
                                f.write(f"  Error: {parse_error.get('error', 'Unknown error')}\n")
                                f.write(f"\n  Detailed Error Information:\n")
                                # Indent the formatted error
                                formatted = parse_error.get('formatted', '')
                                for line in formatted.split('\n'):
                                    f.write(f"    {line}\n")
                        
                        if 'traceback' in file_info:
                            f.write(f"\nTraceback:\n{file_info['traceback']}\n")
                        f.write("-"*80 + "\n")
                    f.write("\n")
                
                if files_with_zero_columns:
                    f.write(f"FILES WITH 0 COLUMNS FOUND ({len(files_with_zero_columns)}):\n")
                    f.write("-"*80 + "\n")
                    for file_info in files_with_zero_columns:
                        f.write(f"\nFile: {file_info['file']}\n")
                        f.write(f"Report: {file_info['report_name']} ({file_info['dataset']})\n")
                        if file_info['total_extracted'] > 0:
                            f.write(f"Reason: {file_info['total_extracted']} columns extracted but filtered ")
                            f.write(f"(wildcards: {file_info['wildcards_filtered']})\n")
                        else:
                            # Check if this is a parse error or just DDL/empty
                            if 'parse_errors' in file_info and file_info['parse_errors']:
                                f.write(f"Reason: Parse error(s) prevented column extraction\n")
                            else:
                                f.write(f"Reason: No columns extracted (may be DDL-only, empty file, or parse error)\n")
                        
                        # Write detailed parse errors if available
                        if 'parse_errors' in file_info and file_info['parse_errors']:
                            f.write(f"\nDetailed Parse Errors:\n")
                            for parse_error in file_info['parse_errors']:
                                f.write(f"\n  Statement #{parse_error.get('statement', 'unknown')}\n")
                                f.write(f"  Dialect: {parse_error.get('dialect', 'unknown')}\n")
                                if 'dialects_tried' in parse_error:
                                    f.write(f"  Dialects tried: {parse_error['dialects_tried']}\n")
                                f.write(f"  Error: {parse_error.get('error', 'Unknown error')}\n")
                                f.write(f"\n  Detailed Error Information:\n")
                                # Indent the formatted error
                                formatted = parse_error.get('formatted', '')
                                for line in formatted.split('\n'):
                                    f.write(f"    {line}\n")
                        
                        f.write("-"*80 + "\n")
                    f.write("\n")
                
                f.write(f"\nTotal files with errors: {len(files_with_errors)}\n")
                f.write(f"Total files with 0 columns: {len(files_with_zero_columns)}\n")
                f.write(f"\nAll error files have been copied to: {error_reports_dir.absolute()}\n")
            
            logger.info(f"Error report written to: {errors_file.absolute()}")
            print(f"\nError report written to: {errors_file.absolute()}")
        except Exception as e:
            logger.error(f"Failed to write error report file: {e}", exc_info=True)
    
    # Show sample of extracted columns
    logger.info("\n" + "="*60)
    logger.info("SAMPLE OUTPUT (first 20 rows)")
    logger.info("="*60)
    print("\n" + "="*60)
    print("SAMPLE OUTPUT (first 20 rows)")
    print("="*60)
    for report_name, dataset, col in column_data[:20]:
        sample_line = f"  {report_name} | {dataset} | {col}"
        logger.info(sample_line)
        print(sample_line)
    if len(column_data) > 20:
        remaining_msg = f"  ... and {len(column_data) - 20} more rows"
        logger.info(remaining_msg)
        print(remaining_msg)
    
    logger.info("="*80)
    logger.info("SQL Column Extractor - Completed Successfully")
    logger.info("="*80)


if __name__ == "__main__":
    main()

