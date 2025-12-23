# Edge Cases Coverage Checklist

This document verifies that all edge cases listed in EDGE_CASES.md have corresponding test files.

## ✅ Complete Coverage Verification

### 1. Advanced JOIN Types ✅
- [x] **LATERAL JOINs** → `01_lateral_joins.sql`
- [x] **CROSS APPLY / OUTER APPLY** → `02_cross_apply.sql`
- [x] **NATURAL JOINs** → `28_natural_join_using.sql`
- [x] **USING clause** → `28_natural_join_using.sql`

### 2. Window Functions and Advanced Analytics ✅
- [x] **Window functions with PARTITION BY** → `03_window_functions.sql`
- [x] **Window functions with multiple columns** → `03_window_functions.sql`
- [x] **Named window definitions** → `03_window_functions.sql`
- [x] **QUALIFY clause** → `10_qualify_clause.sql`

### 3. Table-Valued Functions ✅
- [x] **Table-valued functions** → `08_table_valued_functions.sql`
- [x] **JSON table functions** → `08_table_valued_functions.sql`, `14_json_xml_functions.sql`
- [x] **XML table functions** → `08_table_valued_functions.sql`, `14_json_xml_functions.sql`

### 4. PIVOT and UNPIVOT ✅
- [x] **PIVOT operations** → `07_pivot_unpivot.sql`
- [x] **UNPIVOT operations** → `07_pivot_unpivot.sql`

### 5. Recursive CTEs ✅
- [x] **Recursive CTEs with UNION ALL** → `04_recursive_ctes.sql`
- [x] **Recursive CTEs with multiple base cases** → `04_recursive_ctes.sql`

### 6. Correlated Subqueries ✅
- [x] **Correlated subqueries in SELECT** → `09_correlated_subqueries.sql`
- [x] **Correlated subqueries in WHERE** → `09_correlated_subqueries.sql`
- [x] **Multiple levels of correlation** → `09_correlated_subqueries.sql`

### 7. Derived Tables and Inline Views ✅
- [x] **Derived tables with complex expressions** → `31_derived_tables.sql`
- [x] **Multiple derived tables** → `31_derived_tables.sql`

### 8. Set Operations ✅
- [x] **INTERSECT operations** → `13_set_operations.sql`
- [x] **EXCEPT operations** → `13_set_operations.sql`
- [x] **Multiple UNIONs** → `13_set_operations.sql`

### 9. Schema/Database Qualification ✅
- [x] **Three-part names** → `06_multipart_names.sql`
- [x] **Four-part names** → `06_multipart_names.sql`
- [x] **Quoted identifiers** → `23_special_characters.sql`
- [x] **Mixed case identifiers** → `23_special_characters.sql`

### 10. Column Aliases and Expressions ✅
- [x] **Column aliases in SELECT** → `32_column_aliases_expressions.sql`
- [x] **Expressions referencing other columns** → `32_column_aliases_expressions.sql`
- [x] **Aggregate expressions** → `32_column_aliases_expressions.sql`

### 11. WHERE Clause Edge Cases ✅
- [x] **Subqueries in WHERE** → `33_where_clause_edge_cases.sql`
- [x] **EXISTS subqueries** → `33_where_clause_edge_cases.sql`
- [x] **Multiple conditions with OR** → `33_where_clause_edge_cases.sql`

### 12. HAVING Clause ✅
- [x] **HAVING with aggregates** → `34_having_clause.sql`
- [x] **HAVING with subqueries** → `34_having_clause.sql`

### 13. ORDER BY and LIMIT/OFFSET ✅
- [x] **ORDER BY with expressions** → `29_order_by_limit.sql`
- [x] **LIMIT/OFFSET** → `29_order_by_limit.sql`

### 14. GROUP BY Edge Cases ✅
- [x] **GROUP BY with expressions** → `12_advanced_group_by.sql`
- [x] **GROUP BY ROLLUP** → `12_advanced_group_by.sql`
- [x] **GROUP BY CUBE** → `12_advanced_group_by.sql`
- [x] **GROUP BY GROUPING SETS** → `12_advanced_group_by.sql`

### 15. CASE Expressions ✅
- [x] **CASE expressions with subqueries** → `16_case_expressions.sql`
- [x] **Searched CASE expressions** → `16_case_expressions.sql`

### 16. NULL Handling ✅
- [x] **IS NULL / IS NOT NULL** → `17_null_handling.sql`
- [x] **COALESCE / NULLIF** → `17_null_handling.sql`

### 17. String Functions and Expressions ✅
- [x] **String concatenation** → `18_string_functions.sql`
- [x] **String functions with table references** → `18_string_functions.sql`

### 18. Date/Time Functions ✅
- [x] **Date functions** → `19_date_time_functions.sql`
- [x] **Date arithmetic** → `19_date_time_functions.sql`

### 19. Aggregate Functions ✅
- [x] **DISTINCT in aggregates** → `20_aggregate_functions.sql`
- [x] **Filtered aggregates** → `20_aggregate_functions.sql`

### 20. Multiple Statements ✅
- [x] **Multiple SELECT statements** → `21_multiple_statements.sql`
- [x] **Mixed statement types** → `21_multiple_statements.sql`

### 21. Comments and Formatting ✅
- [x] **Multi-line comments** → `22_comments_formatting.sql`
- [x] **Inline comments** → `22_comments_formatting.sql`
- [x] **Comments in expressions** → `22_comments_formatting.sql`

### 22. Special Characters and Unicode ✅
- [x] **Unicode identifiers** → `23_special_characters.sql`
- [x] **Special characters in identifiers** → `23_special_characters.sql`
- [x] **Emojis in identifiers** → `23_special_characters.sql`

### 23. Dynamic SQL ✅
- [x] **EXEC/EXECUTE statements** → `35_dynamic_sql.sql`
- [x] **Prepared statements** → `35_dynamic_sql.sql`

### 24. Views and Materialized Views ✅
- [x] **Views in FROM clause** → `24_views_materialized.sql`
- [x] **Materialized views** → `24_views_materialized.sql`

### 25. Table Aliases Edge Cases ✅
- [x] **Same alias used multiple times** → `25_table_aliases_edge_cases.sql`
- [x] **Alias same as table name** → `25_table_aliases_edge_cases.sql`
- [x] **No alias on subquery** → `25_table_aliases_edge_cases.sql`

### 26. Column Resolution Edge Cases ✅
- [x] **Ambiguous column names** → `05_ambiguous_columns.sql`, `26_column_resolution_edge_cases.sql`
- [x] **Columns that exist in multiple tables** → `26_column_resolution_edge_cases.sql`
- [x] **Star expansion** → `26_column_resolution_edge_cases.sql`

### 27. Nested Subqueries ✅
- [x] **Deeply nested subqueries (3+ levels)** → `11_deep_nesting.sql`, `27_nested_subqueries.sql`

### 28. Dialect-Specific Features ✅
- [x] **PostgreSQL-specific** → `15_dialect_specific.sql`
- [x] **SQL Server-specific** → `15_dialect_specific.sql`
- [x] **MySQL-specific** → `15_dialect_specific.sql`
- [x] **Oracle-specific** → `15_dialect_specific.sql`
- [x] **Snowflake-specific** → `15_dialect_specific.sql`

### 29. Error Handling Edge Cases ✅
- [x] **Malformed SQL** → `30_error_handling.sql`
- [x] **Incomplete statements** → `30_error_handling.sql`
- [x] **Unclosed quotes** → `30_error_handling.sql`
- [x] **Unclosed parentheses** → `30_error_handling.sql`

### 30. Performance and Scale Edge Cases ⚠️
- [ ] **Very large queries (1000+ columns)** - Not practical to include in test files
- [ ] **Very deep nesting (20+ levels)** - Covered by `11_deep_nesting.sql` (4 levels)
- [ ] **Many CTEs (50+ CTEs)** - Not practical to include in test files
- [ ] **Many JOINs (100+ JOINs)** - Not practical to include in test files

**Note:** Performance and scale edge cases (#30) are not practical to include as test files due to their size, but the existing test files demonstrate the extractor's ability to handle complex queries. These would be better tested with performance/stress tests rather than static test files.

## Summary

- **Total Edge Case Categories:** 30
- **Categories with Test Files:** 29
- **Categories Fully Covered:** 29
- **Categories Partially Covered:** 1 (Performance/Scale - intentionally excluded)

## Test Files Created

1. `01_lateral_joins.sql` - LATERAL JOINs
2. `02_cross_apply.sql` - CROSS APPLY / OUTER APPLY
3. `03_window_functions.sql` - Window Functions
4. `04_recursive_ctes.sql` - Recursive CTEs
5. `05_ambiguous_columns.sql` - Ambiguous Columns
6. `06_multipart_names.sql` - Multi-part Names
7. `07_pivot_unpivot.sql` - PIVOT/UNPIVOT
8. `08_table_valued_functions.sql` - Table-Valued Functions
9. `09_correlated_subqueries.sql` - Correlated Subqueries
10. `10_qualify_clause.sql` - QUALIFY Clause
11. `11_deep_nesting.sql` - Deep Nesting
12. `12_advanced_group_by.sql` - Advanced GROUP BY
13. `13_set_operations.sql` - Set Operations
14. `14_json_xml_functions.sql` - JSON/XML Functions
15. `15_dialect_specific.sql` - Dialect-Specific Features
16. `16_case_expressions.sql` - CASE Expressions
17. `17_null_handling.sql` - NULL Handling
18. `18_string_functions.sql` - String Functions
19. `19_date_time_functions.sql` - Date/Time Functions
20. `20_aggregate_functions.sql` - Aggregate Functions
21. `21_multiple_statements.sql` - Multiple Statements
22. `22_comments_formatting.sql` - Comments
23. `23_special_characters.sql` - Special Characters
24. `24_views_materialized.sql` - Views
25. `25_table_aliases_edge_cases.sql` - Table Aliases
26. `26_column_resolution_edge_cases.sql` - Column Resolution
27. `27_nested_subqueries.sql` - Nested Subqueries
28. `28_natural_join_using.sql` - NATURAL JOIN / USING
29. `29_order_by_limit.sql` - ORDER BY / LIMIT
30. `30_error_handling.sql` - Error Handling
31. `31_derived_tables.sql` - Derived Tables
32. `32_column_aliases_expressions.sql` - Column Aliases
33. `33_where_clause_edge_cases.sql` - WHERE Clause
34. `34_having_clause.sql` - HAVING Clause
35. `35_dynamic_sql.sql` - Dynamic SQL

**Total Test Files:** 35

## Verification Complete ✅

All listed edge cases from EDGE_CASES.md have been covered with corresponding test SQL files, except for performance/scale edge cases which are intentionally excluded as they're not practical for static test files.

