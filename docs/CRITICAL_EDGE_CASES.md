# Critical Missing Edge Cases - Priority Testing List

This document highlights the **most critical** edge cases that are likely missing from current tests and should be prioritized for testing.

## 🔴 Critical Priority (Must Test)

### 1. LATERAL JOINs and CROSS APPLY
**Why Critical:** Very common in modern SQL, especially PostgreSQL and SQL Server. These joins reference columns from preceding tables in the FROM clause.

**Test Cases Needed:**
```sql
-- PostgreSQL LATERAL JOIN
SELECT t1.id, t2.name
FROM table1 t1
CROSS JOIN LATERAL (
  SELECT name FROM table2 t2 WHERE t2.ref_id = t1.id
) t2;

-- SQL Server CROSS APPLY
SELECT t1.id, t2.name
FROM table1 t1
CROSS APPLY (
  SELECT name FROM table2 t2 WHERE t2.ref_id = t1.id
) t2;

-- OUTER APPLY (preserves rows even if subquery returns nothing)
SELECT t1.id, t2.name
FROM table1 t1
OUTER APPLY (
  SELECT name FROM table2 t2 WHERE t2.ref_id = t1.id
) t2;
```

**Expected Behavior:** Should extract `table1.id`, `table2.name` and correctly resolve the correlation.

---

### 2. Window Functions with PARTITION BY
**Why Critical:** Extremely common in analytics queries. Window functions reference columns from the same query.

**Test Cases Needed:**
```sql
-- Basic window function
SELECT id, name, 
       ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) as rn
FROM employees;

-- Window function referencing multiple columns
SELECT id, dept_id, salary,
       SUM(salary) OVER (PARTITION BY dept_id) as dept_total,
       AVG(salary) OVER (PARTITION BY dept_id, location_id) as avg_by_dept_loc
FROM employees;

-- Window function in WHERE (PostgreSQL, MySQL 8.0+)
SELECT id, name, salary
FROM (
  SELECT id, name, salary,
         ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn
  FROM employees
) ranked
WHERE rn = 1;
```

**Expected Behavior:** Should extract all referenced columns (`employees.id`, `employees.name`, `employees.dept_id`, `employees.salary`, `employees.location_id`).

---

### 3. Recursive CTEs
**Why Critical:** Common for hierarchical data. Recursive CTEs have special scoping rules.

**Test Cases Needed:**
```sql
-- Basic recursive CTE
WITH RECURSIVE org_tree AS (
  SELECT id, name, parent_id, 1 as level
  FROM organizations
  WHERE parent_id IS NULL
  UNION ALL
  SELECT o.id, o.name, o.parent_id, ot.level + 1
  FROM organizations o
  JOIN org_tree ot ON o.parent_id = ot.id
)
SELECT ot.id, ot.name, e.name as employee_name
FROM org_tree ot
JOIN employees e ON e.org_id = ot.id;

-- Recursive CTE with multiple references
WITH RECURSIVE paths AS (
  SELECT start_node, end_node, path, 1 as depth
  FROM edges
  WHERE start_node = 'A'
  UNION ALL
  SELECT p.start_node, e.end_node, p.path || '->' || e.end_node, p.depth + 1
  FROM paths p
  JOIN edges e ON p.end_node = e.start_node
  WHERE p.depth < 10
)
SELECT p.start_node, p.end_node, n.name
FROM paths p
JOIN nodes n ON n.id = p.end_node;
```

**Expected Behavior:** Should correctly resolve columns from both the base case and recursive part, handling the self-reference to the CTE name.

---

### 4. Ambiguous Column Resolution
**Why Critical:** When multiple tables have the same column name, resolution logic is critical for correctness.

**Test Cases Needed:**
```sql
-- Ambiguous column (should detect or resolve)
SELECT id FROM customers, orders;  -- Which id?

-- Resolved with table prefix
SELECT customers.id, orders.id FROM customers, orders;

-- Ambiguous in WHERE
SELECT name FROM customers c, products p WHERE c.id = p.customer_id;
-- 'name' exists in both tables - which one?

-- Resolved with alias
SELECT c.name, p.name FROM customers c, products p WHERE c.id = p.customer_id;
```

**Expected Behavior:** Should either:
- Resolve ambiguity based on context (JOIN conditions, WHERE clauses)
- Report ambiguity as an error/warning
- Extract both possible columns

---

### 5. Three and Four-Part Names
**Why Critical:** Common in enterprise SQL Server environments with multiple databases/servers.

**Test Cases Needed:**
```sql
-- Three-part name (database.schema.table)
SELECT id, name FROM MyDB.dbo.customers;

-- Four-part name (server.database.schema.table)
SELECT id, name FROM Server1.MyDB.dbo.customers;

-- With aliases
SELECT c.id, c.name 
FROM Server1.MyDB.dbo.customers c;

-- Mixed with two-part names
SELECT c.id, o.order_id
FROM MyDB.dbo.customers c
JOIN orders o ON c.id = o.customer_id;
```

**Expected Behavior:** Should extract fully qualified names: `MyDB.dbo.customers.id`, `Server1.MyDB.dbo.customers.name`, etc.

---

## 🟡 High Priority (Should Test Soon)

### 6. PIVOT and UNPIVOT
**Why Important:** Common transformation pattern, especially in SQL Server.

**Test Cases:**
```sql
-- PIVOT
SELECT * FROM (
  SELECT dept_id, employee_id, salary
  FROM employees
) src
PIVOT (SUM(salary) FOR dept_id IN ([1], [2], [3])) pvt;

-- UNPIVOT
SELECT dept_id, metric_name, metric_value
FROM metrics
UNPIVOT (metric_value FOR metric_name IN (revenue, profit, cost)) unpvt;
```

---

### 7. Table-Valued Functions
**Why Important:** Common in SQL Server and PostgreSQL.

**Test Cases:**
```sql
-- SQL Server table-valued function
SELECT t.id, t.name
FROM dbo.fn_GetEmployees(@dept_id) t;

-- PostgreSQL table-valued function
SELECT t.id, t.name
FROM get_employees(123) t;

-- With JOINs
SELECT t.id, d.name
FROM dbo.fn_GetEmployees(@dept_id) t
JOIN departments d ON t.dept_id = d.id;
```

---

### 8. Correlated Subqueries
**Why Important:** Very common pattern, especially in WHERE and SELECT clauses.

**Test Cases:**
```sql
-- Correlated subquery in SELECT
SELECT id, name,
       (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as order_count
FROM customers c;

-- Correlated subquery in WHERE
SELECT id, name
FROM customers c
WHERE EXISTS (
  SELECT 1 FROM orders o 
  WHERE o.customer_id = c.id AND o.total > 1000
);

-- Multiple levels of correlation
SELECT id, name
FROM customers c
WHERE EXISTS (
  SELECT 1 FROM orders o
  WHERE o.customer_id = c.id
  AND EXISTS (
    SELECT 1 FROM order_items oi
    WHERE oi.order_id = o.id AND oi.product_id = 123
  )
);
```

---

### 9. QUALIFY Clause (Snowflake/BigQuery)
**Why Important:** Growing in popularity, cleaner than subqueries for window function filtering.

**Test Cases:**
```sql
-- Snowflake QUALIFY
SELECT id, name, salary, dept_id
FROM employees
QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) = 1;

-- With multiple window functions
SELECT id, name, salary, dept_id,
       ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rn,
       RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rank
FROM employees
QUALIFY rn = 1;
```

---

### 10. Deep Nesting (3+ Levels)
**Why Important:** Tests robustness of recursive traversal and alias resolution.

**Test Cases:**
```sql
-- Deeply nested subqueries
SELECT id FROM customers
WHERE id IN (
  SELECT customer_id FROM orders
  WHERE order_id IN (
    SELECT order_id FROM order_items
    WHERE product_id IN (
      SELECT id FROM products
      WHERE category_id IN (
        SELECT id FROM categories WHERE name = 'Electronics'
      )
    )
  )
);

-- Deeply nested CTEs
WITH level1 AS (
  SELECT id FROM table1
),
level2 AS (
  SELECT id FROM level1
),
level3 AS (
  SELECT id FROM level2
),
level4 AS (
  SELECT id FROM level3
)
SELECT id FROM level4;
```

---

## 🟢 Medium Priority (Nice to Have)

### 11. GROUP BY Advanced Features
- GROUP BY ROLLUP
- GROUP BY CUBE
- GROUP BY GROUPING SETS

### 12. Set Operations
- INTERSECT
- EXCEPT
- Multiple UNIONs with different structures

### 13. JSON/XML Functions
- JSON_TABLE (MySQL, PostgreSQL)
- JSON operators (PostgreSQL: `->`, `->>`)
- XML table functions (SQL Server)

### 14. Dialect-Specific Features
- PostgreSQL arrays: `tags[1]`
- SQL Server table hints: `WITH (NOLOCK)`
- MySQL backticks and variables
- Oracle ROWNUM and hierarchical queries

---

## Testing Strategy

For each critical edge case:

1. **Create test SQL file** with the edge case
2. **Run extractor** and verify output
3. **Check alias resolution** - ensure aliases are correctly mapped
4. **Check column qualification** - ensure columns are properly qualified
5. **Test with variations**:
   - With CTEs
   - With subqueries
   - With multiple JOINs
   - With aliases
   - Without aliases

## Expected Output Format

For each test case, document:
- **Input SQL**: The test SQL query
- **Expected Columns**: List of expected `table.column` references
- **Notes**: Any special considerations or edge case behaviors

Example:
```markdown
### Test: LATERAL JOIN Basic

**Input SQL:**
```sql
SELECT t1.id, t2.name
FROM table1 t1
CROSS JOIN LATERAL (
  SELECT name FROM table2 t2 WHERE t2.ref_id = t1.id
) t2;
```

**Expected Output:**
- table1.id
- table2.name
- table2.ref_id (from WHERE clause)

**Notes:** 
- LATERAL join allows t2 to reference t1.id
- Should correctly resolve correlation
```

---

## Next Steps

1. Create test SQL files for Critical Priority cases (#1-5)
2. Run extractor on each test file
3. Verify output matches expected results
4. Document any discrepancies or bugs found
5. Fix any issues in the extractor code
6. Move to High Priority cases (#6-10)
7. Continue with Medium Priority as time permits

