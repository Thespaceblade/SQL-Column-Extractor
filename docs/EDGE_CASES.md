# SQL Column Extraction - Missing Edge Cases

This document identifies edge cases that should be tested but may currently be missing from the test suite.

## 1. Advanced JOIN Types

### Currently Missing:
- **LATERAL JOINs** (PostgreSQL, MySQL 8.0+)
  ```sql
  SELECT t1.id, t2.name
  FROM table1 t1
  CROSS JOIN LATERAL (
    SELECT name FROM table2 t2 WHERE t2.ref_id = t1.id
  ) t2;
  ```

- **CROSS APPLY / OUTER APPLY** (SQL Server, Oracle)
  ```sql
  SELECT t1.id, t2.name
  FROM table1 t1
  CROSS APPLY (
    SELECT name FROM table2 t2 WHERE t2.ref_id = t1.id
  ) t2;
  ```

- **NATURAL JOINs**
  ```sql
  SELECT id, name FROM table1 NATURAL JOIN table2;
  ```

- **USING clause** (alternative to ON)
  ```sql
  SELECT t1.id, t2.name
  FROM table1 t1
  JOIN table2 t2 USING (id);
  ```

## 2. Window Functions and Advanced Analytics

### Currently Missing:
- **Window functions with PARTITION BY**
  ```sql
  SELECT id, name, 
         ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY id) as rn
  FROM employees;
  ```

- **Window functions with multiple columns**
  ```sql
  SELECT id, 
         SUM(salary) OVER (PARTITION BY dept_id, location_id) as dept_total
  FROM employees;
  ```

- **Named window definitions**
  ```sql
  SELECT id, 
         ROW_NUMBER() OVER w1,
         RANK() OVER w1
  FROM employees
  WINDOW w1 AS (PARTITION BY dept_id ORDER BY salary);
  ```

- **QUALIFY clause** (Snowflake, BigQuery)
  ```sql
  SELECT id, name, salary
  FROM employees
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) = 1;
  ```

## 3. Table-Valued Functions

### Currently Missing:
- **Table-valued functions** (SQL Server, PostgreSQL)
  ```sql
  SELECT t.id, t.name
  FROM dbo.fn_GetEmployees(@dept_id) t;
  ```

- **JSON table functions** (PostgreSQL, MySQL, SQL Server)
  ```sql
  SELECT j.id, j.name
  FROM employees e,
       JSON_TABLE(e.json_data, '$[*]' COLUMNS (id INT, name VARCHAR(50))) j;
  ```

- **XML table functions**
  ```sql
  SELECT x.id, x.name
  FROM employees e
  CROSS APPLY e.xml_data.nodes('/employees/employee') AS x(id, name);
  ```

## 4. PIVOT and UNPIVOT

### Currently Missing:
- **PIVOT operations**
  ```sql
  SELECT * FROM (
    SELECT dept_id, employee_id, salary
    FROM employees
  ) src
  PIVOT (SUM(salary) FOR dept_id IN ([1], [2], [3])) pvt;
  ```

- **UNPIVOT operations**
  ```sql
  SELECT dept_id, metric_name, metric_value
  FROM metrics
  UNPIVOT (metric_value FOR metric_name IN (revenue, profit, cost)) unpvt;
  ```

## 5. Recursive CTEs

### Currently Missing:
- **Recursive CTEs with UNION ALL**
  ```sql
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
  ```

- **Recursive CTEs with multiple base cases**
  ```sql
  WITH RECURSIVE paths AS (
    SELECT start_node, end_node, path
    FROM edges
    WHERE start_node = 'A'
    UNION ALL
    SELECT p.start_node, e.end_node, p.path || '->' || e.end_node
    FROM paths p
    JOIN edges e ON p.end_node = e.start_node
  )
  SELECT p.start_node, p.end_node, n.name
  FROM paths p
  JOIN nodes n ON n.id = p.end_node;
  ```

## 6. Correlated Subqueries

### Currently Missing:
- **Correlated subqueries in SELECT**
  ```sql
  SELECT id, name,
         (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as order_count
  FROM customers c;
  ```

- **Correlated subqueries in WHERE**
  ```sql
  SELECT id, name
  FROM customers c
  WHERE EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.customer_id = c.id AND o.total > 1000
  );
  ```

- **Multiple levels of correlation**
  ```sql
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

## 7. Derived Tables and Inline Views

### Currently Missing:
- **Derived tables with complex expressions**
  ```sql
  SELECT dt.id, dt.total
  FROM (
    SELECT customer_id as id, SUM(amount) as total
    FROM orders
    GROUP BY customer_id
    HAVING SUM(amount) > 1000
  ) dt;
  ```

- **Multiple derived tables**
  ```sql
  SELECT a.id, b.name
  FROM (SELECT id FROM table1 WHERE status = 'A') a
  JOIN (SELECT id, name FROM table2 WHERE active = 1) b ON a.id = b.id;
  ```

## 8. Set Operations

### Currently Missing:
- **INTERSECT operations**
  ```sql
  SELECT id, name FROM table1
  INTERSECT
  SELECT id, name FROM table2;
  ```

- **EXCEPT operations**
  ```sql
  SELECT id, name FROM table1
  EXCEPT
  SELECT id, name FROM table2;
  ```

- **Multiple UNIONs with different column counts** (should fail gracefully)
  ```sql
  SELECT id FROM table1
  UNION ALL
  SELECT id, name FROM table2;  -- Column count mismatch
  ```

## 9. Schema/Database Qualification

### Currently Missing:
- **Three-part names** (SQL Server: database.schema.table)
  ```sql
  SELECT id, name FROM MyDB.dbo.customers;
  ```

- **Four-part names** (SQL Server: server.database.schema.table)
  ```sql
  SELECT id, name FROM Server1.MyDB.dbo.customers;
  ```

- **Quoted identifiers** (case-sensitive, special characters)
  ```sql
  SELECT "Id", "Name" FROM "My Schema"."My Table";
  SELECT [Id], [Name] FROM [My Schema].[My Table];
  SELECT `Id`, `Name` FROM `My Schema`.`My Table`;
  ```

- **Mixed case identifiers**
  ```sql
  SELECT CustomerID, FirstName FROM Customers;
  SELECT customerid, firstname FROM customers;  -- Case-insensitive resolution
  ```

## 10. Column Aliases and Expressions

### Currently Missing:
- **Column aliases in SELECT**
  ```sql
  SELECT t1.id AS customer_id, t2.name AS product_name
  FROM customers t1
  JOIN products t2 ON t1.product_id = t2.id;
  ```

- **Expressions referencing other columns**
  ```sql
  SELECT id, name, 
         (SELECT COUNT(*) FROM orders WHERE customer_id = c.id) as order_count
  FROM customers c;
  ```

- **Aggregate expressions**
  ```sql
  SELECT dept_id, COUNT(*) as emp_count, SUM(salary) as total_salary
  FROM employees
  GROUP BY dept_id;
  ```

## 11. WHERE Clause Edge Cases

### Currently Missing:
- **Subqueries in WHERE**
  ```sql
  SELECT id, name FROM customers
  WHERE id IN (SELECT customer_id FROM orders WHERE total > 1000);
  ```

- **EXISTS subqueries**
  ```sql
  SELECT id, name FROM customers c
  WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);
  ```

- **Multiple conditions with OR**
  ```sql
  SELECT id, name FROM customers
  WHERE status = 'A' OR status = 'B' OR id IN (SELECT id FROM vip_customers);
  ```

## 12. HAVING Clause

### Currently Missing:
- **HAVING with aggregates**
  ```sql
  SELECT dept_id, COUNT(*) as emp_count
  FROM employees
  GROUP BY dept_id
  HAVING COUNT(*) > 10;
  ```

- **HAVING with subqueries**
  ```sql
  SELECT dept_id, AVG(salary) as avg_salary
  FROM employees
  GROUP BY dept_id
  HAVING AVG(salary) > (SELECT AVG(salary) FROM employees);
  ```

## 13. ORDER BY and LIMIT/OFFSET

### Currently Missing:
- **ORDER BY with expressions**
  ```sql
  SELECT id, name FROM customers
  ORDER BY (SELECT COUNT(*) FROM orders WHERE customer_id = customers.id);
  ```

- **LIMIT/OFFSET** (should not extract columns, but should parse correctly)
  ```sql
  SELECT id, name FROM customers
  ORDER BY id
  LIMIT 10 OFFSET 20;
  ```

## 14. GROUP BY Edge Cases

### Currently Missing:
- **GROUP BY with expressions**
  ```sql
  SELECT YEAR(order_date) as year, COUNT(*) as order_count
  FROM orders
  GROUP BY YEAR(order_date);
  ```

- **GROUP BY ROLLUP**
  ```sql
  SELECT dept_id, location_id, SUM(salary)
  FROM employees
  GROUP BY ROLLUP(dept_id, location_id);
  ```

- **GROUP BY CUBE**
  ```sql
  SELECT dept_id, location_id, SUM(salary)
  FROM employees
  GROUP BY CUBE(dept_id, location_id);
  ```

- **GROUP BY GROUPING SETS**
  ```sql
  SELECT dept_id, location_id, SUM(salary)
  FROM employees
  GROUP BY GROUPING SETS ((dept_id), (location_id), ());
  ```

## 15. CASE Expressions

### Currently Missing:
- **CASE expressions with subqueries**
  ```sql
  SELECT id, name,
         CASE 
           WHEN (SELECT COUNT(*) FROM orders WHERE customer_id = c.id) > 10 
           THEN 'VIP'
           ELSE 'Regular'
         END as customer_type
  FROM customers c;
  ```

- **Searched CASE expressions**
  ```sql
  SELECT id, name,
         CASE 
           WHEN status = 'A' THEN (SELECT name FROM statuses WHERE id = 1)
           WHEN status = 'B' THEN (SELECT name FROM statuses WHERE id = 2)
           ELSE 'Unknown'
         END as status_name
  FROM customers;
  ```

## 16. NULL Handling

### Currently Missing:
- **IS NULL / IS NOT NULL**
  ```sql
  SELECT id, name FROM customers
  WHERE email IS NULL OR phone IS NOT NULL;
  ```

- **COALESCE / NULLIF**
  ```sql
  SELECT id, COALESCE(phone, email) as contact
  FROM customers;
  ```

## 17. String Functions and Expressions

### Currently Missing:
- **String concatenation**
  ```sql
  SELECT id, first_name || ' ' || last_name as full_name
  FROM employees;
  ```

- **String functions with table references**
  ```sql
  SELECT id, SUBSTRING(name, 1, 10) as short_name
  FROM customers;
  ```

## 18. Date/Time Functions

### Currently Missing:
- **Date functions**
  ```sql
  SELECT id, DATE_TRUNC('month', order_date) as order_month
  FROM orders;
  ```

- **Date arithmetic**
  ```sql
  SELECT id, order_date + INTERVAL '30 days' as due_date
  FROM orders;
  ```

## 19. Aggregate Functions

### Currently Missing:
- **DISTINCT in aggregates**
  ```sql
  SELECT dept_id, COUNT(DISTINCT employee_id) as unique_employees
  FROM employees
  GROUP BY dept_id;
  ```

- **Filtered aggregates** (PostgreSQL)
  ```sql
  SELECT dept_id,
         COUNT(*) FILTER (WHERE salary > 50000) as high_earners
  FROM employees
  GROUP BY dept_id;
  ```

## 20. Multiple Statements

### Currently Missing:
- **Multiple SELECT statements**
  ```sql
  SELECT id, name FROM customers;
  SELECT id, product_name FROM products;
  ```

- **Mixed statement types** (should skip non-SELECT)
  ```sql
  INSERT INTO customers (id, name) VALUES (1, 'Test');
  SELECT id, name FROM customers;
  UPDATE customers SET name = 'Updated' WHERE id = 1;
  ```

## 21. Comments and Formatting

### Currently Missing:
- **Multi-line comments**
  ```sql
  /* This is a comment
     spanning multiple lines */
  SELECT id, name FROM customers;
  ```

- **Inline comments**
  ```sql
  SELECT id, -- inline comment
         name FROM customers;
  ```

- **Comments in expressions**
  ```sql
  SELECT id, /* comment */ name FROM customers;
  ```

## 22. Special Characters and Unicode

### Currently Missing:
- **Unicode identifiers**
  ```sql
  SELECT 客户ID, 客户名称 FROM 客户表;
  ```

- **Special characters in identifiers**
  ```sql
  SELECT "column-name", "table.name" FROM "schema-table";
  ```

- **Emojis in identifiers** (should handle gracefully)
  ```sql
  SELECT id, name FROM customers_😀;
  ```

## 23. Dynamic SQL

### Currently Missing:
- **EXEC/EXECUTE statements** (should skip or handle)
  ```sql
  EXEC('SELECT id, name FROM customers');
  ```

- **Prepared statements**
  ```sql
  PREPARE stmt FROM 'SELECT id, name FROM customers WHERE id = ?';
  EXECUTE stmt USING @id;
  ```

## 24. Views and Materialized Views

### Currently Missing:
- **Views in FROM clause**
  ```sql
  SELECT v.id, v.name
  FROM customer_view v;
  ```

- **Materialized views**
  ```sql
  SELECT mv.id, mv.aggregated_value
  FROM materialized_customer_stats mv;
  ```

## 25. Table Aliases Edge Cases

### Currently Missing:
- **Same alias used multiple times** (should handle gracefully)
  ```sql
  SELECT t1.id, t2.id
  FROM customers t1
  JOIN orders t1 ON t1.id = t1.customer_id;  -- Ambiguous alias
  ```

- **Alias same as table name**
  ```sql
  SELECT customers.id, customers.name
  FROM customers customers;
  ```

- **No alias on subquery**
  ```sql
  SELECT id, name FROM (SELECT id, name FROM customers);
  ```

## 26. Column Resolution Edge Cases

### Currently Missing:
- **Ambiguous column names** (multiple tables with same column)
  ```sql
  SELECT id FROM customers, orders;  -- Ambiguous: which id?
  ```

- **Columns that exist in multiple tables**
  ```sql
  SELECT name FROM customers c, products p WHERE c.id = p.customer_id;
  -- Should resolve 'name' based on context or report ambiguity
  ```

- **Star expansion** (table.*)
  ```sql
  SELECT customers.*, orders.order_id
  FROM customers
  JOIN orders ON customers.id = orders.customer_id;
  ```

## 27. Nested Subqueries

### Currently Missing:
- **Deeply nested subqueries** (3+ levels)
  ```sql
  SELECT id FROM customers
  WHERE id IN (
    SELECT customer_id FROM orders
    WHERE order_id IN (
      SELECT order_id FROM order_items
      WHERE product_id IN (
        SELECT id FROM products WHERE category = 'Electronics'
      )
    )
  );
  ```

## 28. Dialect-Specific Features

### Currently Missing:
- **PostgreSQL-specific**
  - Array columns: `SELECT id, tags[1] FROM products;`
  - JSON operators: `SELECT id, data->>'name' FROM customers;`
  - Range types: `SELECT id, date_range FROM schedules;`

- **SQL Server-specific**
  - Table hints: `SELECT * FROM customers WITH (NOLOCK);`
  - TOP with TIES: `SELECT TOP 10 WITH TIES id, name FROM customers ORDER BY score;`

- **MySQL-specific**
  - Backticks: `` SELECT `id`, `name` FROM `customers`; ``
  - Variables: `SELECT @var := id FROM customers;`

- **Oracle-specific**
  - ROWNUM: `SELECT * FROM customers WHERE ROWNUM <= 10;`
  - Hierarchical queries: `SELECT * FROM employees CONNECT BY PRIOR id = manager_id;`

- **Snowflake-specific**
  - QUALIFY clause
  - Lateral flatten: `SELECT id, value FROM customers, LATERAL FLATTEN(json_data);`

## 29. Error Handling Edge Cases

### Currently Missing:
- **Malformed SQL** (should handle gracefully)
  ```sql
  SELECT id, name FROM customers WHERE;  -- Missing condition
  ```

- **Incomplete statements**
  ```sql
  SELECT id, name FROM customers
  -- Missing semicolon, incomplete
  ```

- **Unclosed quotes**
  ```sql
  SELECT id, name FROM customers WHERE name = 'unclosed;
  ```

- **Unclosed parentheses**
  ```sql
  SELECT id, name FROM customers WHERE id IN (SELECT id FROM orders;
  ```

## 30. Performance and Scale Edge Cases

### Currently Missing:
- **Very large queries** (1000+ columns)
- **Very deep nesting** (20+ levels)
- **Many CTEs** (50+ CTEs)
- **Many JOINs** (100+ JOINs)

## Testing Recommendations

For each edge case above, create test SQL files that:

1. **Test the basic case** - Simple example
2. **Test with aliases** - Same case with table aliases
3. **Test with CTEs** - Same case wrapped in CTEs
4. **Test with subqueries** - Same case in subquery context
5. **Test error handling** - Malformed versions should fail gracefully
6. **Test multiple dialects** - Where applicable, test across different SQL dialects

## Priority Edge Cases

High priority edge cases to test first:

1. **LATERAL JOINs / CROSS APPLY** - Common in modern SQL
2. **Window functions** - Very common in analytics
3. **Recursive CTEs** - Complex but important
4. **PIVOT/UNPIVOT** - Common transformation pattern
5. **Table-valued functions** - Common in SQL Server/PostgreSQL
6. **Three/four-part names** - Important for multi-database scenarios
7. **Ambiguous column resolution** - Critical for correctness
8. **Deep nesting** - Tests robustness of recursive traversal

