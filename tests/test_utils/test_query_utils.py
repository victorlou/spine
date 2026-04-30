"""Tests for ``validate_query_content`` (SQL allowlist / guard behavior)."""

from src.utils.query_utils import validate_query_content


class TestValidateQueryContent:
    """Test suite for validate_query_content function."""

    # Valid SELECT queries
    def test_simple_select_query(self):
        """Test a simple SELECT query."""
        assert validate_query_content("SELECT * FROM users") is True

    def test_select_with_where_clause(self):
        """Test SELECT with WHERE clause."""
        assert validate_query_content("SELECT id, name FROM users WHERE id = 1") is True

    def test_select_with_join(self):
        """Test SELECT with JOIN."""
        assert (
            validate_query_content(
                "SELECT u.id, u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id"
            )
            is True
        )

    def test_select_with_group_by(self):
        """Test SELECT with GROUP BY."""
        assert (
            validate_query_content("SELECT department, COUNT(*) FROM employees GROUP BY department")
            is True
        )

    def test_select_with_order_by(self):
        """Test SELECT with ORDER BY."""
        assert validate_query_content("SELECT * FROM products ORDER BY price DESC") is True

    def test_select_with_limit(self):
        """Test SELECT with LIMIT."""
        assert validate_query_content("SELECT * FROM users LIMIT 10") is True

    def test_select_with_subquery(self):
        """Test SELECT with subquery."""
        assert (
            validate_query_content("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")
            is True
        )

    def test_select_with_aggregate_functions(self):
        """Test SELECT with aggregate functions."""
        assert (
            validate_query_content("SELECT COUNT(*), SUM(amount), AVG(price) FROM transactions")
            is True
        )

    def test_select_with_case_statement(self):
        """Test SELECT with CASE statement."""
        assert (
            validate_query_content(
                "SELECT id, CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM users"
            )
            is True
        )

    def test_select_with_trailing_semicolon(self):
        """Test SELECT with trailing semicolon."""
        assert validate_query_content("SELECT * FROM users;") is True

    def test_select_with_whitespace(self):
        """Test SELECT with leading/trailing whitespace."""
        assert validate_query_content("  SELECT * FROM users  ") is True

    def test_select_with_multiple_spaces(self):
        """Test SELECT with multiple spaces between keywords."""
        assert validate_query_content("SELECT    *    FROM    users") is True

    def test_select_with_distinct(self):
        """Test SELECT DISTINCT."""
        assert validate_query_content("SELECT DISTINCT country FROM users") is True

    def test_select_with_having(self):
        """Test SELECT with HAVING clause."""
        assert (
            validate_query_content(
                "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5"
            )
            is True
        )

    # Invalid queries - Dangerous operations
    def test_delete_query(self):
        """Test that DELETE queries are rejected."""
        assert validate_query_content("DELETE FROM users WHERE id = 1") is False

    def test_drop_query(self):
        """Test that DROP queries are rejected."""
        assert validate_query_content("DROP TABLE users") is False

    def test_truncate_query(self):
        """Test that TRUNCATE queries are rejected."""
        assert validate_query_content("TRUNCATE TABLE users") is False

    def test_insert_query(self):
        """Test that INSERT queries are rejected."""
        assert validate_query_content("INSERT INTO users (name) VALUES ('John')") is False

    def test_update_query(self):
        """Test that UPDATE queries are rejected."""
        assert validate_query_content("UPDATE users SET name = 'Jane' WHERE id = 1") is False

    def test_create_query(self):
        """Test that CREATE queries are rejected."""
        assert validate_query_content("CREATE TABLE users (id INT, name VARCHAR(100))") is False

    def test_alter_query(self):
        """Test that ALTER queries are rejected."""
        assert validate_query_content("ALTER TABLE users ADD COLUMN email VARCHAR(100)") is False

    def test_merge_query(self):
        """Test that MERGE queries are rejected."""
        assert (
            validate_query_content("MERGE INTO users USING source ON users.id = source.id") is False
        )

    def test_grant_query(self):
        """Test that GRANT queries are rejected."""
        assert validate_query_content("GRANT SELECT ON users TO user1") is False

    def test_revoke_query(self):
        """Test that REVOKE queries are rejected."""
        assert validate_query_content("REVOKE SELECT ON users FROM user1") is False

    def test_call_query(self):
        """Test that CALL queries are rejected."""
        assert validate_query_content("CALL stored_procedure()") is False

    def test_execute_query(self):
        """Test that EXECUTE queries are rejected."""
        assert validate_query_content("EXECUTE sp_executesql") is False

    # Stacked query and comment injection attempts
    def test_sql_injection_comment(self):
        """Test SQL injection with stacked query and comment."""
        assert validate_query_content("SELECT * FROM users WHERE id = 1; --") is False

    def test_sql_injection_double_dash_comment(self):
        """Test that comments are blocked anywhere in query."""
        assert validate_query_content("SELECT * FROM users -- DROP TABLE users") is False

    def test_sql_injection_block_comment(self):
        """Test that block comments are blocked."""
        assert validate_query_content("SELECT * FROM users /* DROP TABLE users */") is False

    def test_sql_injection_union(self):
        """Test that UNION is blocked (data exfiltration prevention)."""
        assert validate_query_content("SELECT * FROM users UNION SELECT * FROM admin") is False

    def test_sql_injection_multiple_statements(self):
        """Test that multiple statements are blocked."""
        assert validate_query_content("SELECT * FROM users; DELETE FROM users") is False

    def test_sql_injection_intersect(self):
        """Test that INTERSECT is blocked (data exfiltration prevention)."""
        assert validate_query_content("SELECT * FROM users INTERSECT SELECT * FROM admin") is False

    def test_sql_injection_except(self):
        """Test that EXCEPT is blocked (data exfiltration prevention)."""
        assert (
            validate_query_content("SELECT * FROM users EXCEPT SELECT * FROM public_users") is False
        )

    # Valid queries with OR operator (no longer blocked)
    def test_legitimate_or_in_where(self):
        """Test that legitimate OR operator in WHERE clause is allowed."""
        assert validate_query_content("SELECT * FROM users WHERE id = 1 OR id = 2") is True

    def test_legitimate_or_with_conditions(self):
        """Test OR with complex conditions is allowed."""
        assert (
            validate_query_content(
                "SELECT * FROM users WHERE status = 'active' OR status = 'pending'"
            )
            is True
        )

    def test_legitimate_empty_string_or(self):
        """Test that empty string comparisons with OR are allowed."""
        assert validate_query_content("SELECT * FROM users WHERE name = '' OR name IS NULL") is True

    # Edge cases
    def test_empty_string(self):
        """Test empty string."""
        assert validate_query_content("") is False

    def test_whitespace_only(self):
        """Test whitespace only."""
        assert validate_query_content("   ") is False

    def test_none_input(self):
        """Test None input."""
        assert validate_query_content(None) is False

    def test_non_string_input(self):
        """Test non-string input."""
        assert validate_query_content(123) is False

    def test_query_without_select(self):
        """Test query without SELECT keyword."""
        assert validate_query_content("FROM users") is False

    def test_select_lowercase(self):
        """Test SELECT in lowercase."""
        assert validate_query_content("select * from users") is True

    def test_select_mixed_case(self):
        """Test SELECT in mixed case."""
        assert validate_query_content("SeLeCt * FrOm users") is True

    def test_begin_transaction(self):
        """Test BEGIN transaction."""
        assert validate_query_content("BEGIN; SELECT * FROM users") is False

    def test_commit_transaction(self):
        """Test COMMIT transaction."""
        assert validate_query_content("SELECT * FROM users; COMMIT") is False

    def test_rollback_transaction(self):
        """Test ROLLBACK transaction."""
        assert validate_query_content("SELECT * FROM users; ROLLBACK") is False

    def test_vacuum_operation(self):
        """Test VACUUM operation."""
        assert validate_query_content("VACUUM; SELECT * FROM users") is False

    def test_analyze_operation(self):
        """Test ANALYZE operation."""
        assert validate_query_content("ANALYZE TABLE users") is False

    def test_pragma_operation(self):
        """Test PRAGMA operation."""
        assert validate_query_content("PRAGMA table_info(users)") is False

    def test_select_with_complex_where(self):
        """Test SELECT with complex WHERE clause."""
        assert (
            validate_query_content(
                "SELECT * FROM users WHERE (age > 18 AND status = 'active') OR (role = 'admin')"
            )
            is True
        )

    def test_select_with_cast(self):
        """Test SELECT with CAST."""
        assert validate_query_content("SELECT CAST(id AS VARCHAR) FROM users") is True

    def test_select_with_string_concatenation(self):
        """Test SELECT with string concatenation."""
        assert (
            validate_query_content("SELECT CONCAT(first_name, ' ', last_name) FROM users") is True
        )

    def test_select_with_date_functions(self):
        """Test SELECT with date functions."""
        assert (
            validate_query_content(
                "SELECT * FROM users WHERE created_date > DATE_SUB(NOW(), INTERVAL 30 DAY)"
            )
            is True
        )

    def test_select_with_window_functions(self):
        """Test SELECT with window functions."""
        assert (
            validate_query_content("SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM users") is True
        )

    def test_select_with_cte(self):
        """Test SELECT with Common Table Expression (CTE)."""
        assert (
            validate_query_content(
                "WITH user_orders AS (SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id) SELECT * FROM user_orders"
            )
            is True
        )

    def test_select_with_multiple_joins(self):
        """Test SELECT with multiple JOINs."""
        assert (
            validate_query_content(
                "SELECT u.id, u.name, o.order_id, p.product_name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id"
            )
            is True
        )

    def test_select_with_left_join(self):
        """Test SELECT with LEFT JOIN."""
        assert (
            validate_query_content(
                "SELECT u.id, u.name, o.order_id FROM users u LEFT JOIN orders o ON u.id = o.user_id"
            )
            is True
        )

    def test_select_with_inner_join(self):
        """Test SELECT with INNER JOIN."""
        assert (
            validate_query_content(
                "SELECT u.id, u.name, o.order_id FROM users u INNER JOIN orders o ON u.id = o.user_id"
            )
            is True
        )

    def test_select_with_cross_join(self):
        """Test SELECT with CROSS JOIN."""
        assert (
            validate_query_content("SELECT u.id, p.product_id FROM users u CROSS JOIN products p")
            is True
        )

    def test_select_with_offset(self):
        """Test SELECT with OFFSET."""
        assert validate_query_content("SELECT * FROM users LIMIT 10 OFFSET 20") is True

    def test_select_with_between(self):
        """Test SELECT with BETWEEN."""
        assert validate_query_content("SELECT * FROM users WHERE age BETWEEN 18 AND 65") is True

    def test_select_with_in_clause(self):
        """Test SELECT with IN clause."""
        assert validate_query_content("SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)") is True

    def test_select_with_like(self):
        """Test SELECT with LIKE."""
        assert validate_query_content("SELECT * FROM users WHERE name LIKE '%John%'") is True

    def test_select_with_is_null(self):
        """Test SELECT with IS NULL."""
        assert validate_query_content("SELECT * FROM users WHERE email IS NULL") is True

    def test_select_with_is_not_null(self):
        """Test SELECT with IS NOT NULL."""
        assert validate_query_content("SELECT * FROM users WHERE email IS NOT NULL") is True

    def test_select_with_exists(self):
        """Test SELECT with EXISTS."""
        assert (
            validate_query_content(
                "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
            )
            is True
        )

    def test_select_with_not_exists(self):
        """Test SELECT with NOT EXISTS."""
        assert (
            validate_query_content(
                "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
            )
            is True
        )

    def test_select_with_all_keyword(self):
        """Test SELECT with ALL keyword."""
        assert (
            validate_query_content(
                "SELECT * FROM users WHERE salary > ALL (SELECT salary FROM employees WHERE department = 'IT')"
            )
            is True
        )

    def test_select_with_any_keyword(self):
        """Test SELECT with ANY keyword."""
        assert (
            validate_query_content(
                "SELECT * FROM users WHERE salary > ANY (SELECT salary FROM employees WHERE department = 'IT')"
            )
            is True
        )

    def test_select_with_some_keyword(self):
        """Test SELECT with SOME keyword."""
        assert (
            validate_query_content(
                "SELECT * FROM users WHERE salary > SOME (SELECT salary FROM employees WHERE department = 'IT')"
            )
            is True
        )
