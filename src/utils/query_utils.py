import re


def validate_query_content(query_str: str) -> bool:
    """
    Validates the contents of a query string to ensure it is a safe SELECT statement for Spark SQL.

    Allowed Operations:
        - SELECT with all standard clauses: WHERE, JOIN, GROUP BY, ORDER BY, LIMIT, OFFSET, HAVING, etc.
        - WITH clause for Common Table Expressions (CTEs)
        - All logical operators: OR, AND, NOT in WHERE clauses
        - All standard Spark SQL SELECT features including subqueries

    Not Allowed Operations:
        - Data modification: INSERT, UPDATE, DELETE, REPLACE
        - Schema modification: CREATE, DROP, ALTER, TRUNCATE, RENAME
        - Set operations: UNION, INTERSECT, EXCEPT, MINUS (blocked to prevent data exfiltration)
        - Comments (-- or /* */) anywhere in the query
        - Multiple statements (stacked queries)

    Args:
        query_str (str): The query string to validate.

    Returns:
        bool: True if the query is valid and safe, False otherwise.
    """

    if not query_str or not isinstance(query_str, str):
        return False

    # Strip leading/trailing whitespace
    query_str = query_str.strip()

    if not query_str:
        return False

    # Normalize the query string to uppercase for comparison
    normalized_query = query_str.upper()

    # List of dangerous keywords that indicate non-SELECT operations or SQL injection attempts
    # Note: Removed Spark SQL irrelevant operations (transactions, stored procedures, DB-specific commands)
    dangerous_keywords = {
        # Data Modification
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
        # Schema Modification
        "CREATE",
        "DROP",
        "ALTER",
        "TRUNCATE",
        "RENAME",
        # Set Operations (blocked for injection prevention, though valid in Spark SQL)
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "MINUS",
    }

    # Check for dangerous keywords using word boundaries to avoid false positives
    # (e.g., "ALTER" should not match inside "INTERVAL")
    for keyword in dangerous_keywords:
        # Use word boundaries to match whole words only
        pattern = r"\b" + re.escape(keyword) + r"\b"
        if re.search(pattern, normalized_query):
            return False

    # Check that the query starts with SELECT or WITH (for CTEs)
    if not (normalized_query.startswith("SELECT") or normalized_query.startswith("WITH")):
        return False

    # Regex-based checks for stacked query attempts
    # Note: Tautology-based patterns (e.g., ' OR 1=1) are NOT needed here since we validate
    # complete SELECT statements, not user input being concatenated into WHERE clauses
    injection_patterns = [
        r"(?i);\s*--",  # ; -- (statement termination with line comment)
        r"(?i);\s*/\*",  # ; /* (statement termination with block comment)
    ]

    for pattern in injection_patterns:
        if re.search(pattern, query_str):
            return False

    # Ensure no multiple statements (check for semicolons not at the end)
    # Allow optional semicolon at the very end only
    query_without_trailing_semicolon = normalized_query.rstrip(";").strip()
    if ";" in query_without_trailing_semicolon:
        return False

    # Ensure no comment indicators anywhere in the query
    if "--" in query_str or "/*" in query_str or "*/" in query_str:
        return False

    return True


def format_query_ref_key(query_ref: str) -> str:
    """Format the query reference key for storage."""
    return f"databricks_query:{query_ref}"
