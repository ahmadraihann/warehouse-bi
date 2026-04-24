"""
load_source.py
Converts the inventory MySQL dump into a DuckDB database.
Run from the dbt_transformasi_dan_pemodelan_data/ directory:
    python load_source.py
"""
import duckdb
import re
import os
import sys


def convert_mysql_to_duckdb(content: str) -> str:
    # Remove database-level directives (not needed in DuckDB)
    content = re.sub(r'CREATE\s+DATABASE[^;]+;', '', content, flags=re.IGNORECASE)
    content = re.sub(r'USE\s+\w+\s*;', '', content, flags=re.IGNORECASE)

    # Remove block comments (/* ... */) — they have no semicolon terminator
    # and can merge with the next statement during splitting
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

    # Remove -- line comments
    content = re.sub(r'--[^\n]*', '', content)

    # Convert MySQL-specific data types
    # Note: no \b after closing ) because ) is not a word character
    content = re.sub(r'\bmediumtext\b', 'TEXT', content, flags=re.IGNORECASE)
    content = re.sub(r'\bmediumblob\b', 'BLOB', content, flags=re.IGNORECASE)
    content = re.sub(r'\blongtext\b', 'TEXT', content, flags=re.IGNORECASE)
    content = re.sub(r'\btinyint\(\d+\)', 'TINYINT', content, flags=re.IGNORECASE)
    content = re.sub(r'\bsmallint\(\d+\)', 'SMALLINT', content, flags=re.IGNORECASE)
    content = re.sub(r'\bmediumint\(\d+\)', 'INTEGER', content, flags=re.IGNORECASE)
    content = re.sub(r'\bbigint\(\d+\)', 'BIGINT', content, flags=re.IGNORECASE)
    content = re.sub(r'\bint\(\d+\)', 'INTEGER', content, flags=re.IGNORECASE)

    # Remove FOREIGN KEY constraint lines inside CREATE TABLE
    content = re.sub(
        r',\s*FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s+\w+\s*\([^)]+\)',
        '',
        content,
        flags=re.IGNORECASE
    )

    # Remove MySQL table options after the closing parenthesis
    content = re.sub(
        r'\)\s*ENGINE\s*=[^;]+;',
        ');',
        content,
        flags=re.IGNORECASE | re.DOTALL
    )

    # Remove LOCK TABLES and UNLOCK TABLES
    content = re.sub(r'LOCK TABLES[^;]+;', '', content, flags=re.IGNORECASE)
    content = re.sub(r'UNLOCK TABLES;', '', content, flags=re.IGNORECASE)

    # Remove MySQL backticks
    content = content.replace('`', '')

    # Remove AUTO_INCREMENT (not supported in DuckDB)
    content = re.sub(r'\bAUTO_INCREMENT\b', '', content, flags=re.IGNORECASE)

    # Remove KEY and UNIQUE KEY constraints (MySQL-specific indexes)
    content = re.sub(
        r',?\s*(UNIQUE\s+)?KEY\s+[^\(]+\([^\)]+\)',
        '',
        content,
        flags=re.IGNORECASE
    )

    return content


def split_sql_statements(content: str):
    """
    Split SQL content into individual statements on semicolons,
    but only when the semicolon is outside of single-quoted strings.
    Also converts MySQL backslash-escaped quotes (\') to standard SQL
    doubled quotes ('') so DuckDB can parse them correctly.
    Handles escaped backslashes (\\) inside strings.
    """
    statements = []
    current = []
    in_string = False
    i = 0
    n = len(content)

    while i < n:
        ch = content[i]

        if in_string:
            if ch == '\\' and i + 1 < n:
                next_ch = content[i + 1]
                if next_ch == "'":
                    # MySQL escaped quote \' → standard SQL ''
                    current.append("''")
                    i += 2
                    continue
                elif next_ch == '\\':
                    # Escaped backslash \\ → single backslash
                    current.append('\\')
                    i += 2
                    continue
                else:
                    # Other backslash escapes: pass through
                    current.append(ch)
                    current.append(next_ch)
                    i += 2
                    continue
            elif ch == "'":
                in_string = False
                current.append(ch)
            else:
                current.append(ch)
        else:
            if ch == "'":
                in_string = True
                current.append(ch)
            elif ch == ';':
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(ch)

        i += 1

    # Capture any trailing content after the last semicolon
    remainder = ''.join(current).strip()
    if remainder:
        statements.append(remainder)

    return statements


def load_classicmodels(sql_file: str, db_file: str) -> None:
    if not os.path.exists(sql_file):
        print(f"ERROR: Source file not found: {sql_file}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading {sql_file}...")
    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    print("Converting MySQL syntax to DuckDB...")
    content = convert_mysql_to_duckdb(content)

    # Remove any existing DuckDB file to start fresh
    if os.path.exists(db_file):
        os.remove(db_file)

    print(f"Connecting to {db_file}...")
    con = duckdb.connect(db_file)
    executed = 0
    errors = 0
    error_limit = 5

    try:
        for stmt in split_sql_statements(content):
            stmt = stmt.strip()
            if "alembic_version" in stmt.lower():
                continue
            # Skip empty statements and standalone comments
            if not stmt:
                continue
            if stmt.startswith('--'):
                continue
            try:
                con.execute(stmt)
                executed += 1
            except Exception as e:
                errors += 1
                if errors <= error_limit:
                    print(f"  [WARN] {e}")
                    print(f"         Statement: {stmt[:100]}...")
    finally:
        con.close()
    print(f"\nDone. Executed: {executed} statements, Errors: {errors}")
    print(f"DuckDB file created: {db_file}")


if __name__ == '__main__':
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(script_dir, '../syntetic_data_generation/result_db_source', 'inventory.sql')
    db_path = os.path.join(script_dir, 'inventory.duckdb')
    load_classicmodels(sql_path, db_path)
