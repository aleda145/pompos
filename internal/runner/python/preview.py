"""Pompos-owned table preview. No SQL, filters, or limits are accepted as input."""
import json
import sys

import duckdb
from dlt.common.normalizers.naming.snake_case import NamingConvention

path, schema, table = sys.argv[1:]
schema = NamingConvention().normalize_identifier(schema)
table = NamingConvention().normalize_table_identifier(table)
with duckdb.connect(path, read_only=True, config={"enable_external_access": False}) as db:
    # Only loaded base tables, never views that could execute arbitrary expressions.
    exists = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = ? "
        "AND table_name = ? AND table_type = 'BASE TABLE'", [schema, table]
    ).fetchone()
    if not exists:
        raise ValueError("No loaded table")
    target = '"' + schema.replace('"', '""') + '"."' + table.replace('"', '""') + '"'
    db.execute("BEGIN TRANSACTION")
    total_rows = db.execute("SELECT count(*) FROM " + target).fetchone()[0]
    cursor = db.execute("SELECT * FROM " + target + " LIMIT 10")
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

def display(value):
    if value is None:
        return "NULL"
    if isinstance(value, (dict, list, tuple, bool)):
        text = json.dumps(value, ensure_ascii=False, default=str)
    elif isinstance(value, bytes):
        text = value.hex()
    else:
        text = str(value)
    return text


display_rows = [[display(value) for value in row] for row in rows[:10]]
print(json.dumps({"columns": columns, "rows": display_rows,
                  "total_rows": total_rows, "has_more": total_rows > len(rows)}))
