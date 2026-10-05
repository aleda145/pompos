"""Pompos-owned table preview. No SQL, filters, or limits are accepted as input."""
import json
import sys

import duckdb
from dlt.common.normalizers.naming.snake_case import NamingConvention

path, schema, table, kind = sys.argv[1:]
if kind != "objects":
    schema = NamingConvention().normalize_identifier(schema)
    table = NamingConvention().normalize_table_identifier(table)
with _pompos_destination_lock(path, shared=True), duckdb.connect(path, read_only=True, config={"enable_external_access": False}) as db:
    # Only loaded base tables, never views that could execute arbitrary expressions.
    exists = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = ? "
        "AND table_name = ? AND table_type = 'BASE TABLE'", [schema, table]
    ).fetchone()
    if not exists:
        raise ValueError("No loaded table")
    database = db.execute("SELECT current_database()").fetchone()[0]
    target = ".".join('"' + name.replace('"', '""') + '"' for name in (database, schema, table))
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
