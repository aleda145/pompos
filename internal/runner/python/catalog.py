"""Read destination schema and table names without executing source code or views."""
import json
import sys

import duckdb

with duckdb.connect(sys.argv[1], read_only=True, config={"enable_external_access": False}) as db:
    rows = db.execute("""
        SELECT s.schema_name, t.table_name
        FROM information_schema.schemata AS s
        LEFT JOIN information_schema.tables AS t
          ON t.table_catalog = s.catalog_name AND t.table_schema = s.schema_name
          AND left(t.table_name, 4) <> '_dlt'
        WHERE s.catalog_name = current_database()
          AND s.schema_name NOT IN ('information_schema', 'pg_catalog')
        ORDER BY s.schema_name, t.table_name
    """).fetchall()

schemas = {}
for schema, table in rows:
    schemas.setdefault(schema, [])
    if table is not None:
        schemas[schema].append(table)
print(json.dumps([{"name": name, "tables": tables} for name, tables in schemas.items()]))
