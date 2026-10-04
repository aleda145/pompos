# Pompos owns publication and the catalog. Source code defines fetch(secret, limit)
# and optionally download(object, target_path, secret), writing one staged file.
def _pompos_objects():
    import datetime
    import fcntl
    import hashlib
    import itertools
    import json
    import mimetypes
    import os
    from pathlib import Path, PurePosixPath
    import re
    import resource
    import tempfile
    from urllib.parse import urlparse

    config = json.loads(os.environ.get("POMPOS_CONFIG", "{}"))
    secrets = json.loads(os.environ.get("POMPOS_SECRETS", "{}"))

    def secret(name):
        if name not in secrets:
            raise ValueError("Missing managed secret: " + name)
        return secrets[name]

    probe = os.environ.get("POMPOS_PROBE") == "1"
    if config.get("destination_type") != "objects":
        raise ValueError("File ingestion requires an objects destination")
    validation_limit = config.get("validation_limit", 0)
    validation = config.get("validation", bool(validation_limit))
    limit = 5 if probe else (validation_limit or None)
    byte_budget = (config.get("validation_max_bytes") or None) if validation else None
    downloaded_bytes = 0

    def timestamp(value):
        if value is None:
            return None
        value = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if value.tzinfo is None:
            raise ValueError("source_modified_at must include a timezone")
        return value

    def objects():
        seen = set()
        for item in itertools.islice(fetch(secret, limit), limit):
            if not isinstance(item, dict):
                raise TypeError("fetch must yield object dictionaries")
            item = dict(item)
            for field in ("object_id", "source_uri", "filename"):
                if not isinstance(item.get(field), str) or not item[field].strip() or "\x00" in item[field]:
                    raise ValueError(field + " must be a nonempty string without null characters")
            name = PurePosixPath(item["filename"])
            if name.is_absolute() or ".." in name.parts or "\\" in item["filename"] or not name.name or str(name) == ".":
                raise ValueError("filename must be a safe relative path")
            if item["object_id"] in seen:
                raise ValueError("Duplicate object_id in source collection: " + item["object_id"])
            seen.add(item["object_id"])
            if item.get("source_version") is not None and not isinstance(item["source_version"], str):
                raise ValueError("source_version must be a string or null")
            if item.get("content_type") is not None and not isinstance(item["content_type"], str):
                raise ValueError("content_type must be a string or null")
            if not isinstance(item.get("metadata", {}), dict):
                raise ValueError("metadata must be a JSON object")
            json.dumps(item.get("metadata", {}), allow_nan=False)
            timestamp(item.get("source_modified_at"))
            yield item

    if probe:
        sample = list(objects())
        if not sample:
            raise ValueError("Probe returned no objects")
        # Download URLs may contain short-lived credentials. They are never cataloged.
        fields = ("object_id", "source_uri", "filename", "source_version", "content_type", "metadata")
        rows = [{key: item[key] for key in fields if key in item} for item in sample]
        print("POMPOS_PROBE_RESULT=" + json.dumps({"rows": rows, "sample_count": len(rows), "data": "files"}))
        return

    schema = config.get("schema", "")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) or schema.lower() in ("main", "information_schema", "pg_catalog") or config.get("table") != "objects":
        raise ValueError("Objects require a dedicated schema and table objects")
    strategy = config.get("strategy") or "update"
    if strategy not in ("update", "skip") or config.get("primary_key"):
        raise ValueError("Objects use update or skip and fixed object_id keys")

    root = Path(config["destination"]).resolve()
    root.mkdir(parents=True, exist_ok=True)

    def inside(path):
        resolved = path.resolve()
        if not resolved.is_relative_to(root):
            raise ValueError("Object path escapes the destination")
        return resolved

    files = inside(root / "files" / schema)
    files.mkdir(parents=True, exist_ok=True)
    catalog = inside(root / "objects.duckdb")

    def checksum(path):
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def intact(row):
        if row is None:
            return False
        # Only our file URIs are used to resolve stored bytes.
        from urllib.parse import unquote
        uri = urlparse(row[0])
        if uri.scheme != "file" or uri.netloc:
            return False
        path = inside(Path(unquote(uri.path)))
        return path.is_file() and path.stat().st_size == row[1] and checksum(path) == row[2]

    def download_http(item, path):
        import requests
        url = item.get("download_url") or item["source_uri"]
        if urlparse(url).scheme not in ("http", "https"):
            raise ValueError("Provide an HTTP download_url or define download(object, target_path, secret)")
        with requests.get(url, stream=True, timeout=(15, 60)) as response:
            response.raise_for_status()
            size = 0
            with path.open("wb") as output:
                for chunk in response.iter_content(1024 * 1024):
                    size += len(chunk)
                    if byte_budget is not None and downloaded_bytes + size > byte_budget:
                        raise ValueError("Validation exceeds max_bytes; select a smaller sample or raise the validation budget")
                    output.write(chunk)
            return response.headers.get("Content-Type", "").split(";", 1)[0] or None

    def load(db, items):
        nonlocal downloaded_bytes
        count = 0
        for item in items:
            now = datetime.datetime.now(datetime.timezone.utc)
            row = db.execute("SELECT uri, size_bytes, sha256, source_version FROM " + target + " WHERE object_id = ?", [item["object_id"]]).fetchone()
            unchanged = row and item.get("source_version") and item["source_version"] == row[3]
            if (strategy == "skip" or unchanged) and intact(row):
                db.execute("UPDATE " + target + " SET last_seen_at = ? WHERE object_id = ?", [now, item["object_id"]])
                if unchanged and strategy == "update":
                    db.execute("UPDATE " + target + " SET source_uri = ?, metadata = ? WHERE object_id = ?",
                               [item["source_uri"], json.dumps(item.get("metadata", {}), allow_nan=False), item["object_id"]])
                count += 1
                continue

            # Stage on the destination filesystem so publication is an atomic rename.
            with tempfile.TemporaryDirectory(prefix=".staging-", dir=files) as staging:
                staged = Path(staging) / PurePosixPath(item["filename"]).name
                custom = globals().get("download")
                content_type = None
                if custom:
                    old_limit = resource.getrlimit(resource.RLIMIT_FSIZE)
                    try:
                        if byte_budget is not None:
                            remaining = byte_budget - downloaded_bytes
                            if remaining <= 0:
                                raise ValueError("Validation exceeds max_bytes")
                            resource.setrlimit(resource.RLIMIT_FSIZE, (min(remaining, old_limit[0]) if old_limit[0] >= 0 else remaining, old_limit[1]))
                        custom(item, str(staged), secret)
                    finally:
                        resource.setrlimit(resource.RLIMIT_FSIZE, old_limit)
                else:
                    content_type = download_http(item, staged)
                if staged.is_symlink() or not staged.is_file():
                    raise ValueError("download must write a regular file at target_path")
                size = staged.stat().st_size
                downloaded_bytes += size
                if byte_budget is not None and downloaded_bytes > byte_budget:
                    raise ValueError("Validation exceeds max_bytes")
                sha = checksum(staged)
                identity = hashlib.sha256(item["object_id"].encode()).hexdigest()
                final = inside(files / identity / sha / item["filename"])
                final.parent.mkdir(parents=True, exist_ok=True)
                with staged.open("rb") as ready:
                    os.fsync(ready.fileno())
                os.replace(staged, final)
                directory = final.parent
                while True:
                    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    if directory == root:
                        break
                    directory = directory.parent
                # The row always describes published bytes. A crash before this
                # upsert leaves an unregistered file that the same retry reuses.
                db.execute("INSERT INTO " + target + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                           "ON CONFLICT (object_id) DO UPDATE SET "
                           "source_uri=excluded.source_uri, uri=excluded.uri, filename=excluded.filename, "
                           "content_type=excluded.content_type, size_bytes=excluded.size_bytes, sha256=excluded.sha256, "
                           "source_version=excluded.source_version, source_modified_at=excluded.source_modified_at, "
                           "last_seen_at=excluded.last_seen_at, downloaded_at=excluded.downloaded_at, metadata=excluded.metadata",
                           [item["object_id"], item["source_uri"], final.as_uri(), item["filename"],
                            item.get("content_type") or content_type or mimetypes.guess_type(item["filename"])[0] or "application/octet-stream",
                            size, sha, item.get("source_version"), timestamp(item.get("source_modified_at")),
                            now, now, now, json.dumps(item.get("metadata", {}), allow_nan=False)])
                count += 1
        return count

    import duckdb
    # Serialize catalog writers across ingestions and Pompos processes.
    with inside(root / ".catalog.lock").open("a") as guard:
        fcntl.flock(guard, fcntl.LOCK_EX)
        with duckdb.connect(str(catalog)) as db:
            database = db.execute("SELECT current_database()").fetchone()[0]
            target = '"' + database.replace('"', '""') + '"."' + schema + '"."objects"'
            db.execute('CREATE SCHEMA IF NOT EXISTS "' + schema + '"')
            db.execute("CREATE TABLE IF NOT EXISTS " + target + " ("
                       "object_id VARCHAR PRIMARY KEY, source_uri VARCHAR NOT NULL, uri VARCHAR NOT NULL, "
                       "filename VARCHAR NOT NULL, content_type VARCHAR NOT NULL, size_bytes BIGINT NOT NULL, "
                       "sha256 VARCHAR NOT NULL, source_version VARCHAR, source_modified_at TIMESTAMPTZ, "
                       "first_seen_at TIMESTAMPTZ NOT NULL, last_seen_at TIMESTAMPTZ NOT NULL, "
                       "downloaded_at TIMESTAMPTZ NOT NULL, metadata JSON NOT NULL)")
            if validation:
                sample = list(objects())
                if not sample:
                    raise ValueError("Validation returned no objects")
                load(db, sample)
                first = db.execute("SELECT count(*) FROM " + target).fetchone()[0]
                load(db, sample)
                second = db.execute("SELECT count(*) FROM " + target).fetchone()[0]
                if first != len(sample) or second != first:
                    raise ValueError("Unexpected object counts after repeated loads")
                print("POMPOS_VALIDATION_RESULT=" + json.dumps({"data": "files", "sample_count": len(sample), "first_load_rows": first, "second_load_rows": second, "downloaded_bytes": downloaded_bytes}))
            else:
                count = load(db, objects())
                print("POMPOS_OBJECTS_RESULT=" + json.dumps({"objects": count, "downloaded_bytes": downloaded_bytes}))


if __name__ == "__main__":
    _pompos_objects()
