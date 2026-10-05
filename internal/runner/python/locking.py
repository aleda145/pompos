from contextlib import contextmanager as _pompos_contextmanager


@_pompos_contextmanager
def _pompos_destination_lock(path, shared=False):
    import fcntl
    from pathlib import Path

    # Use the same sidecar for row loads, object catalogs, and readers.
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with Path(str(path) + ".pompos.lock").open("a") as guard:
        fcntl.flock(guard, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        yield
