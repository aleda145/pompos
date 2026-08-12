# pompos v0 — Vertical Slice Build Instructions

Build a very small self-hosted web application called **pompos**.

pompos is a self-service data ingestion tool. Its long-term purpose is to let users connect data sources and ingest raw data into destinations owned by a data platform team.

For this v0, implement only one complete vertical slice:

> **Remote CSV URL → DuckDB file on disk**

pompos should execute the ingestion itself using the **ingestr CLI as a subprocess**. ingestr is a Python application and should not be embedded into the Go process.

Do not add an external orchestrator.

The goal is not production readiness. The goal is to touch each important architectural component with the smallest possible implementation.

## Technology

Use:

- **Go** for the pompos application
- Server-rendered HTML using `html/template`
- Minimal CSS
- No frontend framework
- SQLite for pompos application metadata
- DuckDB as the destination
- **ingestr installed as a Python CLI alongside pompos**
- `os/exec` / `exec.CommandContext` to invoke ingestr
- Docker for local/self-hosted deployment

Keep dependencies minimal.

The runtime architecture should be:

```text
pompos Go process
      │
      │ exec.CommandContext(...)
      ▼
ingestr CLI
      │
      ▼
Python runtime + connector dependencies
      │
      ▼
DuckDB
```

Do not attempt to:

- embed Python into Go
- import ingestr as a Go library
- reimplement CSV ingestion in Go

Treat the ingestr CLI as the integration boundary.

## User experience

The homepage should show:

```text
pompos

Ingestions

Remote CSV → DuckDB
example.csv
Status: succeeded

[ + Add ingestion ]
```

Clicking **Add ingestion** opens:

```text
Add ingestion

CSV URL
[ https://example.com/data.csv ]

Table name
[ customers ]

[ Create ingestion ]
```

After submission:

```text
customers

Source
https://example.com/data.csv

Destination
DuckDB
./data/pompos.duckdb
customers

Status
Running / Succeeded / Failed

[ Run again ]
```

There is no scheduling in v0.

---

## Core domain model

Define a small ingestion model independent of ingestr.

Conceptually:

```go
type Ingestion struct {
    ID          string
    Name        string
    Source      Source
    Destination Destination
    Status      string
}

type Source struct {
    Type string
    URL  string
}

type Destination struct {
    Type  string
    Path  string
    Table string
}
```

Do not expose ingestr CLI arguments in this core model.

For v0:

- source type is always `csv`
- destination type is always `duckdb`
- destination path is always `./data/pompos.duckdb`

---

# Components

Implement these components separately, even if each is very small.

## 1. Web UI

Routes:

```text
GET  /
GET  /ingestions/new
POST /ingestions
GET  /ingestions/{id}
POST /ingestions/{id}/run
```

Use normal HTML forms.

Do not build an SPA.

---

## 2. Metadata store

Use SQLite to store:

- ingestion ID
- name
- CSV URL
- destination table
- status
- last run timestamp
- last error

Create the database automatically on startup.

Default location:

```text
./data/pompos.sqlite
```

---

## 3. Source catalog

Create a minimal source abstraction.

For v0 it only needs to know about CSV:

```go
type SourceDefinition struct {
    Type string
    Name string
}
```

Register:

```text
csv → Remote CSV
```

Structure this so more source types can be added later.

Do not build a plugin system.

---

## 4. Connection / source validation

When creating an ingestion:

- validate that the URL uses HTTP or HTTPS
- perform a lightweight HTTP request to verify that the URL is reachable
- reject clearly invalid URLs
- use sensible HTTP timeouts
- do not download or parse the entire CSV in the Go application

The Go application validates connectivity.

**ingestr is responsible for actually reading and ingesting the file.**

There are no source secrets in this v0 because the CSV is public.

---

## 5. Secret store abstraction

Create the interface, but do not implement a real secrets manager.

For example:

```go
type SecretStore interface {
    Put(ctx context.Context, key string, value []byte) error
    Delete(ctx context.Context, key string) error
}
```

Provide:

```go
type NoopSecretStore struct{}
```

Nothing in v0 needs to call `Put`.

This interface exists only to establish the architectural boundary for future authenticated sources.

---

## 6. Platform configuration

Create a small application configuration.

Defaults:

```yaml
destination:
  type: duckdb
  path: ./data/pompos.duckdb

runner:
  type: ingestr
  binary: ingestr
```

Configuration can come from YAML, environment variables, or both.

The user creating an ingestion must **not** choose the destination.

The destination is platform-owned configuration.

Allow the ingestr binary path to be overridden, for example:

```text
POMPOS_INGESTR_BINARY=/usr/local/bin/ingestr
```

This makes local development and testing easier.

---

## 7. Runner abstraction

Create a small runner interface:

```go
type Runner interface {
    Run(ctx context.Context, ingestion Ingestion) error
}
```

Implement only:

```go
type IngestrRunner struct {
    Binary string
}
```

The rest of pompos should not know how ingestr works.

Future implementations might include:

```text
DLTRunner
CustomRunner
```

Do not implement them.

---

## 8. ingestr CLI integration

`IngestrRunner` should invoke the locally installed `ingestr` binary using Go's standard library.

Use:

```go
exec.CommandContext(...)
```

Conceptually:

```go
cmd := exec.CommandContext(
    ctx,
    r.Binary,
    "ingest",
    "--source-uri", sourceURI,
    "--source-table", sourceTable,
    "--dest-uri", destinationURI,
    "--dest-table", destinationTable,
)
```

The exact arguments must match the installed ingestr version.

Do not shell-concatenate command strings.

Pass arguments individually through `exec.CommandContext`.

This avoids shell injection and quoting problems.

### Capture execution information

Capture:

- stdout
- stderr
- exit code
- execution error

Update status:

```text
pending
running
succeeded
failed
```

Persist the latest error message when execution fails.

Useful errors should include the relevant ingestr stderr, but avoid returning enormous output into the UI.

A reasonable maximum stored error length is acceptable.

### Context

Use `exec.CommandContext` so cancellation propagates to the ingestr subprocess.

The subprocess boundary should be treated as intentional architecture, not a temporary workaround.

---

## 9. Command generation

Separate translating a pompos `Ingestion` into ingestr CLI arguments from actually executing the process.

For example:

```go
type CommandBuilder interface {
    Args(ingestion Ingestion) ([]string, error)
}
```

or simply a function:

```go
func BuildIngestrArgs(ingestion Ingestion) ([]string, error)
```

This should be deterministic and easy to unit test.

The flow should be:

```text
pompos Ingestion
      ↓
BuildIngestrArgs
      ↓
[]string
      ↓
exec.CommandContext
      ↓
ingestr
```

Do not spread ingestr CLI flag construction throughout the application.

---

## 10. Policy layer

Create a tiny deterministic validation layer.

For v0 enforce:

- source URL must use HTTP or HTTPS
- table name cannot be empty
- table name must contain only safe identifier characters
- destination is always the platform-configured DuckDB
- source type must be `csv`
- destination type must be `duckdb`
- transformations are impossible

Structure it as:

```go
type PolicyEngine interface {
    Validate(Ingestion) error
}
```

A single implementation is enough.

---

## 11. Ingestion spec

Create a deterministic YAML representation for each ingestion.

Example:

```yaml
apiVersion: pompos.dev/v1
kind: Ingestion

metadata:
  name: customers

source:
  type: csv
  url: https://example.com/customers.csv

destination:
  type: duckdb
  table: customers
```

Store generated specs under:

```text
./data/ingestions/{id}.yaml
```

The physical DuckDB path should preferably be derived from platform configuration rather than duplicated into every ingestion spec.

The spec establishes the future Git-oriented architecture.

Do not implement Git in v0.

---

## 12. Status/catalog

The homepage acts as the ingestion catalog.

For every ingestion show:

- name
- source URL
- destination table
- status
- last run time

The detail page additionally shows:

- latest error
- ingestion ID
- generated spec path

No full logs UI is required.

---

# Execution model

For v0, execution can happen synchronously inside the pompos server process.

Creating an ingestion:

```text
POST /ingestions
        ↓
validate input
        ↓
build pompos Ingestion
        ↓
policy validation
        ↓
persist metadata
        ↓
write YAML ingestion spec
        ↓
mark running
        ↓
exec ingestr subprocess
        ↓
mark succeeded / failed
        ↓
redirect to detail page
```

`Run again` repeats the execution portion:

```text
policy validation
      ↓
mark running
      ↓
exec ingestr
      ↓
update status
```

Do not build:

- queues
- workers
- goroutine-based job infrastructure
- orchestration

If the HTTP request waits while ingestr executes, that is acceptable for v0.

---

# Repository structure

Prefer approximately:

```text
/
├── cmd/
│   └── pompos/
│       └── main.go
│
├── internal/
│   ├── config/
│   ├── ingestion/
│   ├── policy/
│   ├── runner/
│   │   └── ingestr/
│   ├── secrets/
│   ├── store/
│   └── web/
│
├── templates/
├── static/
├── data/
├── Dockerfile
├── docker-compose.yml
├── go.mod
└── README.md
```

Do not over-engineer the package structure if something simpler is clearer.

---

# Docker

Provide a Docker image containing:

```text
pompos Go binary
Python runtime
ingestr CLI
required ingestr dependencies
writable /data directory
```

A multi-stage build is preferable.

Conceptually:

```text
build Go binary
      ↓
runtime image
  ├── pompos
  ├── python
  └── ingestr
```

Do not attempt to use a `scratch` image because ingestr requires Python.

For this v0, only install the dependencies required for:

```text
remote CSV → DuckDB
```

Do not preinstall every possible ingestr connector dependency.

Running:

```bash
docker compose up
```

should expose:

```text
http://localhost:8080
```

Persist `/data` using a Docker volume.

The volume should contain:

```text
/data/pompos.sqlite
/data/pompos.duckdb
/data/ingestions/*.yaml
```

All should survive container restarts.

---

# Explicit non-goals

Do **not** implement:

- authentication
- users or organizations
- GitHub
- GitLab
- pull requests
- Git integration
- external orchestrators
- scheduling
- queues
- background workers
- AWS/GCP/Azure secret managers
- authenticated data sources
- transformations
- SQL editing
- dbt
- lineage
- data contracts
- schema management
- notifications
- RBAC
- multiple destinations
- multiple runners
- arbitrary ingestion frameworks
- dynamic plugin systems
- connector installation UI
- monitoring infrastructure

Create interfaces where they establish a useful architectural boundary, but implement only what the vertical slice requires.

---

# Design principles

## pompos owns intent

The core domain model describes:

```text
source
resource/data
destination
freshness later
ownership later
```

It does not describe ingestr CLI flags.

---

## ingestr is an external execution backend

ingestr is a Python CLI.

pompos interacts with it exclusively through its command-line interface:

```text
Go
 ↓
os/exec
 ↓
ingestr
```

Do not couple the application to ingestr Python internals.

This subprocess boundary is desirable because future execution backends may be implemented the same way.

---

## EL only

pompos performs:

```text
Extract
Load
```

Never:

```text
Transform
```

Do not add transformation hooks.

Do not rename fields.

Do not add calculated columns.

Do not add SQL.

The source should land raw according to the ingestion framework.

---

## Destination is platform-owned

The stakeholder supplies:

```text
CSV URL
table name
```

Platform configuration supplies:

```text
DuckDB destination
```

The stakeholder cannot modify the destination.

---

## Execution backend is replaceable

ingestr is the only implementation in v0.

Keep it behind:

```go
type Runner interface {
    Run(context.Context, Ingestion) error
}
```

Do not build a generic plugin framework.

A normal Go interface is enough.

---

## Existing ingestion should survive pompos restarts

The persisted state lives under `/data`.

Restarting the pompos process must not remove:

- metadata
- ingestion specs
- DuckDB tables

---

# Tests

Add focused tests for the important boundaries.

## Policy tests

Test:

- valid HTTPS URL
- valid HTTP URL
- invalid scheme
- empty table
- unsafe table name

## Spec tests

Given a known ingestion, verify the exact generated YAML.

Generation should be deterministic.

## ingestr command tests

Given:

```text
CSV URL
table
DuckDB destination
```

verify that `BuildIngestrArgs` generates the expected argument list.

Do not require ingestr to be installed for this unit test.

## Runner integration test

If practical, add one opt-in integration test that runs a real ingestr subprocess against a small public or locally served CSV and verifies the resulting DuckDB table.

Keep it separate from normal unit tests if it requires Python/ingestr.

---

# Acceptance test

A fresh checkout must support this exact flow.

## 1. Start pompos

```bash
docker compose up
```

## 2. Open

```text
http://localhost:8080
```

## 3. Add ingestion

Click:

```text
+ Add ingestion
```

Enter a publicly accessible CSV URL.

Enter:

```text
customers
```

as the table name.

Submit.

## 4. Execution

pompos must:

```text
validate URL
      ↓
create ingestion
      ↓
validate policies
      ↓
write YAML spec
      ↓
execute ingestr CLI
      ↓
ingestr loads CSV into DuckDB
      ↓
update status
```

## 5. Result

The detail page shows:

```text
Status: succeeded
```

The persisted data directory contains:

```text
pompos.sqlite
pompos.duckdb
ingestions/<id>.yaml
```

## 6. Verify data

Query:

```sql
SELECT * FROM customers;
```

inside `pompos.duckdb`.

It must return the rows from the remote CSV.

## 7. Run again

Click:

```text
Run again
```

pompos invokes ingestr again and updates the status successfully.

## 8. Restart

Restart the container.

The ingestion catalog and DuckDB data remain available.

---

# Deliverables

Produce:

- working Go application
- server-rendered HTML templates
- SQLite metadata persistence
- source catalog abstraction
- no-op secret store abstraction
- platform configuration
- policy engine
- deterministic pompos ingestion spec generation
- ingestr argument builder
- ingestr subprocess runner using `exec.CommandContext`
- DuckDB persistence
- Dockerfile containing Go + Python + ingestr
- docker-compose.yml
- README with exact setup and run instructions
- focused unit tests
- optional real ingestr integration test

Favor a small, understandable, working vertical slice over completeness or production hardening.
