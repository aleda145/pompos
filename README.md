# Pompos

Describe what you want to ingest, develop it with an agent, and let Go handle execution and scheduling. Every ingestion uses Python and dlt.

One source table = one ingestion YAML = one Python file = one destination data table. Nested values stay in JSON columns; dlt also maintains its own internal metadata tables. Configured destinations currently support DuckDB.

## Start

```bash
make setup
make run
```

Open `http://localhost:8080`. Setup starts with a separate **Connection** step:

1. **Connect MCP** to use Codex CLI or another MCP client. Your client handles the model and its credentials; Pompos needs no model or search provider key. Setup goes directly to the connection instructions below.
2. **Provide agent credentials** to use the built-in chat. Enter an OpenAI-compatible base URL (for example `https://api.openai.com/v1`) and model ID. Paste a provider key or select an existing managed secret. For a local endpoint without authentication, explicitly select **No authentication**. The model must support Chat Completions function/tool calling. A separate **Web search** step offers an optional [Exa API](https://exa.ai/docs/reference/search) key or **Skip for now**.

Existing valid agent configurations continue working. Incomplete setup resumes at the missing step. New provider keys are stored in managed secrets; settings contain only their references. Saving configuration does not make a paid provider request. Switching modes retains provider settings and existing ingestions, and disables MCP access when you select the built-in agent.

## Codex CLI and Claude Code through MCP

Choose **Connect MCP** in setup, then click **Enable** under **Agent settings → MCP**. MCP defaults to disabled, including existing configurations without the enable flag. The status and action button update immediately; **Disable** rejects subsequent MCP requests without changing provider settings or ingestions. Select and copy the one-line registration command for Codex CLI or Claude Code. No MCP token or model-provider credentials are required by Pompos. Keep Pompos running on localhost and run your client on the same machine; no Pompos project checkout or build is needed. This also works with the provided Docker Compose setup.

Registration persists in your client's configuration. If upgrading from token authentication, remove the old entry with `codex mcp remove pompos` and copy the new command from Agent settings to remove the token environment-variable requirement. Old server token files are no longer used. Claude Code connects with `claude mcp add --transport http pompos http://127.0.0.1:8080/mcp`; see its [MCP documentation](https://code.claude.com/docs/en/mcp).

Try asking Codex:

> Use Pompos to ingest today's ECB exchange rates.

The entire ingestion workflow can stay in Codex CLI. Codex uses Pompos tools to find existing ingestions or develop a new one, without inspecting local project files or setting up Python. It writes and probes a draft, configures loading, validates a sample, saves, and runs it when requested. A request to ingest includes these ordinary steps; a request only to prepare or validate does not authorize a production run. Runs stay manual unless you request a schedule. Saving activates a configured cron schedule.

When a meaningful choice is unclear, Codex asks you directly in the CLI and waits for your answer before the dependent action. For example, it can ask whether you want a current snapshot or historical rates, or which destination to use. Answers are applied in the next tool call; there is no MCP proposal/response loop or web confirmation step. Codex should reuse choices and permission already supplied, and ask before ambiguous replacement of existing data, adding a schedule, or materially broadening the request. These instructions guide the client; they cannot disable its other tools.

The tools are:

- **Develop:** `new_chat`, `list_chats`, `get_chat`, `context`, `write_script`, `test_script`, `read_webpage`, and optional Exa-backed `web_search`.
- **Configure and validate:** `configure_loading` sets UTC cron, strategy and row keys directly. `validate_ingestion` executes a bounded sample check (default 100 rows, maximum 1,000) and returns checks and a preview. Success makes the draft ready to save.
- **Save and operate:** `save_ingestion`, `list_ingestions`, `get_ingestion`, `run_ingestion`, `set_schedule`, and `preview_ingestion`. No separate `finish` call is needed.

Development tools take a `session_id`; saved-ingestion tools take an `ingestion_id`. Chat IDs and drafts persist across reconnects. Normal tool responses contain compact state and the current result, without replaying history or Python code. `list_chats`/`get_chat` resume work; `get_chat` includes draft code and validation, with recent messages available through `include_history: true`. Optional review links let you inspect the same state in the web UI. Questions asked in Codex remain in the Codex conversation; only applied settings and tool activity persist in Pompos. A queued full run is reported as queued, not completed; `get_ingestion` reports its execution status. Previews use the same fixed, read-only, 10-row query as the UI. No arbitrary SQL tool is exposed.

The MCP client is responsible for honoring your scope and authorization when configuring, validating, saving, changing schedules, or starting full loads. Pompos binds validation to the current script and settings, rejects untested scripts, and requires successful current validation before saving. Code or loading changes invalidate validation. Each validation call executes another bounded check. Retrying a save returns the same ingestion; retrying a full-run request can queue another run. The built-in web agent retains its approval cards.

**Source credentials are the exception:** Codex asks you to save the named value on Pompos's Secrets page using `context`'s `credentials_path`. After saving it, tell Codex to continue; it refreshes the available secret names and retries the operation. No secret-value read/write tool is exposed. Keep credentials out of Codex messages and Python code. Public sources need no credential detour. Codex can use its own research tools without an Exa key. Python probes and validation execute generated code with the server's operating-system permissions; use trusted clients. Source samples, code, and tool output are visible to your MCP client and its model provider.

The server uses the [official Go MCP SDK](https://github.com/modelcontextprotocol/go-sdk) and Streamable HTTP. `/mcp` is unauthenticated and available only when its status is **Enabled** (`mcp_enabled: true` in settings). It rejects browser origins and non-loopback Host names, bounds request sizes, and validates tool arguments. Access is controlled by the localhost listener, not a token; Host checks alone do not restrict network peers. This is a trusted local integration, like the rest of Pompos's unauthenticated interface. The app defaults to `127.0.0.1:8080`; Docker Compose publishes its port on `127.0.0.1`.

## Built-in agent

Choose **Add ingestion** and try:

> Ingest individual GitHub stargazers from https://github.com/aleda145/kavla

The agent lists available destinations and source secret names, researches current official API docs with `web_search` and `read_webpage`, asks for missing information, writes an extractor, and executes a small source probe. Exa search returns up to five links with content highlights; the agent is instructed to read the actual documentation to verify versions, authentication, fields, pagination and limits before coding. Expand search and documentation activity rows to open source links and inspect the text and retrieval timestamps. Research results persist with the conversation. A compact activity log streams each step as it happens. Expand Thinking or a tool row to inspect its progress update, request, or result. Python errors and samples feed back into the model so it can repair its code. Common questions appear as action buttons. Credential requests provide an inline managed-secret form, **I've added a key, try again**, and **Tell me more**. Save a key and retry directly, or update the named secret on the Secrets page and click retry. Choices and pending credential requests survive a reload; free-text chat remains available for anything else. Model and search provider keys are excluded from source credentials. Do not paste keys into chat.

Documentation reading supports public HTML, text, Markdown, JSON and YAML. It extracts readable text, code blocks and links, with up to 12,000 characters per tool result and a continuation offset for longer pages. Requests have a 20-second timeout and a 2 MiB response limit. The reader does not execute JavaScript, sign into sites, or parse PDFs. A blocked or unreadable page returns an error so the agent can try another official reference or ask for an accessible excerpt. Search snippets are treated as leads, and fetched content as untrusted reference data. Documentation requests carry no managed credentials; private-network destinations and redirects are rejected.

Each turn allows up to 12 model/tool rounds and four minutes. A source probe has a 45-second timeout and consumes at most five yielded rows. The generated extractor receives `limit=5` so it can also bound network requests. These are application limits, not an operating-system sandbox. A successful, nonempty probe of the current code is required before the agent can mark it ready. This verifies extraction; it does not replace the user-approved loading validation below.

After inspecting the source, the agent proposes a schedule and loading strategy with a short explanation. Confirm them with **Use these settings**, or adjust the frequency, five-field cron expression, strategy, and row keys in the settings card. All schedules use UTC; blank cron means manual runs only. The agent uses replace for current-state snapshots, merge for updates with stable row keys, or append for events and intentional history, and asks when the desired behavior is unclear. Confirmation is required before saving, and script revisions cannot silently override those choices.

Before saving, the agent proposes **Run validation**, **Tell me more**, or **Not now**. The card shows a source-row limit (100 by default, 1–1,000 allowed) and the temporary loading checks. Only clicking **Run validation** starts this step. It has a 90-second timeout, fetches the bounded sample once, and loads that same sample twice into a disposable DuckDB using the confirmed strategy. It checks missing/null/duplicate row keys when configured, load errors, and row counts: replace and merge should keep one copy; append should keep both. The temporary database and its dlt state are removed afterward. API requests may return whole pages even when fewer rows are consumed.

A successful validation of the current script and settings is required before **Save ingestion** becomes available. Failures return to the agent for repair; code or loading changes require fresh validation approval. **Not now** keeps the draft. Validation checks a sample in a temporary database, so it does not prove every source page will work or verify production destination permissions and existing schema conflicts.

Successful validation also shows a table preview of up to 10 rows actually loaded into the temporary database after the second load. The preview stays in the conversation after the temporary database is removed. The ingestion detail page shows up to 10 rows from the saved destination table alongside its actual total row count, without rerunning the source extractor. Previews use fixed read-only queries to count the table and fetch at most 10 rows. When more validation rows exist, the chat directs you to run the saved ingestion and query your destination database in a client of your choice. Long cell values are shortened. Empty, missing, or busy tables have an explicit preview state.

Pompos does not accept or execute user-defined SQL. Previews use a built-in, fixed, read-only table query against the configured DuckDB destination; there is no SQL editor or arbitrary-query endpoint.

The agent skips row estimates by default. When the source or user context suggests a full extraction may exceed 1 million rows, it uses readily available metadata to discuss volume, scope, cadence, and loading strategy in the conversation. It does not scan the source just to count it, and an unavailable count does not block progress. Estimates are not part of the extractor contract or saved ingestion YAML.

Review the source, destination, schedule, load strategy, Python draft, and validation results. **Save ingestion** creates the Python and YAML files. A confirmed cron schedule becomes active when you save; its first automatic load runs at the next scheduled time. From the detail page, **Run ingestion** queues an immediate full load, and the schedule can also be changed there. Full loads have a 30-minute timeout. Existing run status, retries and scheduler behavior remain in Go. `replace` is the default strategy; `append` and primary-key-based `merge` are also supported for Python jobs.

One chat can create multiple ingestions, one at a time. Saving keeps you in the conversation and adds a link to the saved ingestion. For example, after saving men's high-jump records, ask "do the same for women" to reuse the conversation's research and code. The new ingestion has its own table, Python file, YAML, loading confirmation, and validation; saving it does not change earlier ingestions. Choose **New chat** when you want a separate conversation.

Conversations and drafts survive restarts under `data/agent/`. Open **Chat** in the header to revisit a conversation or start a new one. Chats are named from their first message and listed by most recent activity, with their saved-ingestion counts. Each saved ingestion has its own ID, independent of the chat; its artifacts live under `data/ingestions/<id>.py` and `<id>.yaml`. YAML stores the Python digest and secret references, never secret values. Schedule changes preserve those fields. If the saved Python changes, execution fails its digest check; develop and test a new ingestion through chat to replace it.

## Python contract

The agent supplies a generator:

```python
def fetch(secret, limit):
    # Read credentials with secret("managed-secret-name") when necessary.
    # Make source requests with explicit timeouts and check their status.
    # Yield dictionaries for one entity. Respect any integer limit; paginate when None.
    yield {"id": 1, "name": "example"}
```

Pompos adds a runnable entrypoint that probes `fetch`, validates a sample, or loads through a single dlt resource. `limit` is 5 during a source probe, the approved cap during validation, and `None` during a full load. No connector catalog is required. Python's standard library, requests, dlt and DuckDB are installed. Extra source libraries must be installed by the operator; the agent has no package-install tool.

Run a saved ingestion from the CLI with the same data directory and interpreter:

```bash
POMPOS_PYTHON_BINARY="$PWD/.venv/bin/python" go run ./cmd/pompos run data/ingestions/<id>.yaml
```

The generated file can also run directly with `POMPOS_PROBE=1`, or with `POMPOS_CONFIG` JSON containing `destination`, `table`, `strategy` and `primary_key`. Supply selected secrets as a `POMPOS_SECRETS` JSON object. Normal Pompos execution prepares these variables from managed storage.

## Runtime and trust

This is a trusted, self-hosted operator tool. Generated Python executes with Pompos's operating-system permissions and network access. Probe mode skips the provided dlt loader, but arbitrary generated code is not sandboxed. Use a dedicated environment and trusted model endpoint. The endpoint receives conversation text, generated code, destination descriptions, source samples, search results and fetched documentation. Search queries are sent to Exa; fetched URLs are requested directly from their hosts. Only explicitly referenced secrets are injected into Python; exact secret values are redacted from captured output. The existing secret store uses SQLite, without encryption at rest. Do not expose the unauthenticated application to untrusted users.

`POMPOS_DATA_DIR` defaults to `./data`, `POMPOS_ADDRESS` to `127.0.0.1:8080`, and `POMPOS_PYTHON_BINARY` to `python3`. `make run` selects the project virtual environment. Docker installs the same pinned Python dependencies.

## Development

Go 1.25 or newer is required by the MCP SDK. Go installations with automatic toolchain downloads enabled can obtain it from the module requirement.

```bash
make build
make test
make vet
node --test static/*.test.cjs
# Real dlt/DuckDB integration test (after make setup):
POMPOS_TEST_PYTHON="$PWD/.venv/bin/python" go test ./internal/runner/python -v
make docker-build
make docker-up
make docker-down
```

Tests cover the iterative repair loop, persistent conversations, documentation research and source links, provider credential separation, user-approved validation before saving, secret redaction, script digest checks, chat publication, scheduling, and a real dlt load. Provider behavior is tested with a fake OpenAI-compatible endpoint; a real configured model still needs an end-to-end trial.

The runtime uses dlt's [resource](https://dlthub.com/docs/general-usage/resource) and [pipeline](https://dlthub.com/docs/general-usage/pipeline) APIs.
