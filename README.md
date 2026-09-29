# Pompos

Describe what you want to ingest, develop it with an agent, and let Go handle execution and scheduling. Every ingestion uses Python and dlt.

One source table = one ingestion YAML = one Python file = one destination data table. Nested values stay in JSON columns; dlt also maintains its own internal metadata tables. Configured destinations currently support DuckDB.

## Start

```bash
make setup
make run
```

Open `http://localhost:8080`. When configuration is missing, Pompos opens a two-step setup flow:

1. **Agent — required.** Enter an OpenAI-compatible base URL (for example `https://api.openai.com/v1`) and model ID. Paste the provider key directly into setup or select an existing managed secret. For a local endpoint without authentication, explicitly select **My endpoint does not require authentication**. The model must support Chat Completions function/tool calling.
2. **Web search — optional, recommended.** Add an [Exa API](https://exa.ai/docs/reference/search) key, select an existing secret, or choose **Skip for now**. Pompos remembers that choice across restarts. Reading a supplied public documentation URL works without a search key.

Setup stores new keys in managed secrets and saves only their references in agent settings. Existing valid configuration is reused, and incomplete setup resumes at the missing step. If a referenced credential is deleted or empty, setup offers to repair it. Saving setup checks the configuration and local secret availability; it does not make a paid model or search request to verify the credentials. You can change the model or enable Exa later in **Agent settings**; manage key values under **Secrets**.

Choose **Add ingestion** and try:

> Ingest individual GitHub stargazers from https://github.com/aleda145/kavla

The agent lists available destinations and source secret names, researches current official API docs with `web_search` and `read_webpage`, asks for missing information, writes an extractor, and executes a small source probe. Exa search returns up to five links with content highlights; the agent is instructed to read the actual documentation to verify versions, authentication, fields, pagination and limits before coding. Expand search and documentation activity rows to open source links and inspect the text and retrieval timestamps. Research results persist with the conversation. A compact activity log streams each step as it happens. Expand Thinking or a tool row to inspect its progress update, request, or result. Python errors and samples feed back into the model so it can repair its code. Common questions appear as action buttons. Credential requests provide an inline managed-secret form, **I've added a key, try again**, and **Tell me more**. Save a key and retry directly, or update the named secret on the Secrets page and click retry. Choices and pending credential requests survive a reload; free-text chat remains available for anything else. Model and search provider keys are excluded from source credentials. Do not paste keys into chat.

Documentation reading supports public HTML, text, Markdown, JSON and YAML. It extracts readable text, code blocks and links, with up to 12,000 characters per tool result and a continuation offset for longer pages. Requests have a 20-second timeout and a 2 MiB response limit. The reader does not execute JavaScript, sign into sites, or parse PDFs. A blocked or unreadable page returns an error so the agent can try another official reference or ask for an accessible excerpt. Search snippets are treated as leads, and fetched content as untrusted reference data. Documentation requests carry no managed credentials; private-network destinations and redirects are rejected.

Each turn allows up to 12 model/tool rounds and four minutes. A source probe has a 45-second timeout and consumes at most five yielded rows. The generated extractor receives `limit=5` so it can also bound network requests. These are application limits, not an operating-system sandbox. A successful, nonempty probe of the current code is required before the agent can mark it ready. This verifies extraction; it does not replace the user-approved loading validation below.

After inspecting the source, the agent proposes a schedule and loading strategy with a short explanation. Confirm them with **Use these settings**, or adjust the frequency, five-field cron expression, strategy, and row keys in the settings card. All schedules use UTC; blank cron means manual runs only. The agent uses replace for current-state snapshots, merge for updates with stable row keys, or append for events and intentional history, and asks when the desired behavior is unclear. Confirmation is required before saving, and script revisions cannot silently override those choices.

Before saving, the agent proposes **Run validation**, **Tell me more**, or **Not now**. The card shows a source-row limit (100 by default, 1–1,000 allowed), expected sample size when known, and a separate production extraction estimate. Only clicking **Run validation** starts this step. It has a 90-second timeout, fetches the bounded sample once, and loads that same sample twice into a disposable DuckDB using the confirmed strategy. It checks missing/null/duplicate row keys when configured, load errors, and row counts: replace and merge should keep one copy; append should keep both. The temporary database and its dlt state are removed afterward. API requests may return whole pages even when fewer rows are consumed.

A successful validation of the current script and settings is required before **Save ingestion** becomes available. Failures return to the agent for repair; code or loading changes require fresh validation approval. **Not now** keeps the draft. Validation checks a sample in a temporary database, so it does not prove every source page will work or verify production destination permissions and existing schema conflicts.

Production row estimates use cheap source metadata where available, such as a matching API total count. They are labeled exact at observation time, approximate, or unknown, with an explanation and timestamp. Unknown counts do not block validation. Estimates describe rows the extractor emits on a full run, not newly inserted rows, and are never derived from sample size alone. They are saved in YAML and shown on the ingestion detail page; they are observations from development, not automatically refreshed forecasts for every scheduled run.

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

An extractor may also define `estimate(secret)`, returning `{"rows": 123, "kind": "exact", "basis": "API total_count for this query"}`. Use `approximate` for inferred counts or `{"rows": None, "kind": "unknown", "basis": "No count endpoint"}` when unavailable. This optional function runs during the source probe, should make only bounded metadata requests with explicit timeouts, and must account for the extractor's filters. Failed or invalid estimates fall back to unknown.

Pompos adds a runnable entrypoint that probes `fetch`, validates a sample, or loads through a single dlt resource. `limit` is 5 during a source probe, the approved cap during validation, and `None` during a full load. No connector catalog is required. Python's standard library, requests, dlt and DuckDB are installed. Extra source libraries must be installed by the operator; the agent has no package-install tool.

Run a saved ingestion from the CLI with the same data directory and interpreter:

```bash
POMPOS_PYTHON_BINARY="$PWD/.venv/bin/python" go run ./cmd/pompos run data/ingestions/<id>.yaml
```

The generated file can also run directly with `POMPOS_PROBE=1`, or with `POMPOS_CONFIG` JSON containing `destination`, `table`, `strategy` and `primary_key`. Supply selected secrets as a `POMPOS_SECRETS` JSON object. Normal Pompos execution prepares these variables from managed storage.

## Runtime and trust

This is a trusted, self-hosted operator tool. Generated Python executes with Pompos's operating-system permissions and network access. Probe mode skips the provided dlt loader, but arbitrary generated code is not sandboxed. Use a dedicated environment and trusted model endpoint. The endpoint receives conversation text, generated code, destination descriptions, source samples, search results and fetched documentation. Search queries are sent to Exa; fetched URLs are requested directly from their hosts. Only explicitly referenced secrets are injected into Python; exact secret values are redacted from captured output. The existing secret store uses SQLite, without encryption at rest. Do not expose the unauthenticated application to untrusted users.

`POMPOS_DATA_DIR` defaults to `./data`, `POMPOS_ADDRESS` to `:8080`, and `POMPOS_PYTHON_BINARY` to `python3`. `make run` selects the project virtual environment. Docker installs the same pinned Python dependencies.

## Development

```bash
make build
make test
make vet
node --test static/chat-stream.test.cjs
# Real dlt/DuckDB integration test (after make setup):
POMPOS_TEST_PYTHON="$PWD/.venv/bin/python" go test ./internal/runner/python -v
make docker-build
make docker-up
make docker-down
```

Tests cover the iterative repair loop, persistent conversations, documentation research and source links, provider credential separation, user-approved validation before saving, row estimates, secret redaction, script digest checks, chat publication, scheduling, and a real dlt load. Provider behavior is tested with a fake OpenAI-compatible endpoint; a real configured model still needs an end-to-end trial.

The runtime uses dlt's [resource](https://dlthub.com/docs/general-usage/resource) and [pipeline](https://dlthub.com/docs/general-usage/pipeline) APIs.
