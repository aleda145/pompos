# Pompos

Describe what you want to ingest, develop it with an agent, and let Go handle execution and scheduling. Every ingestion uses Python and dlt.

One source table = one ingestion YAML = one Python file = one destination data table. Nested values stay in JSON columns; dlt also maintains its own internal metadata tables. Configured destinations currently support DuckDB.

## Start

```bash
make setup
make run
```

Open `http://localhost:8080`, add your model provider key under **Secrets**, then configure **Agent settings** with an OpenAI-compatible base URL (for example `https://api.openai.com/v1`), model ID, and the secret name. Local endpoints can omit authentication. The model must support Chat Completions function/tool calling.

Choose **Add ingestion** and try:

> Ingest individual GitHub stargazers from https://github.com/aleda145/kavla

The agent lists available destinations and source secret names, asks for missing information, writes an extractor, and executes a small source probe. A compact activity log streams each step as it happens. Expand Thinking or a tool row to inspect its progress update, request, or result. Python errors and samples feed back into the model so it can repair its code. Common questions appear as action buttons. Credential requests provide an inline managed-secret form, **I've added a key, try again**, and **Tell me more**. Save a key and retry directly, or update the named secret on the Secrets page and click retry. Choices and pending credential requests survive a reload; free-text chat remains available for anything else. The model-provider key is excluded from source credentials. Do not paste keys into chat.

Each turn allows up to 12 model/tool rounds and four minutes. A source probe has a 45-second timeout and consumes at most five yielded rows. The generated extractor receives `limit=5` so it can also bound network requests. These are application limits, not an operating-system sandbox. A successful, nonempty probe of the current code is required before the agent can mark it ready. This verifies extraction, not whether the full dataset will load successfully.

After inspecting the source, the agent proposes a schedule and loading strategy with a short explanation. Confirm them with **Use these settings**, or adjust the frequency, five-field cron expression, strategy, and row keys in the settings card. All schedules use UTC; blank cron means manual runs only. The agent uses replace for current-state snapshots, merge for updates with stable row keys, or append for events and intentional history, and asks when the desired behavior is unclear. Confirmation is required before saving, and script revisions cannot silently override those choices.

Review the source, destination, schedule, load strategy, Python draft, and sample results. **Save ingestion** creates the Python and YAML files. A confirmed cron schedule becomes active when you save; its first automatic load runs at the next scheduled time. From the detail page, **Run ingestion** queues an immediate full load, and the schedule can also be changed there. Full loads have a 30-minute timeout. Existing run status, retries and scheduler behavior remain in Go. `replace` is the default strategy; `append` and primary-key-based `merge` are also supported for Python jobs.

Conversations and drafts survive restarts under `data/agent/`. Keep the conversation URL to resume it. Published artifacts live under `data/ingestions/<id>.py` and `<id>.yaml`. YAML stores the Python digest and secret references, never secret values. Schedule changes preserve those fields. If the saved Python changes, execution fails its digest check; develop and test a new ingestion through chat to replace it.

## Python contract

The agent supplies a generator:

```python
def fetch(secret, limit):
    # Read credentials with secret("managed-secret-name") when necessary.
    # Make source requests with explicit timeouts and check their status.
    # Yield dictionaries for one entity. Respect limit; paginate when None.
    yield {"id": 1, "name": "example"}
```

Pompos adds a runnable entrypoint that probes `fetch` or loads it through a single dlt resource. No connector catalog is required. Python's standard library, requests, dlt and DuckDB are installed. Extra source libraries must be installed by the operator; the agent has no package-install tool.

Run a saved ingestion from the CLI with the same data directory and interpreter:

```bash
POMPOS_PYTHON_BINARY="$PWD/.venv/bin/python" go run ./cmd/pompos run data/ingestions/<id>.yaml
```

The generated file can also run directly with `POMPOS_PROBE=1`, or with `POMPOS_CONFIG` JSON containing `destination`, `table`, `strategy` and `primary_key`. Supply selected secrets as a `POMPOS_SECRETS` JSON object. Normal Pompos execution prepares these variables from managed storage.

## Runtime and trust

This is a trusted, self-hosted operator tool. Generated Python executes with Pompos's operating-system permissions and network access. Probe mode skips the provided dlt loader, but arbitrary generated code is not sandboxed. Use a dedicated environment and trusted model endpoint. The endpoint receives conversation text, generated code, destination descriptions and source samples. Only explicitly referenced secrets are injected into Python; exact secret values are redacted from captured output. The existing secret store uses SQLite, without encryption at rest. Do not expose the unauthenticated application to untrusted users.

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

Tests cover the iterative repair loop, persistent conversations, test-before-save gating, secret redaction, script digest checks, chat publication, scheduling, and a real dlt load. Provider behavior is tested with a fake OpenAI-compatible endpoint; a real configured model still needs an end-to-end trial.

The runtime uses dlt's [resource](https://dlthub.com/docs/general-usage/resource) and [pipeline](https://dlthub.com/docs/general-usage/pipeline) APIs.
