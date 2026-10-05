(() => {
  const $ = (selector) => document.querySelector(selector);
  const root = $('#chat');
  const initial = JSON.parse($('#chat-state').textContent);
  let session = initial.session;
  let busy = initial.running;
  let submitting = false;
  let statusKnown = true;
  let pollTimer;
  let pollRequest;
  let lastInput = null;
  let activeCall = null;
  let handoffKey = '';
  const toolNames = {web_search: 'Search the web', read_webpage: 'Read documentation', context: 'Inspect connections', write_script: 'Write Python', test_script: 'Test source', finish: 'Check ingestion', ask_user: 'Request input', propose_loading: 'Suggest schedule & loading', propose_validation: 'Request validation', validate_ingestion: 'Validate sample load'};

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }
  function pretty(value) {
    try { return JSON.stringify(JSON.parse(value), null, 2); } catch { return value; }
  }
  function disclosure(key, title, status, content) {
    const details = element('details', `activity ${status}`);
    details.dataset.key = key;
    const summary = element('summary');
    summary.append(element('span', 'activity-dot'), element('span', 'activity-title', title));
    if (status) summary.append(element('span', 'activity-status', {running: 'RUNNING', failed: 'FAILED', complete: 'SUCCESS', waiting: 'IDLE'}[status] || ''));
    details.append(summary, content);
    return details;
  }
  function validationPreview(result) {
    const section = element('section', 'results-preview validation-preview');
    section.setAttribute('aria-label', 'Validation table preview');
    section.append(element('h3', '', 'Validation table preview'));
    const preview = result.preview;
    if (!preview) {
      section.append(element('p', 'hint', result.preview_error));
      return section;
    }
    section.append(element('p', 'hint', `${preview.rows.length} / ${result.second_load_rows} rows · second load`));
    if (preview.rows.length) {
      const scroll = element('div', 'table-scroll');
      scroll.tabIndex = 0;
      scroll.setAttribute('role', 'region');
      scroll.setAttribute('aria-label', 'Validation rows');
      const table = element('table', 'preview-table');
      const head = element('thead');
      const headers = element('tr');
      for (const column of preview.columns) {
        const cell = element('th', '', column);
        cell.scope = 'col';
        headers.append(cell);
      }
      head.append(headers);
      const body = element('tbody');
      for (const row of preview.rows) {
        const cells = element('tr');
        for (const value of row) cells.append(element('td', '', value));
        body.append(cells);
      }
      table.append(head, body); scroll.append(table); section.append(scroll);
    } else {
      section.append(element('p', 'hint', 'No rows.'));
    }
    if (preview.truncated) section.append(element('p', 'hint', 'Values truncated.'));
    return section;
  }
  function renderLog() {
    const log = $('#messages');
    const open = new Set([...log.querySelectorAll('details[open]')].map(node => node.dataset.key));
    const nearBottom = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 160;
    const fragment = document.createDocumentFragment();
    const messages = session.messages || [];
    const lastAssistant = messages.findLastIndex(item => item.role === 'assistant');
    messages.forEach((message, index) => {
      if (message.role === 'system' || message.role === 'tool') return;
      if (message.role === 'user' && message.selection) {
        fragment.append(handoffCard(message.selection.handoff, message.selection.action_id));
      } else if (message.content) {
        if (message.role === 'assistant' && message.tool_calls?.length) {
          fragment.append(disclosure(`thinking-${index}`, 'Thinking', '', element('pre', 'activity-detail', message.content)));
        } else {
          const article = element('article', `log-message ${message.role}`);
          article.append(element('span', 'log-role', message.role === 'user' ? 'You' : 'Pompos'));
          const content = element('div', 'message-content', message.content);
          article.append(content);
          fragment.append(article);
        }
      }
      (message.tool_calls || []).forEach((call, callIndex) => {
        const following = [];
        for (let next = index + 1; next < messages.length && messages[next].role === 'tool'; next++) following.push(messages[next]);
        const result = following.find(item => item.tool_call_id === call.id);
        const name = call.function.name;
        const failed = result?.content.startsWith('Error:');
        const status = result ? (failed ? 'failed' : 'complete') : (busy && index === lastAssistant && activeCall === call.id ? 'running' : 'waiting');
        let title = name === 'read_saved_ingestion' ? 'Load saved ingestion' : toolNames[name] || name;
        let args;
        try { args = JSON.parse(call.function.arguments); } catch { args = {}; }
        if (name === 'web_search' && args.query) title += ` · ${args.query.slice(0, 100)}`;
        if (name === 'read_webpage' && args.url) { try { title += ` · ${new URL(args.url).hostname}`; } catch {} }
        if (name === 'write_script' && args.table) title += ` · ${args.table}`;
        if (name === 'ask_user' && args.kind === 'secret') title = `Request key · ${args.secret_name || 'source credential'}`;
        const isValidation = name === 'validate_ingestion' || name === 'propose_validation';
        if (isValidation && result && validationResult(result.content)) title = 'Validate sample load';
        if ((name === 'test_script' || isValidation) && result && !failed) {
          const match = result.content.match(/"sample_count"\s*:\s*(\d+)/);
          if (match) title += ` · ${match[1]} sample rows`;
        }
        const content = element('div', 'tool-detail');
        if (name === 'write_script' && typeof args.code === 'string') {
          const {code, ...metadata} = args;
          const source = element('code', 'language-python', code);
          const pre = element('pre');
          pre.append(source);
          content.append(element('h3', '', 'Request'), element('pre', '', JSON.stringify(metadata, null, 2)), element('h3', '', 'Python'), pre);
          if (window.hljs) hljs.highlightElement(source);
        } else {
          content.append(element('h3', '', 'Request'), element('pre', '', pretty(call.function.arguments)));
        }
        if (result) content.append(element('h3', '', 'Result'), element('pre', '', pretty(result.content)));
        if (result && !failed && (name === 'web_search' || name === 'read_webpage')) {
          try {
            const research = JSON.parse(result.content);
            const sources = name === 'web_search' ? research.results : [{title: research.title || 'Open documentation', url: research.url}, ...(research.links || [])];
            const links = element('ul', 'research-links');
            for (const source of sources || []) {
              const url = new URL(source.url);
              if (!['https:', 'http:'].includes(url.protocol) || url.username || url.password) continue;
              const link = element('a', '', source.title || url.hostname);
              link.href = url.href; link.target = '_blank'; link.rel = 'noopener noreferrer';
              const row = element('li'); row.append(link); links.append(row);
            }
            content.prepend(links);
          } catch {}
        }
        const row = disclosure(`tool-${index}-${callIndex}`, title, status, content);
        fragment.append(row);
        if (isValidation && result && !failed) {
          const validation = validationResult(result.content);
          if (validation?.preview || validation?.preview_error) fragment.append(validationPreview(validation));
        }
      });
    });
    if (busy && session.pending && lastInput?.action_id && lastInput.handoff_id === session.pending.id) {
      const pending = {...session.pending, loading: lastInput.loading || session.pending.loading};
      fragment.append(handoffCard(pending, lastInput.action_id));
    }
    log.replaceChildren(fragment);
    log.querySelectorAll('details').forEach(node => { node.open = open.has(node.dataset.key); });
    $('#chat-empty').hidden = messages.length > 0 || busy;
    if (nearBottom) window.scrollTo({top: document.documentElement.scrollHeight, behavior: 'instant'});
  }
  function button(label, action) {
    const node = element('button', 'suggestion', label);
    node.type = 'button'; node.addEventListener('click', action);
    return node;
  }
  function loadingCard(pending) {
    const options = pending.loading;
    const files = ['update', 'skip'].includes(options.strategy);
    const form = element('form', 'loading-options');
    const presetLabel = element('label', '', 'Frequency');
    const preset = element('select');
    const presets = [['', 'Manual'], ['0 * * * *', 'Hourly'], ['0 6 * * *', 'Daily at 06:00 UTC'], ['0 6 * * 1', 'Mondays at 06:00 UTC'], ['custom', 'Custom cron']];
    for (const [value, label] of presets) { const option = element('option', '', label); option.value = value; preset.append(option); }
    preset.value = presets.some(([value]) => value === options.cron) ? options.cron : 'custom';
    presetLabel.append(preset);
    const cronLabel = element('label', '', 'Cron · UTC');
    const cron = element('input'); cron.value = options.cron; cron.placeholder = 'Manual'; cron.maxLength = 120; cron.autocomplete = 'off'; cronLabel.append(cron);
    preset.addEventListener('change', () => { if (preset.value !== 'custom') cron.value = preset.value; else cron.focus(); });
    cron.addEventListener('input', () => { preset.value = presets.some(([value]) => value === cron.value) ? cron.value : 'custom'; });
    const strategyLabel = element('label', '', 'Loading strategy');
    const strategy = element('select');
    const strategies = files ? [['update', 'Update changed files'], ['skip', 'Skip existing files']] : [['replace', 'Replace — current snapshot'], ['append', 'Append — keep previous rows'], ['merge', 'Merge — update matching keys']];
    for (const [value, label] of strategies) { const option = element('option', '', label); option.value = value; strategy.append(option); }
    strategy.value = options.strategy; strategyLabel.append(strategy);
    const keysLabel = element('label', '', 'Row keys · comma-separated');
    const keys = element('input'); keys.value = options.primary_key?.join(', ') || ''; keys.placeholder = 'e.g. id'; keys.maxLength = 500; keysLabel.append(keys);
    const updateStrategy = () => {
      keys.required = strategy.value === 'merge';
    };
    strategy.addEventListener('change', updateStrategy); updateStrategy();
    const submitButton = element('button', 'primary', 'Use these settings'); submitButton.type = 'submit';
    form.append(presetLabel, cronLabel, strategyLabel);
    if (!files) form.append(keysLabel);
    if (files) form.append(element('p', 'hint', 'Files missing from the source are retained. Without a source version, Update downloads again.'));
    form.append(submitButton);
    form.addEventListener('submit', event => {
      event.preventDefault();
      submit({action_id: 'accept_loading', handoff_id: pending.id, loading: {cron: cron.value.trim(), strategy: strategy.value, primary_key: files ? [] : keys.value.split(',').map(value => value.trim()).filter(Boolean)}});
    });
    return form;
  }
  function validationCard(pending) {
    const container = element('div', 'loading-card');
    const summary = element('dl', 'loading-summary');
    const limit = pending.validation.limit;
    const strategy = (pending.loading || session.loading)?.strategy;
    const files = ['update', 'skip'].includes(strategy);
    const bytes = pending.validation.max_bytes || 0;
    const seconds = pending.validation.timeout_seconds || 0;
    const sample = `${limit ? `Up to ${limit.toLocaleString('en-US')}` : 'All'} ${files ? 'files' : 'rows'}`;
    for (const [label, text] of [
      ['Validation sample', sample],
      ...(files ? [['Download budget', bytes ? `${(bytes / 1024 / 1024).toLocaleString('en-US')} MiB total` : 'Unlimited']] : []),
      ['Timeout', seconds ? `${seconds}s` : 'None'],
      ['Loading check', `${strategy} · 2 sample loads`],
      ['Test destination', files ? 'Temporary files + DuckDB catalog' : 'Temporary DuckDB'],
    ]) {
      const row = element('div'); row.append(element('dt', '', label), element('dd', '', text)); summary.append(row);
    }
    if (pending.validation.min_count) {
      const row = element('div');
      row.append(element('dt', '', 'Required sample'), element('dd', '', `At least ${pending.validation.min_count.toLocaleString('en-US')} ${files ? 'files' : 'rows'}`));
      summary.append(row);
    }
    container.append(summary);
    if (strategy === 'append') container.append(element('p', 'hint', 'Append can duplicate rows on repeated runs.'));
    return container;
  }
  function handoffCard(pending, selectedAction = '') {
    const target = element('section', 'chat-handoff');
    target.append(element('p', 'handoff-prompt', pending.prompt));
    if (pending.kind === 'loading' && pending.loading) target.append(loadingCard(pending));
    if (pending.kind === 'validation' && pending.validation) target.append(validationCard(pending));
    if (pending.kind === 'secret' && !selectedAction) {
      const form = element('form', 'inline-secret');
      const nameLabel = element('label', '', 'Secret name');
      const name = element('input'); name.value = pending.secret_name || ''; name.required = true; name.maxLength = 200; name.autocomplete = 'off'; nameLabel.append(name);
      const valueLabel = element('label', '', 'Key');
      const value = element('input'); value.type = 'password'; value.required = true; value.maxLength = 16000; value.autocomplete = 'new-password'; valueLabel.append(value);
      const save = element('button', '', 'Save key & retry'); save.type = 'submit';
      const error = element('p', 'secret-error'); error.setAttribute('role', 'alert');
      form.append(nameLabel, valueLabel, save, error);
      form.addEventListener('submit', async (event) => {
        event.preventDefault(); save.disabled = true; error.textContent = '';
        try {
          const response = await fetch(`/chat/${root.dataset.id}/secret`, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({name: name.value, value: value.value, handoff_id: pending.id})});
          if (!response.ok) throw new Error(await response.text());
          value.value = '';
          await submit({action_id: 'retry_secret', handoff_id: pending.id});
        } catch (err) { error.textContent = err.message; }
        finally { save.disabled = false; }
      });
      target.append(form);
    }
    const actions = element('div', 'quick-actions');
    for (const action of pending.actions) {
      if (pending.kind === 'loading' && pending.loading && action.id === 'accept_loading') continue;
      const option = button(action.label, () => submit({action_id: action.id, handoff_id: pending.id}));
      if (selectedAction) option.setAttribute('aria-pressed', String(action.id === selectedAction));
      actions.append(option);
    }
    if (actions.hasChildNodes()) target.append(actions);
    if (selectedAction) {
      target.querySelectorAll('button, input, select').forEach(control => { control.disabled = true; });
      const loadingSubmit = target.querySelector('.loading-options button');
      if (loadingSubmit) {
        loadingSubmit.classList.remove('primary');
        loadingSubmit.setAttribute('aria-pressed', String(selectedAction === 'accept_loading'));
      }
    }
    return target;
  }
  function renderHandoff() {
    const target = $('#handoff');
    if (busy || submitting || !statusKnown || session.ready) { target.hidden = true; return; }
    const pending = session.pending;
    const key = JSON.stringify(pending || null) + (session.messages?.length || 0);
    if (key === handoffKey) { target.hidden = !target.hasChildNodes(); return; }
    handoffKey = key; target.replaceChildren();
    if (pending) target.append(handoffCard(pending));
    target.hidden = !target.hasChildNodes();
  }
  function render() {
    renderLog(); renderHandoff();
    $('#chat-title').textContent = session.edit && !session.published_id ? 'Edit ingestion' : session.messages?.length ? 'Ingestion chat' : 'New ingestion';
    $('#draft').hidden = !session.draft || !!session.published_id;
    $('#draft-target').textContent = session.draft ? `${session.draft.destination} / ${session.draft.schema || 'main'} / ${session.draft.table} · ${session.draft.strategy} · ${session.draft.schedule ? `${session.draft.schedule} UTC` : 'manual'}` : '';
    const draftCode = $('#draft-code');
    draftCode.textContent = session.draft?.code || '';
    delete draftCode.dataset.highlighted;
    if (window.hljs && draftCode.textContent) hljs.highlightElement(draftCode);
    $('#validation-result').hidden = !session.validation || !!session.published_id;
    if (session.validation) {
      const result = session.validation.result;
      const unit = result.data === 'files' ? 'objects' : 'rows';
      $('#validation-result').textContent = `Validation passed · ${result.sample_count} source ${unit} · loads: ${result.first_load_rows} → ${result.second_load_rows} ${unit}`;
    }
    $('#publish').hidden = busy || submitting || !statusKnown || !session.ready || !!session.published_id;
    $('#publish-status').textContent = session.loading?.cron ? `Schedule on save: ${session.loading.cron} · UTC` : 'Manual';
    $('#send').disabled = busy || submitting || !statusKnown;
    $('#send').classList.toggle('primary', !session.ready || !!session.published_id);
    $('#message').disabled = busy || submitting || !statusKnown;
    const saved = session.saved_ingestions || [];
    $('#publish button').textContent = session.edit ? 'Review changes' : 'Save ingestion';
    $('#publish').method = session.edit ? 'get' : 'post';
    $('#publish').action = `/chat/${root.dataset.id}/${session.edit ? 'review' : 'publish'}`;
    $('#published').hidden = !saved.length;
    const links = saved.map(ingestion => {
      const item = element('li');
      const link = element('a', '', `${ingestion.name} · ${ingestion.destination}/${ingestion.schema || 'main'}/${ingestion.table} →`);
      link.href = `/ingestions/${ingestion.id}`;
      item.append(link);
      return item;
    });
    $('#saved-ingestions').replaceChildren(...links);
  }
  function applyStatus(status) {
    session = status.session;
    busy = status.running;
    statusKnown = true;
    activeCall = busy && status.activity?.type === 'tool_start' ? status.activity.call.id : null;
    let label = busy ? 'RUNNING' : status.error ? 'FAILED' : 'IDLE';
    if (busy && status.activity?.type === 'thinking') label += ' · Thinking';
    if (activeCall) label += ` · ${toolNames[status.activity.call.function.name] || status.activity.call.function.name}`;
    $('#working').textContent = label;
    $('#chat-error').textContent = status.error || '';
    $('#chat-error').hidden = !status.error;
    $('#recovery').hidden = busy || !status.error;
    render();
  }

  async function pollStatus() {
    clearTimeout(pollTimer);
    if (pollRequest || submitting || document.hidden) return;
    const controller = new AbortController();
    pollRequest = controller;
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetch(`/chat/${root.dataset.id}/status`, {cache: 'no-store', signal: controller.signal});
      if (!response.ok || response.redirected) throw new Error('Chat status unavailable');
      const status = await response.json();
      if (pollRequest === controller && !submitting && !controller.signal.aborted) applyStatus(status);
    } catch {
      if (!submitting && !document.hidden && pollRequest === controller) {
        statusKnown = false;
        $('#chat-error').textContent = 'Chat update failed. Reconnecting…';
        $('#chat-error').hidden = false;
        $('#recovery').hidden = true;
        render();
      }
    } finally {
      clearTimeout(timeout);
      if (pollRequest === controller) {
        pollRequest = null;
        if (!submitting && !document.hidden) pollTimer = setTimeout(pollStatus, busy ? 1000 : 5000);
      }
    }
  }
  async function submit(input) {
    if (busy || submitting || !statusKnown) return;
    clearTimeout(pollTimer);
    pollRequest?.abort();
    pollRequest = null;
    lastInput = input; busy = true; submitting = true;
    $('#working').textContent = 'RUNNING';
    $('#chat-error').hidden = true; $('#recovery').hidden = true;
    render();
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetch(`/chat/${root.dataset.id}`, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(input), signal: controller.signal});
      if (!response.ok) throw new Error(await response.text());
      applyStatus(await response.json());
      $('#message').value = '';
    } catch (error) {
      // A lost response does not prove the turn failed to start. Read its status
      // before enabling another submission; never resend a message automatically.
      statusKnown = false;
      $('#chat-error').textContent = error.message; $('#chat-error').hidden = false;
      $('#recovery').hidden = true;
    } finally {
      clearTimeout(timeout);
      submitting = false;
      render();
      pollTimer = setTimeout(pollStatus, 0);
    }
  }
  $('#chat-form').addEventListener('submit', (event) => {
    event.preventDefault();
    const message = $('#message').value.trim();
    if (message) submit({message});
  });
  $('#message').addEventListener('keydown', (event) => {
    if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) { event.preventDefault(); $('#chat-form').requestSubmit(); }
  });
  $('#example').addEventListener('click', () => { $('#message').value = 'Ingest individual GitHub stargazers from https://github.com/aleda145/kavla'; $('#message').focus(); });
  $('#retry-turn').addEventListener('click', () => submit(session.pending && lastInput?.action_id ? lastInput : {message: 'Continue from the last saved step. Retry if needed.'}));
  document.addEventListener('visibilitychange', () => {
    clearTimeout(pollTimer);
    if (!document.hidden) pollStatus();
  });
  window.addEventListener('pagehide', () => {
    clearTimeout(pollTimer);
    pollRequest?.abort();
  });
  window.addEventListener('pageshow', (event) => { if (event.persisted) pollStatus(); });
  applyStatus(initial);
  pollTimer = setTimeout(pollStatus, 0);
})();
