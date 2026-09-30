(() => {
  const $ = (selector) => document.querySelector(selector);
  const root = $('#chat');
  let session = JSON.parse($('#chat-state').textContent);
  let busy = false;
  let thinking = false;
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
  function renderLog() {
    const log = $('#messages');
    const open = new Set([...log.querySelectorAll('details[open]')].map(node => node.dataset.key));
    const nearBottom = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 160;
    const fragment = document.createDocumentFragment();
    const messages = session.messages || [];
    const lastAssistant = messages.findLastIndex(item => item.role === 'assistant');
    messages.forEach((message, index) => {
      if (message.role === 'system' || message.role === 'tool') return;
      if (message.content) {
        if (message.role === 'assistant' && message.tool_calls?.length) {
          fragment.append(disclosure(`thinking-${index}`, 'Thinking', '', element('pre', 'activity-detail', message.content)));
        } else {
          const article = element('article', `log-message ${message.role}`);
          article.append(element('span', 'log-role', message.role === 'user' ? 'You' : 'Pompos'));
          const content = element('div', 'message-content', message.content);
          if (message.role === 'assistant' && (message.content.length > 600 || message.content.split('\n').length > 8)) {
            const details = element('details', 'message-more');
            details.dataset.key = `response-${index}`;
            const preview = message.content.split('\n').find(line => line.trim()) || 'Response';
            details.append(element('summary', '', `${preview.slice(0, 160)}${preview.length > 160 ? '…' : ''} · expand`), content);
            article.append(details);
          } else { article.append(content); }
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
        let title = toolNames[name] || name;
        let args;
        try { args = JSON.parse(call.function.arguments); } catch { args = {}; }
        if (name === 'web_search' && args.query) title += ` · ${args.query.slice(0, 100)}`;
        if (name === 'read_webpage' && args.url) { try { title += ` · ${new URL(args.url).hostname}`; } catch {} }
        if (name === 'write_script' && args.table) title += ` · ${args.table}`;
        if (name === 'ask_user' && args.kind === 'secret') title = `Request key · ${args.secret_name || 'source credential'}`;
        if ((name === 'test_script' || name === 'validate_ingestion') && result && !failed) {
          const match = result.content.match(/"sample_count"\s*:\s*(\d+)/);
          if (match) title += ` · ${match[1]} sample rows`;
        }
        const content = element('div', 'tool-detail');
        content.append(element('h3', '', 'Request'), element('pre', '', pretty(call.function.arguments)));
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
      });
    });
    if (thinking) {
      fragment.append(disclosure('thinking-current', 'Thinking…', 'running', element('p', 'activity-detail', 'Preparing the next step from your request and the latest tool results.')));
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
    const container = element('div', 'loading-card');
    const summary = element('dl', 'loading-summary');
    for (const [label, text] of [
      ['Schedule', options.cron ? `${options.cron} · UTC` : 'Manual only'],
      ['Loading', options.strategy],
      ['Row keys', options.primary_key?.join(', ') || 'None'],
    ]) {
      const row = element('div'); row.append(element('dt', '', label), element('dd', '', text)); summary.append(row);
    }
    container.append(summary);
    const details = element('details', 'loading-adjust');
    details.append(element('summary', '', 'Adjust schedule or loading'));
    const form = element('form', 'loading-options');
    const presetLabel = element('label', '', 'Frequency');
    const preset = element('select');
    const presets = [['', 'Manual only'], ['0 * * * *', 'Hourly'], ['0 6 * * *', 'Daily at 06:00 UTC'], ['0 6 * * 1', 'Mondays at 06:00 UTC'], ['custom', 'Custom cron']];
    for (const [value, label] of presets) { const option = element('option', '', label); option.value = value; preset.append(option); }
    preset.value = presets.some(([value]) => value === options.cron) ? options.cron : 'custom';
    presetLabel.append(preset);
    const cronLabel = element('label', '', 'Cron expression · UTC');
    const cron = element('input'); cron.value = options.cron; cron.placeholder = 'Blank for manual runs'; cron.maxLength = 120; cron.autocomplete = 'off'; cronLabel.append(cron);
    preset.addEventListener('change', () => { if (preset.value !== 'custom') cron.value = preset.value; else cron.focus(); });
    cron.addEventListener('input', () => { preset.value = presets.some(([value]) => value === cron.value) ? cron.value : 'custom'; });
    const strategyLabel = element('label', '', 'Loading strategy');
    const strategy = element('select');
    for (const [value, label] of [['replace', 'Replace — current snapshot'], ['append', 'Append — keep previous rows'], ['merge', 'Merge — update matching keys']]) { const option = element('option', '', label); option.value = value; strategy.append(option); }
    strategy.value = options.strategy; strategyLabel.append(strategy);
    const keysLabel = element('label', '', 'Row keys · comma-separated');
    const keys = element('input'); keys.value = options.primary_key?.join(', ') || ''; keys.placeholder = 'e.g. id'; keys.maxLength = 500; keysLabel.append(keys);
    const explanation = element('p', 'hint');
    const updateStrategy = () => {
      keys.required = strategy.value === 'merge';
      explanation.textContent = {
        replace: 'Replaces the destination table each run. Rows removed from the source disappear.',
        append: 'Keeps existing rows and adds every extracted row. Repeated full extracts can create duplicates.',
        merge: 'Updates or inserts by stable row keys. Rows missing from the source are not deleted.',
      }[strategy.value];
    };
    strategy.addEventListener('change', updateStrategy); updateStrategy();
    const submitButton = element('button', '', 'Use adjusted settings'); submitButton.type = 'submit';
    form.append(presetLabel, cronLabel, strategyLabel, keysLabel, explanation, element('p', 'hint', 'Five cron fields: minute, hour, day, month, weekday. Saving enables the schedule; the first run occurs at its next scheduled time.'), submitButton);
    form.addEventListener('submit', event => {
      event.preventDefault();
      submit({action_id: 'accept_loading', handoff_id: pending.id, loading: {cron: cron.value.trim(), strategy: strategy.value, primary_key: keys.value.split(',').map(value => value.trim()).filter(Boolean)}});
    });
    details.append(form); container.append(details);
    return container;
  }
  function validationCard(pending) {
    const container = element('div', 'loading-card');
    const summary = element('dl', 'loading-summary');
    const estimate = session.estimate;
    const limit = pending.validation.limit;
    const expected = estimate?.rows != null ? Math.min(limit, estimate.rows) : null;
    for (const [label, text] of [
      ['Validation sample', `Up to ${limit.toLocaleString('en-US')} rows${expected != null ? ` · expected ${estimate.kind === 'approximate' ? 'about ' : ''}${expected.toLocaleString('en-US')}` : ''}`],
      ['Production extraction', rowEstimateText(estimate)],
      ['Loading check', `${session.loading?.strategy} · load the same sample twice`],
      ['Test destination', 'Temporary DuckDB database, removed after validation'],
    ]) {
      const row = element('div'); row.append(element('dt', '', label), element('dd', '', text)); summary.append(row);
    }
    container.append(summary, element('p', 'hint', estimate?.basis || 'No reliable source count is available; validation can still proceed.'));
    if (estimate?.observed_at) container.append(element('p', 'hint', `Count observed ${estimate.observed_at}. Production estimates describe extracted rows, not new rows inserted, and may change before a run.`));
    container.append(element('p', 'hint', 'Checks sample loading, row keys and repeat-load counts. The source may return whole API pages. This does not verify the full dataset or access to the production destination. Ask for a different sample size if needed.'));
    if (session.loading?.strategy === 'append') container.append(element('p', 'hint', 'Append intentionally keeps both copies in this test; repeated full extractions can duplicate production rows.'));
    return container;
  }
  function renderHandoff() {
    const target = $('#handoff');
    if (busy || session.ready) { target.hidden = true; return; }
    const pending = session.pending;
    const key = JSON.stringify(pending || null) + (session.messages?.length || 0);
    if (key === handoffKey) { target.hidden = !target.hasChildNodes(); return; }
    handoffKey = key; target.replaceChildren();
    if (pending) {
      target.append(element('p', 'handoff-prompt', pending.prompt));
      if (pending.kind === 'loading' && pending.loading) target.append(loadingCard(pending));
      if (pending.kind === 'validation' && pending.validation) target.append(validationCard(pending));
      if (pending.kind === 'secret') {
        const details = element('details', 'secret-entry');
        details.append(element('summary', '', `Add or update “${pending.secret_name}”`));
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
        details.append(form, element('p', 'hint', 'Stored in managed secrets. The key value is never added to this conversation.'));
        target.append(details);
      }
      const actions = element('div', 'quick-actions');
      for (const action of pending.actions) actions.append(button(action.label, () => submit({action_id: action.id, handoff_id: pending.id})));
      target.append(actions);
    }
    target.hidden = !target.hasChildNodes();
  }
  function render() {
    renderLog(); renderHandoff();
    $('#chat-title').textContent = session.messages?.length ? 'Ingestion chat' : 'New ingestion';
    $('#draft').hidden = !session.draft || !!session.published_id;
    $('#draft-target').textContent = session.draft ? `${session.draft.destination} / ${session.draft.table} · ${session.draft.strategy} · ${session.draft.schedule ? `${session.draft.schedule} UTC` : 'manual'}` : '';
    $('#draft-code').textContent = session.draft?.code || '';
    $('#row-estimate').hidden = !session.draft || !!session.published_id;
    $('#row-estimate').textContent = `Production extraction: ${rowEstimateText(session.estimate)}${session.estimate?.basis ? ` · ${session.estimate.basis}` : ''}${session.estimate?.observed_at ? ` · observed ${session.estimate.observed_at}` : ''}. Future runs may differ.`;
    $('#validation-result').hidden = !session.validation || !!session.published_id;
    if (session.validation) {
      const result = session.validation.result;
      $('#validation-result').textContent = `Validation passed: ${result.sample_count} source rows; ${result.first_load_rows} rows after the first load, ${result.second_load_rows} after replaying the sample. Temporary database removed.`;
    }
    $('#publish').hidden = busy || !session.ready || !!session.published_id;
    $('#publish-status').textContent = session.loading?.cron ? `Validation passed. Saving enables ${session.loading.cron} (UTC) · ${session.loading.strategy}.` : `Validation passed. Manual runs · ${session.loading?.strategy || session.draft?.strategy || 'replace'}.`;
    $('#send').disabled = busy;
    $('#send').classList.toggle('primary', !session.ready || !!session.published_id);
    $('#message').disabled = busy;
    const saved = session.saved_ingestions || [];
    $('#published').hidden = !saved.length;
    const links = saved.map(ingestion => {
      const item = element('li');
      const link = element('a', '', `${ingestion.name} · ${ingestion.table} →`);
      link.href = `/ingestions/${ingestion.id}`;
      item.append(link);
      return item;
    });
    $('#saved-ingestions').replaceChildren(...links);
  }
  function applyEvent(event) {
    if (event.type === 'thinking') { thinking = true; activeCall = null; $('#working').textContent = 'RUNNING · Thinking'; }
    if (event.type === 'message') {
      thinking = false;
      session.messages ||= [];
      session.messages.push(event.message);
      if (event.message.role === 'user') $('#message').value = '';
    }
    if (event.type === 'tool_start') { thinking = false; activeCall = event.call.id; $('#working').textContent = `RUNNING · ${toolNames[event.call.function.name] || event.call.function.name}`; }
    if (event.type === 'done') {
      session = event.session; thinking = false; activeCall = null;
      if (event.error) throw new Error(event.error);
    }
    renderLog();
  }
  async function submit(input) {
    if (busy) return;
    lastInput = input; busy = true; thinking = true;
    $('#working').textContent = 'RUNNING';
    $('#chat-error').hidden = true; $('#recovery').hidden = true;
    render();
    try {
      const response = await fetch(`/chat/${root.dataset.id}`, {method: 'POST', headers: {'Content-Type': 'application/json', Accept: 'application/x-ndjson'}, body: JSON.stringify(input)});
      if (!response.ok) throw new Error(await response.text());
      await readChatEvents(response.body, applyEvent);
    } catch (error) {
      $('#chat-error').textContent = error.message; $('#chat-error').hidden = false;
      $('#recovery').hidden = false;
    } finally {
      busy = false; thinking = false; $('#working').textContent = $('#chat-error').hidden ? 'IDLE' : 'FAILED'; render();
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
  render();
})();
