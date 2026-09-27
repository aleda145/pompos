(() => {
  const $ = (selector) => document.querySelector(selector);
  const root = $('#chat');
  let session = JSON.parse($('#chat-state').textContent);
  let busy = false;
  let thinking = false;
  let lastInput = null;
  let activeCall = null;
  let handoffKey = '';
  const toolNames = {context: 'Inspect connections', write_script: 'Write Python', test_script: 'Test source', finish: 'Check ingestion', ask_user: 'Request input'};

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
    if (status) summary.append(element('span', 'activity-status', {running: 'running', failed: 'failed', complete: 'done', waiting: 'queued'}[status] || ''));
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
        if (name === 'write_script' && args.table) title += ` · ${args.table}`;
        if (name === 'ask_user' && args.kind === 'secret') title = `Request key · ${args.secret_name || 'source credential'}`;
        if (name === 'test_script' && result && !failed) {
          const match = result.content.match(/"sample_count"\s*:\s*(\d+)/);
          if (match) title += ` · ${match[1]} sample rows`;
        }
        const content = element('div', 'tool-detail');
        content.append(element('h3', '', 'Request'), element('pre', '', pretty(call.function.arguments)));
        if (result) content.append(element('h3', '', 'Result'), element('pre', '', pretty(result.content)));
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
  function renderHandoff() {
    const target = $('#handoff');
    if (busy || session.published_id || session.ready) { target.hidden = true; return; }
    const pending = session.pending;
    const key = JSON.stringify(pending || null) + (session.messages?.length || 0);
    if (key === handoffKey) { target.hidden = !target.hasChildNodes(); return; }
    handoffKey = key; target.replaceChildren();
    if (pending) {
      target.append(element('p', 'handoff-prompt', pending.prompt));
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
    } else if (session.messages?.at(-1)?.role === 'assistant') {
      const actions = element('div', 'quick-actions');
      actions.append(button('Continue', () => submit({message: 'Continue from the latest results.'})), button('Tell me more', () => submit({message: 'Tell me more about the last response and the next step.'})));
      target.append(actions);
    }
    target.hidden = !target.hasChildNodes();
  }
  function render() {
    renderLog(); renderHandoff();
    $('#chat-title').textContent = session.draft?.name || (session.messages?.length ? 'Ingestion session' : 'What do you want to bring in?');
    $('#draft').hidden = !session.draft;
    $('#draft-target').textContent = session.draft ? `${session.draft.destination} / ${session.draft.table} · ${session.draft.strategy}` : '';
    $('#draft-code').textContent = session.draft?.code || '';
    $('#publish').hidden = busy || !session.ready || !!session.published_id;
    $('#send').disabled = busy || !!session.published_id;
    $('#message').disabled = busy || !!session.published_id;
    $('#published').hidden = !session.published_id;
    if (session.published_id) $('#published a').href = `/ingestions/${session.published_id}`;
  }
  function applyEvent(event) {
    if (event.type === 'thinking') { thinking = true; activeCall = null; $('#working').textContent = 'Thinking'; }
    if (event.type === 'message') {
      thinking = false;
      session.messages ||= [];
      session.messages.push(event.message);
      if (event.message.role === 'user') $('#message').value = '';
    }
    if (event.type === 'tool_start') { thinking = false; activeCall = event.call.id; $('#working').textContent = toolNames[event.call.function.name] || event.call.function.name; }
    if (event.type === 'done') {
      session = event.session; thinking = false; activeCall = null;
      if (event.error) throw new Error(event.error);
    }
    renderLog();
  }
  async function submit(input) {
    if (busy) return;
    lastInput = input; busy = true; thinking = true;
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
      busy = false; thinking = false; $('#working').textContent = 'Your turn'; render();
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
