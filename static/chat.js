const root = document.querySelector('#chat');
const form = document.querySelector('#chat-form');
const errorBox = document.querySelector('#chat-error');
function render(session) {
  const messages = document.querySelector('#messages');
  messages.replaceChildren();
  for (const message of session.messages || []) {
    if (message.role === 'system' || !message.content) continue;
    const article = document.createElement('article');
    article.className = `chat-message ${message.role}`;
    const title = document.createElement('strong');
    title.textContent = message.role === 'tool' ? 'Tool result' : message.role;
    const content = document.createElement('pre');
    content.textContent = message.content;
    article.append(title, content); messages.append(article);
  }
  document.querySelector('#draft').hidden = !session.draft;
  document.querySelector('#draft-target').textContent = session.draft ? `${session.draft.source} → ${session.draft.destination} / ${session.draft.table} · ${session.draft.strategy}` : '';
  document.querySelector('#draft-code').textContent = session.draft?.code || '';
  document.querySelector('#publish').hidden = !session.ready || !!session.published_id;
}
form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const input = document.querySelector('#message');
  const button = document.querySelector('#send');
  const status = document.querySelector('#working');
  const message = input.value.trim();
  if (!message) return;
  button.disabled = true; input.disabled = true;
  document.querySelector('#publish').hidden = true;
  errorBox.hidden = true;
  status.textContent = 'Agent working: investigating, coding, and testing. This can take a few minutes…';
  try {
    const response = await fetch(`/chat/${root.dataset.id}`, {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({message})});
    if (!response.ok) throw new Error(await response.text());
    const result = await response.json();
    render(result.session); input.value = '';
    if (result.error) throw new Error(result.error);
  } catch (error) {
    errorBox.textContent = error.message;
    errorBox.hidden = false;
  } finally {
    button.disabled = false; input.disabled = false; status.textContent = ''; input.focus();
  }
});
