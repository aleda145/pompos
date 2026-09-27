// Shared by the chat client and Node tests; independent of the DOM.
async function readChatEvents(body, onEvent) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let completed = false;
  let ended = false;
  function line(value) {
    if (!value.trim()) return;
    const event = JSON.parse(value);
    if (!event || typeof event.type !== 'string') throw new Error('Invalid activity event.');
    if (event.type === 'done') completed = true;
    onEvent(event);
  }
  try {
    while (true) {
      const {value, done} = await reader.read();
      buffer += decoder.decode(value, {stream: !done});
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const value of lines) line(value);
      if (done) { ended = true; line(buffer); break; }
    }
    if (!completed) throw new Error('Connection interrupted. Reload to recover saved progress before continuing.');
  } finally {
    if (!ended) await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
if (typeof module !== 'undefined') module.exports = {readChatEvents};
