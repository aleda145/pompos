const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {runInNewContext} = require('node:vm');

function mount(fetch) {
  const attributes = new Map();
  let click, timeout;
  const panel = {
    dataset: {previewUrl: '/ingestions/rows/preview'},
    innerHTML: '',
    getAttribute: name => attributes.get(name),
    setAttribute: (name, value) => attributes.set(name, value),
    addEventListener: (_, listener) => { click = listener; },
  };
  runInNewContext(readFileSync(`${__dirname}/preview.js`, 'utf8'), {
    document: {querySelector: () => panel, addEventListener: () => {}}, fetch, AbortController,
    setTimeout: callback => { timeout = callback; return 1; },
    clearTimeout: () => { timeout = null; },
  });
  return {
    panel,
    retry: () => click({target: {closest: () => true}}),
    expire: () => timeout(),
    hasTimer: () => timeout !== null,
  };
}

const settle = () => new Promise(resolve => setImmediate(resolve));

test('preview loads independently and inserts the returned fragment', async () => {
  let resolveFetch, calls = 0;
  const view = mount((url, options) => {
    calls++;
    assert.equal(url, '/ingestions/rows/preview');
    assert.equal(options.cache, 'no-store');
    assert.equal(options.redirect, 'error');
    return new Promise(resolve => { resolveFetch = resolve; });
  });
  assert.match(view.panel.innerHTML, /Loading preview/);
  assert.equal(view.panel.getAttribute('aria-busy'), 'true');
  view.retry();
  assert.equal(calls, 1);
  resolveFetch({ok: true, text: async () => '<h2>Table preview</h2><table></table>'});
  await settle();
  assert.match(view.panel.innerHTML, /<table>/);
  assert.equal(view.panel.getAttribute('aria-busy'), 'false');
  assert.equal(view.hasTimer(), false);
});

test('HTTP and network failures allow manual retry without automatic requests', async () => {
  for (const failure of ['http', 'network', 'fragment']) {
    let calls = 0;
    const view = mount(async () => {
      calls++;
      if (calls === 1) {
        if (failure === 'network') throw new Error('offline');
        if (failure === 'http') return {ok: false};
        return {ok: true, text: async () => '<p>Preview unavailable.</p><button data-preview-retry>Retry</button>'};
      }
      return {ok: true, text: async () => '<p>No rows.</p>'};
    });
    await settle();
    assert.match(view.panel.innerHTML, /data-preview-retry/);
    assert.equal(calls, 1);
    view.retry();
    await settle();
    assert.equal(calls, 2);
    assert.match(view.panel.innerHTML, /No rows/);
  }
});

test('a stalled request is aborted and exposes retry', async () => {
  const view = mount((_, {signal}) => new Promise((resolve, reject) => {
    signal.addEventListener('abort', () => reject(new Error('timeout')));
  }));
  view.expire();
  await settle();
  assert.match(view.panel.innerHTML, /data-preview-retry/);
  assert.equal(view.panel.getAttribute('aria-busy'), 'false');
  assert.equal(view.hasTimer(), false);
});
