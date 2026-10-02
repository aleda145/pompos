const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readChatEvents, validationResult} = require('./chat-stream.js');
const encoder = new TextEncoder();

test('steps arrive before completion, even across split Unicode and lines', async () => {
  let controller;
  const body = new ReadableStream({start(value) { controller = value; }});
  const events = [];
  let first;
  const received = new Promise(resolve => { first = resolve; });
  const reading = readChatEvents(body, event => { events.push(event); first(); });
  const data = encoder.encode('{"type":"message","text":"key → source"}\n');
  const arrow = data.indexOf(0xe2);
  controller.enqueue(data.slice(0, arrow + 1));
  controller.enqueue(data.slice(arrow + 1));
  await received;
  assert.deepEqual(events, [{type: 'message', text: 'key → source'}]);
  controller.enqueue(encoder.encode('{"type":"tool_sta'));
  controller.enqueue(encoder.encode('rt"}\n{"type":"done"}'));
  controller.close();
  await reading;
  assert.deepEqual(events.map(event => event.type), ['message', 'tool_start', 'done']);
});

test('an interrupted turn keeps its events but is not treated as completed', async () => {
  const events = [];
  const body = new ReadableStream({start(controller) { controller.enqueue(encoder.encode('{"type":"thinking"}\n')); controller.close(); }});
  await assert.rejects(readChatEvents(body, event => events.push(event)), /Connection interrupted/);
  assert.equal(events.length, 1);
});

test('a rejected event cancels the stream and releases the reader', async () => {
  let cancelled = false;
  const body = new ReadableStream({start(controller) { controller.enqueue(encoder.encode('{"type":"done","error":"Model offline"}\n')); }, cancel() { cancelled = true; }});
  await assert.rejects(readChatEvents(body, event => { throw new Error(event.error); }), /Model offline/);
  assert.equal(cancelled, true);
  assert.equal(body.locked, false);
});

test('validation previews are recovered from saved tool output, including legacy results', () => {
  const preview = {columns: ['id'], rows: [['1234567890123456789']], has_more: true};
  const result = {sample_count: 100, preview};
  assert.deepEqual(validationResult(`loading log\nPOMPOS_VALIDATION_RESULT=${JSON.stringify(result)}\n`), result);
  assert.deepEqual(validationResult('POMPOS_VALIDATION_RESULT={"sample_count":1}'), {sample_count: 1});
  assert.equal(validationResult('Error: failed'), null);
  assert.equal(validationResult('POMPOS_VALIDATION_RESULT={broken'), null);
});
