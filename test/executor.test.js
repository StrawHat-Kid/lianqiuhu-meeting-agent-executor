const http = require('node:http');
const { EventEmitter } = require('node:events');
const test = require('node:test');
const assert = require('node:assert/strict');
const { readConfig } = require('../src/config');
const { createLogger, formatBeijingTimestamp } = require('../src/logger');
const { createRuisiClient } = require('../src/ruisi-client');
const { PollingRunner } = require('../src/polling-runner');
const script = require('../src/scripts/messages');
const { TO, start, installShutdownHandlers } = require('../src');

const TEST_AGENT = 'test-meeting-agent';
const TEST_SCRIPT = Object.freeze({
  initialDelayMs: 0,
  defaultMessageIntervalMs: 40000,
  paragraphIntervalMs: 720000,
  paragraphs: Object.freeze([
    Object.freeze({ id: 'A', messages: Object.freeze(['segment A-1', 'segment A-2']) }),
    Object.freeze({ id: 'B', messages: Object.freeze(['segment B-1']) })
  ])
});

function loggerCapture() {
  const entries = [];
  return {
    entries,
    info: (message, details) => entries.push({ level: 'info', message, details }),
    error: (message, details) => entries.push({ level: 'error', message, details })
  };
}

// 注入的 fake sleep 不使用真实 setTimeout；所有节奏测试均即时推进时间。
function fakeSleep(events) {
  return async (ms) => { events.push(ms); return true; };
}

function createRunner(options = {}) {
  return new PollingRunner({
    agent: options.agent || TEST_AGENT,
    to: TO,
    script: options.script || TEST_SCRIPT,
    client: options.client || { sendMessage: async () => ({ ok: true, status: 200 }) },
    logger: options.logger || loggerCapture(),
    sleep: options.sleep,
    subjectName: '会议智能体'
  });
}

async function mockIngress(handler) {
  const instance = http.createServer(handler);
  await new Promise((resolve) => instance.listen(0, '127.0.0.1', resolve));
  return {
    url: `http://127.0.0.1:${instance.address().port}/agent/send`,
    close: () => new Promise((resolve) => instance.close(resolve))
  };
}

function readJson(request) {
  return new Promise((resolve, reject) => {
    let text = '';
    request.setEncoding('utf8');
    request.on('data', (chunk) => { text += chunk; });
    request.on('end', () => {
      try { resolve(JSON.parse(text)); } catch (error) { reject(error); }
    });
    request.on('error', reject);
  });
}

function createTestConfig(port) {
  return {
    agent: TEST_AGENT,
    port,
    ingressHost: '127.0.0.1',
    ingressPort: 29876,
    ingressToken: 'test-token',
    ingressTimeoutMs: 1000,
    ingressUrl: 'http://127.0.0.1:29876/agent/send'
  };
}

async function bindPort(port) {
  const instance = http.createServer();
  await new Promise((resolve, reject) => {
    instance.once('error', reject);
    instance.listen(port, () => {
      instance.removeListener('error', reject);
      resolve();
    });
  });
  return instance;
}

async function requestHealth(port) {
  return new Promise((resolve, reject) => {
    const request = http.get({ host: '127.0.0.1', port, path: '/health' }, (response) => {
      response.setEncoding('utf8');
      let body = '';
      response.on('data', (chunk) => { body += chunk; });
      response.on('end', () => resolve({ statusCode: response.statusCode, body }));
    });
    request.on('error', reject);
  });
}

test('配置读取 PORT=18035，AGENT 缺失仍按既有规则失败', () => {
  const baseEnv = { AGENT: TEST_AGENT, INGRESS_HOST: '127.0.0.1', INGRESS_PORT: '29876', INGRESS_TOKEN: 'test' };
  assert.deepEqual(readConfig(baseEnv), {
    agent: TEST_AGENT, port: 18035, ingressHost: '127.0.0.1', ingressPort: 29876,
    ingressToken: 'test', ingressTimeoutMs: 5000, ingressUrl: 'http://127.0.0.1:29876/agent/send'
  });
  assert.equal(readConfig({ ...baseEnv, PORT: '18035' }).port, 18035);
  assert.throws(() => readConfig({ INGRESS_HOST: '127.0.0.1', INGRESS_PORT: '29876', INGRESS_TOKEN: 'test' }), /AGENT/);
  assert.throws(() => readConfig({ ...baseEnv, PORT: 'invalid' }), /PORT/);
});

test('initialDelay=0 时立即发送；段内 40 秒、段尾 12 分钟均由 fake timer 推进', async () => {
  const events = [];
  const runner = createRunner({
    client: { sendMessage: async ({ body }) => { events.push(`send:${body}`); return { ok: true, status: 200 }; } },
    sleep: fakeSleep(events)
  });
  await runner.run({ maxRounds: 1 });
  assert.deepEqual(events, ['send:segment A-1', 40000, 'send:segment A-2', 720000, 'send:segment B-1', 720000]);
});

test('最后一段的 12 分钟等待后回到第 1 段', async () => {
  const events = [];
  const runner = createRunner({
    client: { sendMessage: async ({ paragraphId }) => { events.push(`send:${paragraphId}`); return { ok: true, status: 200 }; } },
    sleep: fakeSleep(events)
  });
  await runner.run({ maxRounds: 2 });
  assert.deepEqual(events, ['send:A', 40000, 'send:A', 720000, 'send:B', 720000, 'send:A', 40000, 'send:A', 720000, 'send:B', 720000]);
});

test('AGENT、to、ingress 和 token 沿配置链进入 /agent/send 请求体与请求头', async () => {
  const requests = [];
  const ingress = await mockIngress(async (request, response) => {
    requests.push({ method: request.method, headers: request.headers, body: await readJson(request) });
    response.writeHead(200, { 'Content-Type': 'application/json' }).end('{"status":"ok"}');
  });
  try {
    const client = createRuisiClient({ ingressUrl: ingress.url, authToken: 'test-token', timeoutMs: 1000 });
    await createRunner({ client, sleep: fakeSleep([]) }).run({ maxRounds: 1 });
    assert.deepEqual(requests.map(({ body }) => body), [
      { agent: TEST_AGENT, to: TO, body: 'segment A-1', groupchat: false },
      { agent: TEST_AGENT, to: TO, body: 'segment A-2', groupchat: false },
      { agent: TEST_AGENT, to: TO, body: 'segment B-1', groupchat: false }
    ]);
    assert.ok(requests.every((request) => request.method === 'POST' && request.headers['x-auth-token'] === 'test-token'));
  } finally {
    await ingress.close();
  }
});

test('正式会议内容为空时 Runner 空闲、不调用 RUISI、不空转，stop 后正常结束', async () => {
  const log = loggerCapture();
  let sent = 0;
  const runner = createRunner({
    script,
    logger: log,
    client: { sendMessage: async () => { sent += 1; return { ok: true, status: 200 }; } }
  });
  const running = runner.run();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(sent, 0);
  assert.equal(log.entries.filter((entry) => entry.message.includes('暂无正式轮询内容')).length, 1);
  runner.stop();
  await running;
});

test('HTTP 服务实际监听传入配置端口，GET /health 返回 200，且 AGENT 传入 Runner', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));
  let runnerOptions;
  let clientOptions;
  const runner = {
    run: () => new Promise(() => {}),
    stop: () => {}
  };
  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: (options) => { clientOptions = options; return {}; },
    createPollingRunnerFn: (options) => { runnerOptions = options; return runner; }
  });
  try {
    assert.equal(running.server.address().port, port);
    assert.equal(running.config.agent, TEST_AGENT);
    assert.equal(runnerOptions.agent, TEST_AGENT);
    assert.equal(runnerOptions.to, TO);
    assert.deepEqual(
      { ingressHost: clientOptions.ingressHost, ingressPort: clientOptions.ingressPort, authToken: clientOptions.authToken, timeoutMs: clientOptions.timeoutMs, ingressUrl: clientOptions.ingressUrl },
      { ingressHost: '127.0.0.1', ingressPort: 29876, authToken: 'test-token', timeoutMs: 1000, ingressUrl: 'http://127.0.0.1:29876/agent/send' }
    );
    assert.deepEqual(await requestHealth(port), { statusCode: 200, body: JSON.stringify({ status: 'ok' }) });
  } finally {
    // The fake Runner's unresolved promise is intentional for this server-lifecycle test.
    running.runPromise = Promise.resolve();
    await new Promise((resolve) => running.server.close(resolve));
  }
});

test('graceful shutdown 停止空内容 Runner 并释放实际监听端口', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));
  const running = await start({ logger: loggerCapture(), loadConfigFn: () => createTestConfig(port) });
  const processRef = new EventEmitter();
  const handlers = installShutdownHandlers({ running, logger: loggerCapture(), processRef });
  processRef.emit('SIGTERM');
  await handlers.shutdown('SIGTERM');
  assert.equal(running.runner.stopped, true);
  const rebound = await bindPort(port);
  await new Promise((resolve) => rebound.close(resolve));
});

test('日志使用北京时间且 token 脱敏', () => {
  assert.equal(formatBeijingTimestamp(new Date('2026-08-25T03:35:05.281Z')), '2026-08-25 11:35:05');
  const lines = [];
  createLogger({ log: (line) => lines.push(line) }).info('test', { ingressToken: 'must-not-appear' });
  assert.doesNotMatch(lines[0], /must-not-appear/);
});
