const http = require('node:http');
const { EventEmitter } = require('node:events');
const test = require('node:test');
const assert = require('node:assert/strict');
const { loadConfig, readConfig } = require('../src/config');
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
const EMPTY_SCRIPT = Object.freeze({
  initialDelayMs: 0,
  defaultMessageIntervalMs: 40000,
  paragraphIntervalMs: 720000,
  paragraphs: Object.freeze([])
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

test('配置读取 PORT=18035，AGENT 缺失时 HTTP Server 和 Runner 均不启动', async () => {
  const baseEnv = { AGENT: TEST_AGENT, INGRESS_HOST: '127.0.0.1', INGRESS_PORT: '29876', INGRESS_TOKEN: 'test' };
  assert.deepEqual(readConfig(baseEnv), {
    agent: TEST_AGENT, port: 18035, ingressHost: '127.0.0.1', ingressPort: 29876,
    ingressToken: 'test', ingressTimeoutMs: 5000, ingressUrl: 'http://127.0.0.1:29876/agent/send'
  });
  assert.equal(readConfig({ ...baseEnv, PORT: '18035' }).port, 18035);
  assert.throws(() => readConfig({ INGRESS_HOST: '127.0.0.1', INGRESS_PORT: '29876', INGRESS_TOKEN: 'test' }), /AGENT/);
  assert.throws(() => readConfig({ ...baseEnv, PORT: 'invalid' }), /PORT/);
  let healthServerCreated = false;
  let runnerCreated = false;
  await assert.rejects(
    start({
      loadConfigFn: () => readConfig({ INGRESS_HOST: '127.0.0.1', INGRESS_PORT: '29876', INGRESS_TOKEN: 'test' }),
      createHealthServerFn: () => { healthServerCreated = true; },
      createPollingRunnerFn: () => { runnerCreated = true; }
    }),
    /AGENT/
  );
  assert.equal(healthServerCreated, false);
  assert.equal(runnerCreated, false);
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

test('正式会议文案为 12 段 28 条，完整周期后第 29 条回到段落01第1条', async () => {
  const sent = [];
  const waits = [];
  const runner = createRunner({
    script,
    client: { sendMessage: async (message) => { sent.push(message); return { ok: true, status: 200 }; } },
    sleep: fakeSleep(waits)
  });
  await runner.run({ maxRounds: 2 });

  const firstCycle = script.paragraphs.flatMap((paragraph) => paragraph.messages.map((body, index) => ({
    paragraphId: paragraph.id, body, messageIndex: index + 1, messageCount: paragraph.messages.length
  })));
  assert.equal(script.paragraphs.length, 12);
  assert.deepEqual(script.paragraphs.map((paragraph) => paragraph.messages.length), [3, 2, 2, 3, 2, 2, 3, 2, 2, 3, 2, 2]);
  assert.equal(firstCycle.length, 28);
  assert.equal(script.paragraphs[0].messages[0], '【会议态势】正在检查园区会议室总量、当前使用状态、空闲资源与近期告警，评估整体会议保障情况。');
  assert.equal(script.paragraphs[3].messages[2], '【调度建议】建议根据不同规模会议的预约趋势优化会议室推荐与分配，减少大空间低人数使用造成的资源占用。');
  assert.equal(script.paragraphs[11].messages[1], '【持续关注】下一轮将继续跟踪会议室使用效率、会议保障质量和重点事项处置进展，持续优化办公会议体验。');
  assert.deepEqual(
    sent.slice(0, 29).map(({ paragraphId, body, messageIndex, messageCount }) => ({ paragraphId, body, messageIndex, messageCount })),
    [...firstCycle, firstCycle[0]]
  );
  assert.equal(waits.filter((ms) => ms === 40000).length, 32);
  assert.equal(waits.filter((ms) => ms === 720000).length, 24);
});

test('process.env AGENT、ingress、to 沿完整配置链进入 /agent/send 请求体与请求头', async () => {
  const requests = [];
  const ingress = await mockIngress(async (request, response) => {
    requests.push({ method: request.method, headers: request.headers, body: await readJson(request) });
    response.writeHead(200, { 'Content-Type': 'application/json' }).end('{"status":"ok"}');
  });
  const envKeys = ['AGENT', 'PORT', 'INGRESS_HOST', 'INGRESS_PORT', 'INGRESS_TOKEN', 'INGRESS_TIMEOUT_MS'];
  const savedEnv = Object.fromEntries(envKeys.map((key) => [key, process.env[key]]));
  try {
    const ingressUrl = new URL(ingress.url);
    Object.assign(process.env, {
      AGENT: TEST_AGENT,
      PORT: '18035',
      INGRESS_HOST: ingressUrl.hostname,
      INGRESS_PORT: ingressUrl.port,
      INGRESS_TOKEN: 'test-token',
      INGRESS_TIMEOUT_MS: '1000'
    });
    const config = loadConfig();
    assert.equal(config.agent, TEST_AGENT);
    assert.equal(config.ingressUrl, ingress.url);
    const client = createRuisiClient({ ...config, authToken: config.ingressToken, timeoutMs: config.ingressTimeoutMs });
    await createRunner({ agent: config.agent, client, sleep: fakeSleep([]) }).run({ maxRounds: 1 });
    assert.deepEqual(requests.map(({ body }) => body), [
      { agent: TEST_AGENT, to: TO, body: 'segment A-1', groupchat: false },
      { agent: TEST_AGENT, to: TO, body: 'segment A-2', groupchat: false },
      { agent: TEST_AGENT, to: TO, body: 'segment B-1', groupchat: false }
    ]);
    assert.ok(requests.every((request) => request.method === 'POST' && request.headers['x-auth-token'] === 'test-token'));
  } finally {
    for (const key of envKeys) {
      if (savedEnv[key] === undefined) delete process.env[key]; else process.env[key] = savedEnv[key];
    }
    await ingress.close();
  }
});

test('正式会议内容为空时 Runner 经 fake timer 推进后仍空闲、不调用 RUISI、不空转，stop 后正常结束', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const log = loggerCapture();
  let sent = 0;
  const runner = createRunner({
    script: EMPTY_SCRIPT,
    logger: log,
    client: { sendMessage: async () => { sent += 1; return { ok: true, status: 200 }; } }
  });
  const running = runner.run();
  await new Promise((resolve) => setImmediate(resolve));
  t.mock.timers.tick(43200000);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(sent, 0);
  assert.equal(log.entries.filter((entry) => entry.message.includes('暂无正式轮询内容')).length, 1);
  runner.stop();
  await running;
});

test('HTTP 服务实际监听配置 PORT=18035，GET /health 返回 200，且 AGENT 传入 Runner', async () => {
  const port = 18035;
  const probe = await bindPort(port);
  await new Promise((resolve) => probe.close(resolve));
  let runnerOptions;
  let clientOptions;
  let resolveRun;
  const runner = {
    run: () => new Promise((resolve) => { resolveRun = resolve; }),
    stop: () => resolveRun()
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
    await running.shutdown('test');
  }
});

test('graceful shutdown 停止空内容 Runner 并释放实际监听端口', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));
  const running = await start({ logger: loggerCapture(), loadConfigFn: () => createTestConfig(port), scriptDefinition: EMPTY_SCRIPT });
  const processRef = new EventEmitter();
  const handlers = installShutdownHandlers({ running, logger: loggerCapture(), processRef });
  processRef.emit('SIGTERM');
  await handlers.shutdown('SIGTERM');
  assert.equal(running.runner.stopped, true);
  const rebound = await bindPort(port);
  await new Promise((resolve) => rebound.close(resolve));
});

test('stop 使用 fake timers 清理默认段内和段尾 timer，且不再发送后续消息', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const sent = [];
  const runner = createRunner({
    script: { initialDelayMs: 0, defaultMessageIntervalMs: 40000, paragraphIntervalMs: 720000, paragraphs: [{ id: 'A', messages: ['one', 'two'] }] },
    client: { sendMessage: async ({ body }) => { sent.push(body); return { ok: true, status: 200 }; } }
  });
  const running = runner.run();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(sent, ['one']);
  runner.stop();
  await running;
  t.mock.timers.tick(800000);
  assert.deepEqual(sent, ['one']);

  const paragraphSent = [];
  const paragraphRunner = createRunner({
    script: { initialDelayMs: 0, defaultMessageIntervalMs: 40000, paragraphIntervalMs: 720000, paragraphs: [{ id: 'A', messages: ['paragraph-end'] }] },
    client: { sendMessage: async ({ body }) => { paragraphSent.push(body); return { ok: true, status: 200 }; } }
  });
  const paragraphRunning = paragraphRunner.run();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(paragraphSent, ['paragraph-end']);
  paragraphRunner.stop();
  await paragraphRunning;
  t.mock.timers.tick(800000);
  assert.deepEqual(paragraphSent, ['paragraph-end']);
});

test('日志使用北京时间且 token 脱敏', () => {
  assert.equal(formatBeijingTimestamp(new Date('2026-08-25T03:35:05.281Z')), '2026-08-25 11:35:05');
  const lines = [];
  createLogger({ log: (line) => lines.push(line) }).info('test', { ingressToken: 'must-not-appear' });
  assert.doesNotMatch(lines[0], /must-not-appear/);
});
