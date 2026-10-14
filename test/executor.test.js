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

async function requestJson(port, method, path, payload) {
  const body = payload === undefined ? null : JSON.stringify(payload);
  return new Promise((resolve, reject) => {
    const headers = body === null ? {} : { 'Content-Type': 'application/json' };
    const request = http.request({ host: '127.0.0.1', port, method, path, headers }, (response) => {
      response.setEncoding('utf8');
      let responseBody = '';
      response.on('data', (chunk) => { responseBody += chunk; });
      response.on('end', () => resolve({ statusCode: response.statusCode, body: JSON.parse(responseBody) }));
    });
    request.on('error', reject);
    if (body !== null) request.write(body);
    request.end();
  });
}

async function requestRaw(port, method, path, text) {
  return new Promise((resolve, reject) => {
    const request = http.request({ host: '127.0.0.1', port, method, path, headers: { 'Content-Type': 'application/json' } }, (response) => {
      response.setEncoding('utf8');
      let responseBody = '';
      response.on('data', (chunk) => { responseBody += chunk; });
      response.on('end', () => resolve({ statusCode: response.statusCode, body: JSON.parse(responseBody) }));
    });
    request.on('error', reject);
    request.write(text);
    request.end();
  });
}

async function waitFor(predicate, timeoutMs = 1000) {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) throw new Error('waitFor timeout');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function createControllableRunner() {
  let resolveRun;
  let resolveStopped;

  const runner = {
    started: 0,
    stopped: 0,
    stoppedPromise: new Promise((resolve) => { resolveStopped = resolve; }),
    run() {
      this.started += 1;
      return new Promise((resolve) => { resolveRun = resolve; });
    },
    stop() {
      this.stopped += 1;
      resolveStopped();
      resolveRun();
    }
  };

  return runner;
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
    assert.equal(runnerOptions, undefined);
    assert.deepEqual(
      { ingressHost: clientOptions.ingressHost, ingressPort: clientOptions.ingressPort, authToken: clientOptions.authToken, timeoutMs: clientOptions.timeoutMs, ingressUrl: clientOptions.ingressUrl },
      { ingressHost: '127.0.0.1', ingressPort: 29876, authToken: 'test-token', timeoutMs: 1000, ingressUrl: 'http://127.0.0.1:29876/agent/send' }
    );
    assert.deepEqual(await requestHealth(port), { statusCode: 200, body: JSON.stringify({ status: 'ok' }) });
    await running.controller.start();
    assert.equal(runnerOptions.agent, TEST_AGENT);
    assert.equal(runnerOptions.to, TO);
  } finally {
    await running.shutdown('test');
  }
});

test('启动后仅监听 HTTP 服务，不自动创建 Polling Runner', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  const runners = [];
  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => ({}),
    createPollingRunnerFn: () => { const runner = createControllableRunner(); runners.push(runner); return runner; }
  });

  try {
    assert.equal(runners.length, 0);
    assert.deepEqual(await requestHealth(port), { statusCode: 200, body: JSON.stringify({ status: 'ok' }) });
  } finally {
    await running.shutdown('test');
  }
});

test('执行器启动后不自动发起任何 RUISI 回程请求', async () => {
  const requests = [];
  const mock = await mockIngress(async (request, response) => {
    requests.push(await readJson(request));
    response.writeHead(200, { 'Content-Type': 'application/json' }).end('{"status":"ok"}');
  });
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => createRuisiClient({ ingressUrl: mock.url, authToken: 'test-token', timeoutMs: 1000 })
  });

  try {
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.equal(requests.length, 0);
    assert.deepEqual(await requestHealth(port), { statusCode: 200, body: JSON.stringify({ status: 'ok' }) });
  } finally {
    await running.shutdown('test');
    await mock.close();
  }
});

test('POST /api/commands 启动指令开启轮询且重复调用只创建一个 Runner', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  const runners = [];
  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => ({}),
    createPollingRunnerFn: () => { const runner = createControllableRunner(); runners.push(runner); return runner; }
  });

  try {
    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{ action: '启动园区AI会议智能体', params: {} }]), {
      statusCode: 200,
      body: { ok: true, message: 'polling started' }
    });
    assert.equal(runners.length, 1);
    assert.equal(runners[0].started, 1);

    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{ action: '启动园区AI会议智能体', params: {} }]), {
      statusCode: 200,
      body: { ok: true, message: 'polling already running' }
    });
    assert.equal(runners.length, 1);
    assert.equal(runners[0].started, 1);
  } finally {
    await running.shutdown('test');
  }
});

test('POST /api/commands 取消指令停止后 HTTP 保持可用，再次启动创建新 Runner', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  const runners = [];
  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => ({}),
    createPollingRunnerFn: () => { const runner = createControllableRunner(); runners.push(runner); return runner; }
  });

  try {
    await requestJson(port, 'POST', '/api/commands', [{ action: '启动园区AI会议智能体', params: {} }]);

    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{ action: '取消园区AI会议智能体', params: {} }]), {
      statusCode: 200,
      body: { ok: true, message: 'polling stopped' }
    });
    assert.equal(runners[0].stopped, 1);

    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{ action: '取消园区AI会议智能体', params: {} }]), {
      statusCode: 200,
      body: { ok: true, message: 'polling already stopped' }
    });
    assert.deepEqual(await requestHealth(port), { statusCode: 200, body: JSON.stringify({ status: 'ok' }) });

    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{ action: '启动园区AI会议智能体', params: {} }]), {
      statusCode: 200,
      body: { ok: true, message: 'polling started' }
    });
    assert.equal(runners.length, 2);
    assert.equal(runners[1].started, 1);
  } finally {
    await running.shutdown('test');
  }
});

test('空脚本时指令接口开启与关闭均正常且不发送任何消息', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  let sendCount = 0;
  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => ({ sendMessage: async () => { sendCount += 1; return { ok: true, status: 200 }; } }),
    scriptDefinition: EMPTY_SCRIPT
  });

  try {
    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{ action: '启动园区AI会议智能体', params: {} }]), {
      statusCode: 200,
      body: { ok: true, message: 'polling started' }
    });
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(sendCount, 0);

    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{ action: '取消园区AI会议智能体', params: {} }]), {
      statusCode: 200,
      body: { ok: true, message: 'polling stopped' }
    });
    assert.equal(sendCount, 0);
  } finally {
    await running.shutdown('test');
  }
});

test('取消指令停止后不再产生新的回程请求，且在途请求先完成', async () => {
  const requests = [];
  let releaseResponse = null;
  const mock = await mockIngress(async (request, response) => {
    requests.push(await readJson(request));
    releaseResponse = () => {
      response.writeHead(200, { 'Content-Type': 'application/json' }).end('{"status":"ok"}');
    };
  });
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => createRuisiClient({ ingressUrl: mock.url, authToken: 'test-token', timeoutMs: 1000 }),
    createPollingRunnerFn: (options) => new PollingRunner({
      ...options,
      script: { initialDelayMs: 0, defaultMessageIntervalMs: 40000, paragraphIntervalMs: 720000, paragraphs: [{ id: '01', messages: ['one', 'two', 'three'] }] },
      sleep: async () => {}
    })
  });

  try {
    await requestJson(port, 'POST', '/api/commands', [{ action: '启动园区AI会议智能体', params: {} }]);
    await waitFor(() => requests.length === 1);

    const stopRequest = requestJson(port, 'POST', '/api/commands', [{ action: '取消园区AI会议智能体', params: {} }]);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(requests.length, 1);

    releaseResponse();
    assert.deepEqual(await stopRequest, {
      statusCode: 200,
      body: { ok: true, message: 'polling stopped' }
    });
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(requests.length, 1);
    assert.deepEqual(await requestHealth(port), { statusCode: 200, body: JSON.stringify({ status: 'ok' }) });
  } finally {
    await running.shutdown('test');
    await mock.close();
  }
});

test('未知 action 不会误启动或误停止轮询', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  const runners = [];
  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => ({}),
    createPollingRunnerFn: () => { const runner = createControllableRunner(); runners.push(runner); return runner; }
  });

  try {
    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{ action: '启动园区AI不存在的智能体', params: {} }]), {
      statusCode: 400,
      body: { ok: false, error: 'command at index 0 action is not registered' }
    });
    assert.equal(runners.length, 0);

    await requestJson(port, 'POST', '/api/commands', [{ action: '启动园区AI会议智能体', params: {} }]);
    assert.equal(runners.length, 1);

    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [
      { action: '启动园区AI会议智能体', params: {} },
      { action: '停止轮询', params: {} }
    ]), {
      statusCode: 400,
      body: { ok: false, error: 'command at index 1 action is not registered' }
    });
    assert.equal(runners.length, 1);
    assert.equal(runners[0].stopped, 0);
    assert.deepEqual(await requestHealth(port), { statusCode: 200, body: JSON.stringify({ status: 'ok' }) });
  } finally {
    await running.shutdown('test');
  }
});

test('异常请求返回合理错误且不改变轮询状态', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  const runners = [];
  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => ({}),
    createPollingRunnerFn: () => { const runner = createControllableRunner(); runners.push(runner); return runner; }
  });

  try {
    assert.deepEqual(await requestRaw(port, 'POST', '/api/commands', 'not-json'), {
      statusCode: 400, body: { ok: false, error: 'invalid JSON request body' }
    });
    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', []), {
      statusCode: 400, body: { ok: false, error: 'request body must not be empty' }
    });
    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', { action: '启动园区AI会议智能体' }), {
      statusCode: 400, body: { ok: false, error: 'request body must be an array or context envelope' }
    });
    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{}]), {
      statusCode: 400, body: { ok: false, error: 'command at index 0 must include a non-empty string action' }
    });
    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', [{ action: '启动园区AI会议智能体', params: [] }]), {
      statusCode: 400, body: { ok: false, error: 'command at index 0 params must be a plain object' }
    });
    assert.deepEqual(await requestJson(port, 'GET', '/api/commands'), {
      statusCode: 405, body: { ok: false, error: 'method not allowed' }
    });
    assert.deepEqual(await requestJson(port, 'GET', '/api/unknown'), {
      statusCode: 404, body: { status: 'not_found' }
    });
    assert.equal(runners.length, 0);
  } finally {
    await running.shutdown('test');
  }
});

test('兼容 context 信封请求体', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => ({}),
    createPollingRunnerFn: () => createControllableRunner()
  });

  try {
    assert.deepEqual(await requestJson(port, 'POST', '/api/commands', {
      context: { agent: 'hc-agent' },
      commands: [{ action: '取消园区AI会议智能体', params: {} }]
    }), {
      statusCode: 200, body: { ok: true, message: 'polling already stopped' }
    });
  } finally {
    await running.shutdown('test');
  }
});

test('stop({ abortInFlight: false }) 等待在途请求完成且不再发送后续消息', async () => {
  let resolveSend;
  let sendCount = 0;
  const client = {
    sendMessage: () => {
      sendCount += 1;
      return new Promise((resolve) => { resolveSend = () => resolve({ ok: true, status: 200 }); });
    }
  };
  const runner = createRunner({
    script: { initialDelayMs: 0, defaultMessageIntervalMs: 40000, paragraphIntervalMs: 720000, paragraphs: [{ id: '01', messages: ['one', 'two'] }] },
    client,
    sleep: async () => {}
  });
  const running = runner.run();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(sendCount, 1);

  runner.stop({ abortInFlight: false });
  assert.equal(runner.activeController.signal.aborted, false);

  resolveSend();
  await running;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(sendCount, 1);
});

test('stop() 默认中止在途请求并结束轮询', async () => {
  let capturedSignal;
  let sendCount = 0;
  const client = {
    sendMessage: ({ signal }) => {
      sendCount += 1;
      capturedSignal = signal;
      return new Promise((resolve) => {
        signal.addEventListener('abort', () => resolve({ ok: false, cancelled: true }), { once: true });
      });
    }
  };
  const runner = createRunner({
    script: { initialDelayMs: 0, defaultMessageIntervalMs: 40000, paragraphIntervalMs: 720000, paragraphs: [{ id: '01', messages: ['one', 'two'] }] },
    client,
    sleep: async () => {}
  });
  const running = runner.run();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(sendCount, 1);

  runner.stop();
  assert.equal(capturedSignal.aborted, true);
  await running;
  assert.equal(sendCount, 1);
});

test('graceful shutdown 停止空内容 Runner 并释放实际监听端口', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));
  const running = await start({ logger: loggerCapture(), loadConfigFn: () => createTestConfig(port), scriptDefinition: EMPTY_SCRIPT });
  await running.controller.start();
  const processRef = new EventEmitter();
  const handlers = installShutdownHandlers({ running, logger: loggerCapture(), processRef });
  processRef.emit('SIGTERM');
  await handlers.shutdown('SIGTERM');
  assert.equal(running.controller.getStatus().state, 'stopped');
  const rebound = await bindPort(port);
  await new Promise((resolve) => rebound.close(resolve));
});

test('未开启轮询时收到 SIGTERM 仍释放监听端口', async () => {
  const probe = await bindPort(0);
  const port = probe.address().port;
  await new Promise((resolve) => probe.close(resolve));

  const running = await start({
    logger: loggerCapture(),
    loadConfigFn: () => createTestConfig(port),
    createRuisiClientFn: () => ({}),
    createPollingRunnerFn: () => createControllableRunner()
  });
  const processRef = new EventEmitter();
  const shutdownHandlers = installShutdownHandlers({ running, logger: loggerCapture(), processRef });

  processRef.emit('SIGTERM');
  await shutdownHandlers.shutdown('SIGTERM');

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
