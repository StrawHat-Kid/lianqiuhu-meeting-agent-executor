# 练秋湖 HC2026 会议智能体轮询执行器

HC2026 会议智能体固定轮询回程执行器。它将会议智能体的固定轮询内容逐条发送到 RUISI-OSCA 回程接口；不接收 IOC 指令、不使用 MQTT，也不生成园区数据。

当前状态：会议智能体正式固定轮询文案已接入，共 12 段、28 条。正式 AGENT 仍待产品方案提供并配置；未配置 AGENT 时，进程会在启动前校验失败，不会发送消息。

## 使用

```bash
npm install
# 从 .env.example 创建 .env，并填入产品提供的配置
npm test
npm start
```

`.env` 已被 Git 忽略，不能提交。默认启动端口为 `18035`；`18034` 已被问答智能体执行器使用。

## 配置

| 配置项 | 说明 |
| --- | --- |
| `PORT` | HTTP 健康检查监听端口，默认 `18035`。 |
| `AGENT` | 会议智能体正式 AGENT，当前必须由产品方案提供；为空时配置校验失败，Runner 不启动。 |
| `INGRESS_HOST` | RUISI ingress 主机或 IP，不含协议和路径。 |
| `INGRESS_PORT` | RUISI ingress 端口。 |
| `INGRESS_TOKEN` | RUISI ingress 的 `X-Auth-Token`，不得提交真实值。 |
| `INGRESS_TIMEOUT_MS` | 可选请求超时，默认 `5000` 毫秒。 |

回程请求固定为 `POST http://${INGRESS_HOST}:${INGRESS_PORT}/agent/send`，请求体的 `to` 为 `xslatdzp.demo001@rscom-chat.rsagent.net`，并携带 `X-Auth-Token`。HTTP 任意 2xx 均视为成功；单条失败会记录日志，后续消息和轮询继续执行。

## 轮询框架

轮询内容独立定义在 `src/scripts/messages.js`。正式文案、顺序和段落均在该文件中维护，不需要修改 HTTP Server、配置、PollingRunner、RUISI Client、计时、停止逻辑或健康检查。

固定轮询共 12 段、28 条：首次发送延迟 `0`；同段相邻消息间隔 `40000` 毫秒；每段最后一条后等待 `720000` 毫秒，再进入下一段；第 12 段同样等待 12 分钟后回到第 1 段无限循环。

健康检查：`GET /health` 返回 HTTP `200` 与 `{"status":"ok"}`。
