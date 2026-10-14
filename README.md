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

## 回程轮询指令接口

执行器启动后只监听 HTTP 端口，**默认不开启回程轮询**；由外部通过 `POST /api/commands` 发送指令开启或关闭。该路径、方法与请求体结构与 `lianqiuhu-ioc-executor` 一致；本接口**未实现应用层鉴权**（不校验任何 Token）。

请求体为 JSON 数组，元素为 `{ "action": "...", "params": {} }`：

```bash
curl -X POST http://127.0.0.1:18035/api/commands \
  -H "Content-Type: application/json" \
  -d '[{"action":"启动园区AI会议智能体","params":{}}]'
# {"ok":true,"message":"polling started"}

curl -X POST http://127.0.0.1:18035/api/commands \
  -H "Content-Type: application/json" \
  -d '[{"action":"取消园区AI会议智能体","params":{}}]'
# {"ok":true,"message":"polling stopped"}
```

| 指令（action） | 行为 |
| --- | --- |
| `启动园区AI会议智能体` | 开启回程轮询；重复调用不会创建多个轮询任务（返回 `polling already running`） |
| `取消园区AI会议智能体` | 停止回程轮询；等待在途请求完成后停止（最长 `INGRESS_TIMEOUT_MS`）；重复调用返回 `polling already stopped` |

> 名称状态：`启动园区AI会议智能体` / `取消园区AI会议智能体` 为已正式确认的 action 名称；睿司侧的指令注册与真实调用联调尚未完成，部署前需完成对应配置与联调。

- 也接受 `{"commands":[...]}` 信封格式（与指令执行器一致）；`params` 可省略或为空对象，内容会被忽略。
- 未知 action、非法 JSON、空数组、仅含 action 的单个对象、`params` 非对象等返回 `400 {"ok":false,"error":"..."}`；`/api/commands` 使用非 POST 方法返回 `405`；其他路径返回 `404 {"status":"not_found"}`。
- 空脚本（无轮询内容）时，启动指令进入空闲保持、不会发送任何消息；取消指令可正常停止。
- 停止后再次发送启动指令会创建新的轮询任务，并从段落 01 第 1 条重新开始；`SIGINT`/`SIGTERM` 仍会中止在途请求并退出进程。
- 原 `/api/polling/start`、`/api/polling/stop`、`/api/polling/status` 路由已移除，不再对外提供。
- 健康检查：`GET /health` 返回 HTTP `200` 与 `{"status":"ok"}`，停止轮询后仍然可用。

**安全说明（本轮）**：指令接口未实现应用层鉴权（不校验任何 Token）。部署时必须通过内网访问限制、防火墙、安全组或其他网络访问控制手段，限制可访问执行器端口的来源，以降低未授权调用风险。

## 轮询框架

轮询内容独立定义在 `src/scripts/messages.js`。正式文案、顺序和段落均在该文件中维护，不需要修改 HTTP Server、配置、PollingRunner、RUISI Client、计时、停止逻辑或健康检查。

固定轮询共 12 段、28 条：开启轮询后首次发送延迟 `0`；同段相邻消息间隔 `40000` 毫秒；每段最后一条后等待 `720000` 毫秒，再进入下一段；第 12 段同样等待 12 分钟后回到第 1 段无限循环。

健康检查：`GET /health` 返回 HTTP `200` 与 `{"status":"ok"}`，停止轮询后仍然可用。
