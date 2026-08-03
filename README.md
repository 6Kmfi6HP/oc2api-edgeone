# oc2api-edgeone

[![Test](https://github.com/6Kmfi6HP/oc2api-edgeone/actions/workflows/test.yml/badge.svg)](https://github.com/6Kmfi6HP/oc2api-edgeone/actions/workflows/test.yml)

部署在 EdgeOne Makers 上的 OpenCode Zen 免费模型网关。它只公开上游 ID 以 `-free` 结尾的模型，并为客户端提供不带后缀的模型别名。

在线地址：<https://oc2api-edgeone.edgeone.dev>

## 工作方式

- `GET /v1/models` 过滤掉非免费模型，并将 `deepseek-v4-flash-free` 显示为 `deepseek-v4-flash`。
- 上游固定使用 OpenCode 的公开免费 Key `Bearer public`。客户端无需提供 API Key，客户端传入的 `Authorization` 或 `x-api-key` 不会转发。
- 所有请求都会把顶层 `model` 映射回上游 `*-free` ID，并丢弃缺少可解析 `name` 的 function 工具（OpenCode CLI 与 `CLAUDE_CODE_SIMPLE=1` 客户端会把工具序列化为 `{"type":"function","function":{}}`，上游 Console 会以 `tools[0].function: missing field "name"` 拒绝）。若清理后 `tools` 为空数组，则删除该字段。

三个推理接口统一内部转为 Chat Completions 协议转发上游，再把响应转回各自的协议：

- `/v1/chat/completions`：天生就是 Chat 协议，请求仅做模型映射、工具清理与 reasoning 兜底，响应（含 SSE 流）直接透传，不做额外改写。
- `/v1/messages`（Claude）：请求把 `system`、`thinking`、`tool_use`、`tool_result`、`image` 内容块转为 Chat 消息与工具；响应反向转回 Claude `message` 格式——文本、`thinking` 块和 `tool_use` 输出，SSE 流则重写为 `message_start` / `content_block_start` / `content_block_delta` / `message_stop` 事件。
- `/v1/responses`（OpenAI Responses API）：请求把 `input`、`instructions`、`function_call_output`、内置 `apply_patch` / `shell` 工具等转为 Chat 消息；响应反向转回 Responses 的 `message` / `reasoning` / `function_call` 输出项，SSE 流重写为 `response.created` / `output_text.delta` / `function_call_arguments.delta` / `response.completed` 事件。

转换时保持 `reasoning_content` 与 thinking 的对应（Claude 侧显示为 `thinking` 块，Responses 侧显示为 `reasoning` 摘要），并修复缺失的 `tool_result`（无响应的 tool call 会补占位消息）。上游的错误响应和 4xx/5xx 状态码原样返回，不参与改写。

### Thinking 模式下 `reasoning_content` 兜底

上游在 thinking 模式下要求历史中每条 assistant 消息都必须携带 `reasoning_content`，否则返回
`[invalid_request_error] The reasoning_content in the thinking mode must be passed back to the API`。
但 Claude Code、Cherry Studio 等多轮回传时往往只带回文本、丢掉了 thinking 块。网关对此做两层处理：

1. **先还原真实推理内容**：Claude 路径把客户端回传的 `thinking` 内容块提取回 `reasoning_content`；Responses 路径把 `reasoning` 条目累积回对应的 assistant 消息。
2. **再以空串兜底**：仍缺 `reasoning_content`（字段缺失或显式为 `null`）的 assistant 历史消息补成 `""`——上游会把"传了空推理"当作合法，只有字段整体缺失才拒绝。三条入口均已挂载：

   - `/v1/messages` 与 `/v1/responses`：在请求转成 Chat 消息后调用 `ensureReasoningContent`。
   - `/v1/chat/completions`：走透传不进转换器，故在 `mapRequestBody` 内做同样的兜底。

   `thinking: { type: "disabled" }` 时跳过兜底，不做任何字段注入；返回给客户端的方向则由 `cleanStreamDelta` 负责，在不需要 reasoning 时把 `reasoning_content`（含空串）从流中清理掉。

   注：上游为 OpenAI 兼容端点，网关不校验 Anthropic 原生 thinking 的 `signature`。若将来直连 Anthropic 官方端点，空串兜底会失效（官方要求 `signature` 原样回传），届时应改为丢弃无签名的 thinking 块。

## 使用

查询免费模型：

```bash
curl https://oc2api-edgeone.edgeone.dev/v1/models
```

发起 Chat Completions 请求：

```bash
curl https://oc2api-edgeone.edgeone.dev/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "deepseek-v4-flash",
    "messages": [
      {
        "role": "user",
        "content": "Reply with exactly OK"
      }
    ],
    "stream": false,
    "max_tokens": 128
  }'
```

启用 SSE 时只需设置 `"stream": true`。

## 路由

| 客户端路径 | OpenCode Zen 上游路径 |
| --- | --- |
| `/v1/models` | `/zen/v1/models` |
| `/v1/responses` | `/zen/v1/responses` |
| `/v1/chat/completions` | `/zen/v1/chat/completions` |
| `/v1/messages` | `/zen/v1/messages` |

查询参数、端到端请求头和上游状态码会保留。逐跳头、`Host`、`Content-Length` 和客户端鉴权头会被移除，由运行时重新生成必要字段。

## 本地开发

需要 Node.js 22 或更高版本：

```bash
npm ci
npm test
```

启动 EdgeOne Makers 本地服务：

```bash
npm exec edgeone makers dev
```

EdgeOne CLI 默认监听 `http://localhost:8088`。本地调试环境不能通过 `fetch` 访问 EdgeOne 节点缓存或源站，因此真实上游连通性需要部署后验证。

## 部署

登录并绑定 EdgeOne Makers 项目后运行：

```bash
npm exec -- edgeone makers deploy -a overseas -e production
```

项目锁定使用 `edgeone@1.6.19`，仓库不包含账号 Token、`.env` 或本地 `.edgeone` 项目绑定。

## 运行时限制

- Edge Function 客户端请求 body 上限为 1 MB。
- 上游连接、读取和写入超时均设置为 300 秒。
- 每个请求只发起一次上游 `fetch`；网络异常返回通用 `502 Bad Gateway`。
- 模型列表响应和三种推理请求体需要在函数中读取，受 1 MB body 限制。`/v1/chat/completions` 的响应流式透传；`/v1/messages` 与 `/v1/responses` 的响应（含 SSE 流）会被解析并重写为对应协议。
- 除成功的模型列表响应外，上游 4xx、5xx、重定向、响应头和响应体不会被业务逻辑改写。

该服务没有代理层限流或访问控制。公开部署前应根据流量和滥用风险增加相应保护。

## 项目结构

```text
edge-functions/v1/[[default]].js  EdgeOne 路由与代理实现
test/v1-proxy.test.js             Node.js 单元测试
```

## License

[MIT](LICENSE)
