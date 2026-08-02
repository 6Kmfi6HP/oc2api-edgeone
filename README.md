# oc2api-edgeone

[![Test](https://github.com/6Kmfi6HP/oc2api-edgeone/actions/workflows/test.yml/badge.svg)](https://github.com/6Kmfi6HP/oc2api-edgeone/actions/workflows/test.yml)

部署在 EdgeOne Makers 上的 OpenCode Zen 免费模型网关。它只公开上游 ID 以 `-free` 结尾的模型，并为客户端提供不带后缀的模型别名。

在线地址：<https://oc2api-edgeone.edgeone.dev>

## 工作方式

- `GET /v1/models` 过滤掉非免费模型，并将 `deepseek-v4-flash-free` 显示为 `deepseek-v4-flash`。
- `/v1/responses`、`/v1/chat/completions` 和 `/v1/messages` 将 JSON 请求体顶层的 `model` 映射回上游 `*-free` ID。
- 除顶层 `model` 外，其他 JSON 字段保持不变；推理响应和 SSE 流不做解析或转换。
- 上游固定使用 OpenCode 的公开免费 Key `Bearer public`。客户端无需提供 API Key，客户端传入的 `Authorization` 或 `x-api-key` 不会转发。

当前 OpenCode Zen 将免费模型列为 OpenAI-compatible Chat Completions 模型。`/v1/responses` 和 `/v1/messages` 会完成模型 ID 映射，但不做协议转换；如果上游模型不支持相应协议，Zen 的错误响应会原样返回。

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
npm exec edgeone makers deploy --area overseas --env production
```

项目锁定使用 `edgeone@1.6.19`，仓库不包含账号 Token、`.env` 或本地 `.edgeone` 项目绑定。

## 运行时限制

- Edge Function 客户端请求 body 上限为 1 MB。
- 上游连接、读取和写入超时均设置为 300 秒。
- 每个请求只发起一次上游 `fetch`；网络异常返回通用 `502 Bad Gateway`。
- 模型列表响应和三种推理请求体需要在函数中读取，受 1 MB body 限制；推理响应仍直接流式返回。
- 除成功的模型列表响应外，上游 4xx、5xx、重定向、响应头和响应体不会被业务逻辑改写。

该服务没有代理层限流或访问控制。公开部署前应根据流量和滥用风险增加相应保护。

## 项目结构

```text
edge-functions/v1/[[default]].js  EdgeOne 路由与代理实现
test/v1-proxy.test.js             Node.js 单元测试
```

## License

[MIT](LICENSE)
