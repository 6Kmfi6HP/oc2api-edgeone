const UPSTREAM_ORIGIN = "https://opencode.ai";
const UPSTREAM_PREFIX = "/zen";
const PUBLIC_AUTHORIZATION = "Bearer public";
const FREE_MODEL_SUFFIX = "-free";
const MAX_TIMEOUT_MS = 300_000;

const MODEL_LIST_PATH = "/v1/models";
const MODEL_REQUEST_PATHS = new Set([
  "/v1/responses",
  "/v1/chat/completions",
  "/v1/messages",
]);

const CHAT_PATH = "/v1/chat/completions";
const MESSAGES_PATH = "/v1/messages";
const RESPONSES_PATH = "/v1/responses";

const HOP_BY_HOP_HEADERS = [
  "connection",
  "content-length",
  "host",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "proxy-connection",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
];

const TRANSFORMED_RESPONSE_HEADERS = [
  "content-encoding",
  "content-length",
  "content-md5",
  "digest",
  "etag",
  "transfer-encoding",
];

const SSE_RESPONSE_HEADERS = {
  "content-type": "text/event-stream",
  "cache-control": "no-cache",
  connection: "keep-alive",
};

// ======================== 基础辅助 ========================

function randomString(n) {
  const letters = "abcdefghijklmnopqrstuvwxyz0123456789";
  let out = "";
  for (let i = 0; i < n; i++) {
    out += letters[Math.floor(Math.random() * letters.length)];
  }
  return out;
}

function toFloat64(v) {
  if (typeof v === "number") {
    return v;
  }
  if (typeof v === "bigint") {
    return Number(v);
  }
  return 0;
}

function jsonStringify(value) {
  try {
    return JSON.stringify(value);
  } catch {
    return null;
  }
}

// Deep JSON clone used where a value must be copied (not shared) into output.
function jsonClone(value) {
  return JSON.parse(JSON.stringify(value));
}

async function* sseBodyLines(respBody) {
  const reader = respBody.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      let index;
      while ((index = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, index);
        buffer = buffer.slice(index + 1);
        yield line.trimEnd();
      }
    }
    const rest = decoder.decode();
    if (rest) {
      yield rest.trimEnd();
    }
  } finally {
    reader.releaseLock();
  }
}

// ======================== 响应 JSON/SSE 包装 ========================

function transformResponseHeaders(sourceHeaders) {
  const headers = new Headers(sourceHeaders);
  for (const header of TRANSFORMED_RESPONSE_HEADERS) {
    headers.delete(header);
  }
  return headers;
}

function jsonResponseBody(status, payload) {
  const headers = transformResponseHeaders(new Headers());
  headers.set("content-type", "application/json; charset=utf-8");
  return new Response(jsonStringify(payload), {
    status,
    headers,
  });
}

function sseResponse(stream) {
  return new Response(stream, {
    status: 200,
    headers: new Headers(SSE_RESPONSE_HEADERS),
  });
}

// ======================== 模型 ID / 转发（原有） ========================

function upstreamUrl(requestUrl) {
  const incomingUrl = new URL(requestUrl);
  return `${UPSTREAM_ORIGIN}${UPSTREAM_PREFIX}${incomingUrl.pathname}${incomingUrl.search}`;
}

function chatUpstreamUrl(requestUrl) {
  const incomingUrl = new URL(requestUrl);
  return `${UPSTREAM_ORIGIN}${UPSTREAM_PREFIX}${CHAT_PATH}${incomingUrl.search}`;
}

function forwardHeaders(headers) {
  const forwardedHeaders = new Headers(headers);
  const connectionHeader = forwardedHeaders.get("connection");

  if (connectionHeader) {
    for (const token of connectionHeader.split(",")) {
      const header = token.trim().toLowerCase();
      if (/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(header)) {
        forwardedHeaders.delete(header);
      }
    }
  }

  for (const header of HOP_BY_HOP_HEADERS) {
    forwardedHeaders.delete(header);
  }

  forwardedHeaders.delete("x-api-key");
  forwardedHeaders.set("authorization", PUBLIC_AUTHORIZATION);
  return forwardedHeaders;
}

function toUpstreamModelId(modelId) {
  return modelId.endsWith(FREE_MODEL_SUFFIX)
    ? modelId
    : `${modelId}${FREE_MODEL_SUFFIX}`;
}

function toPublicModelId(modelId) {
  if (
    typeof modelId !== "string"
    || modelId.length <= FREE_MODEL_SUFFIX.length
    || !modelId.endsWith(FREE_MODEL_SUFFIX)
  ) {
    return null;
  }

  return modelId.slice(0, -FREE_MODEL_SUFFIX.length);
}

/**
 * Drops function tools that carry no resolvable name. OpenCode CLI and
 * CLAUDE_CODE_SIMPLE=1 clients can serialize tools as `{ type: "function",
 * function: {} }`; strict upstreams (OpenCode Zen Console) reject those with
 * `tools[0].function: missing field "name"`. Non-function tools and function
 * tools that keep a name (chat `function.name` or responses top-level `name`)
 * are passed through untouched. Returns the original array when nothing was
 * removed, so callers can detect whether the body actually changed.
 */
function sanitizeTools(tools) {
  if (!Array.isArray(tools) || tools.length === 0) {
    return tools;
  }

  let anyRemoved = false;
  const result = [];
  for (const tool of tools) {
    if (
      tool !== null
      && typeof tool === "object"
      && !Array.isArray(tool)
      && tool.type === "function"
    ) {
      const fn =
        tool.function != null && typeof tool.function === "object"
          ? tool.function
          : null;
      const fnName =
        fn !== null && typeof fn.name === "string" && fn.name.length > 0
          ? fn.name
          : tool.name;
      if (typeof fnName !== "string" || fnName.length === 0) {
        anyRemoved = true;
        continue;
      }
    }
    result.push(tool);
  }

  return anyRemoved ? result : tools;
}

// ======================== 清理：Chat 响应 / 流 ========================

function cleanNulls(map) {
  for (const key of Object.keys(map)) {
    const value = map[key];
    if (value === null) {
      delete map[key];
    } else if (typeof value === "string" && value === "") {
      delete map[key];
    }
  }
}

function cleanStreamDelta(delta, keepReasoning) {
  if (Object.prototype.hasOwnProperty.call(delta, "content") && delta.content === null) {
    delete delta.content;
  }
  if (typeof delta.content === "string" && delta.content === "") {
    delete delta.content;
  }
  if (!keepReasoning) {
    delete delta.reasoning_content;
  } else {
    if (Object.prototype.hasOwnProperty.call(delta, "reasoning_content") && delta.reasoning_content === null) {
      delete delta.reasoning_content;
    }
    if (typeof delta.reasoning_content === "string" && delta.reasoning_content === "") {
      delete delta.reasoning_content;
    }
  }
  if (typeof delta.role === "string" && delta.role === "") {
    delete delta.role;
  }
}

function convertStreamChunkWithUsage(line, keepReasoning) {
  const trimmed = line;
  if (trimmed === "data: [DONE]" || trimmed === "[DONE]") {
    return [line, null];
  }
  if (!line.startsWith("data: ")) {
    return [line, null];
  }
  const data = line.slice(6);
  let raw;
  try {
    raw = JSON.parse(data);
  } catch {
    return [line, null];
  }
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    return [line, null];
  }

  let usage = null;
  if (raw.usage != null && typeof raw.usage === "object") {
    usage = raw.usage;
  }

  const choices = raw.choices;
  if (!Array.isArray(choices) || choices.length === 0) {
    delete raw.cost;
    return ["data: " + jsonStringify(raw), usage];
  }

  for (let i = 0; i < choices.length; i++) {
    const choice = choices[i];
    if (choice === null || typeof choice !== "object" || Array.isArray(choice)) {
      continue;
    }
    if (choice.delta != null && typeof choice.delta === "object") {
      const delta = choice.delta;
      cleanStreamDelta(delta, keepReasoning);
      choice.delta = delta;
    }
    if (choice.message != null && typeof choice.message === "object") {
      const msg = choice.message;
      cleanNulls(msg);
      if (!keepReasoning) {
        delete msg.reasoning_content;
      }
      choice.message = msg;
    }
    if (Object.prototype.hasOwnProperty.call(choice, "logprobs") && choice.logprobs === null) {
      delete choice.logprobs;
    }
    if (Object.prototype.hasOwnProperty.call(choice, "finish_reason") && choice.finish_reason === null) {
      delete choice.finish_reason;
    }
    if (typeof choice.finish_reason === "string" && choice.finish_reason === "") {
      delete choice.finish_reason;
    }
    choices[i] = choice;
  }
  raw.choices = choices;
  if (Object.prototype.hasOwnProperty.call(raw, "usage") && raw.usage === null) {
    delete raw.usage;
  }
  delete raw.cost;
  return ["data: " + jsonStringify(raw), usage];
}

function convertResponse(data, keepReasoning) {
  let raw;
  try {
    raw = JSON.parse(data);
  } catch {
    return data;
  }
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    return data;
  }
  const choices = raw.choices;
  if (Array.isArray(choices)) {
    for (let i = 0; i < choices.length; i++) {
      const choice = choices[i];
      if (choice === null || typeof choice !== "object" || Array.isArray(choice)) {
        continue;
      }
      if (choice.message != null && typeof choice.message === "object") {
        const msg = choice.message;
        cleanNulls(msg);
        if (!keepReasoning) {
          delete msg.reasoning_content;
        }
        choice.message = msg;
      }
      if (Object.prototype.hasOwnProperty.call(choice, "logprobs") && choice.logprobs === null) {
        delete choice.logprobs;
      }
      choices[i] = choice;
    }
    raw.choices = choices;
  }
  delete raw.cost;
  return jsonStringify(raw);
}

// ======================== Thinking / Reasoning ========================

function isThinkingEnabled(value) {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value.type === "enabled";
  }
  if (typeof value === "boolean") {
    return value;
  }
  return false;
}

function isThinkingDisabled(value) {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value.type === "disabled";
  }
  if (typeof value === "boolean") {
    return !value;
  }
  return false;
}

function wantsReasoning(req) {
  if (isThinkingDisabled(req.thinking)) {
    return false;
  }
  if (isThinkingEnabled(req.thinking)) {
    return true;
  }
  const extra = req.extra_body;
  if (extra != null && typeof extra === "object") {
    if (isThinkingDisabled(extra.thinking)) {
      return false;
    }
    if (isThinkingEnabled(extra.thinking)) {
      return true;
    }
  }
  return true;
}

// ======================== Chat 消息处理 ========================

function normalizeContent(content) {
  if (content === null || content === undefined) {
    return null;
  }
  if (typeof content === "string") {
    return content;
  }
  if (Array.isArray(content)) {
    return content;
  }
  const json = jsonStringify(content);
  return json === null ? null : JSON.parse(json);
}

function fixToolCallGaps(messages) {
  const toolResponses = new Map();
  for (const msg of messages) {
    if (msg.role === "tool" && msg.tool_call_id) {
      toolResponses.set(msg.tool_call_id, msg);
    }
  }
  const fixed = [];
  const emitted = new Map();
  for (const msg of messages) {
    if (msg.role === "tool" && msg.tool_call_id) {
      if (emitted.has(msg.tool_call_id)) {
        continue;
      }
    }
    fixed.push(msg);
    if (msg.role === "assistant" && Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) {
      for (const tc of msg.tool_calls) {
        if (!tc || !tc.id) {
          continue;
        }
        const resp = toolResponses.get(tc.id);
        if (resp) {
          fixed.push(resp);
        } else {
          fixed.push({ role: "tool", tool_call_id: tc.id, content: "Tool call result not available" });
        }
        emitted.set(tc.id, true);
      }
    }
  }
  return fixed;
}

function ensureReasoningContent(messages, thinking) {
  if (!thinking) {
    return messages;
  }
  const result = messages.slice();
  for (let i = 0; i < result.length; i++) {
    if (result[i].role === "assistant" && result[i].reasoning_content === undefined) {
      result[i] = { ...result[i], reasoning_content: "" };
    }
  }
  return result;
}

function convertMessagesForUpstream(messages) {
  const converted = [];
  for (const msg of messages) {
    const clean = {};
    if (msg.role) {
      clean.role = msg.role;
    }
    const content = normalizeContent(msg.content);
    if (content !== null) {
      clean.content = content;
    }
    if (msg.reasoning_content !== undefined && msg.reasoning_content !== null) {
      clean.reasoning_content = msg.reasoning_content;
    }
    if (Array.isArray(msg.tool_calls) && msg.tool_calls.length > 0) {
      clean.tool_calls = msg.tool_calls;
    }
    if (msg.tool_call_id) {
      clean.tool_call_id = msg.tool_call_id;
    }
    if (msg.name) {
      clean.name = msg.name;
    }
    converted.push(clean);
  }
  return converted;
}

// ======================== Chat 上游请求体组装 ========================

function openAIRequestToBody(o) {
  const converted = {
    model: o.model,
    messages: convertMessagesForUpstream(o.messages || []),
    stream: Boolean(o.stream),
  };
  if (o.temperature !== undefined && o.temperature !== null) {
    converted.temperature = o.temperature;
  }
  if (o.max_tokens !== undefined && o.max_tokens !== null) {
    converted.max_tokens = o.max_tokens;
  }
  if (o.top_p !== undefined && o.top_p !== null) {
    converted.top_p = o.top_p;
  }
  if (Array.isArray(o.tools) && o.tools.length > 0) {
    const cleaned = sanitizeTools(o.tools);
    if (Array.isArray(cleaned) && cleaned.length === 0) {
      delete converted.tools;
    } else {
      converted.tools = cleaned;
    }
  }
  if (o.tool_choice !== undefined && o.tool_choice !== null) {
    converted.tool_choice = o.tool_choice;
  }

  if (isThinkingDisabled(o.thinking)) {
    converted.thinking = { type: "disabled" };
  } else if (o.thinking !== undefined && o.thinking !== null && isThinkingEnabled(o.thinking)) {
    converted.thinking = { type: "enabled" };
  } else if (o.extra_body != null && typeof o.extra_body === "object") {
    if (isThinkingDisabled(o.extra_body.thinking)) {
      converted.thinking = { type: "disabled" };
    } else if (isThinkingEnabled(o.extra_body.thinking)) {
      converted.thinking = { type: "enabled" };
    }
  }

  if (o.reasoning_effort) {
    converted.reasoning_effort = o.reasoning_effort;
  }

  if (o.extra_body != null && typeof o.extra_body === "object") {
    for (const key of Object.keys(o.extra_body)) {
      if (!Object.prototype.hasOwnProperty.call(converted, key)) {
        converted[key] = o.extra_body[key];
      }
    }
  }
  return converted;
}

function chatRequestInit(request, bodyObj) {
  const init = {
    method: "POST",
    headers: forwardHeaders(request.headers),
    redirect: "manual",
    eo: {
      timeoutSetting: {
        connectTimeout: MAX_TIMEOUT_MS,
        readTimeout: MAX_TIMEOUT_MS,
        writeTimeout: MAX_TIMEOUT_MS,
      },
    },
  };
  init.body = jsonStringify(bodyObj);
  return init;
}

// ======================== Claude Messages -> Chat（请求） ========================

function extractClaudeSystemText(system) {
  if (system === null || system === undefined) {
    return "";
  }
  if (typeof system === "string") {
    return system;
  }
  if (Array.isArray(system)) {
    const parts = [];
    for (const item of system) {
      if (item !== null && typeof item === "object" && typeof item !== "string" && typeof item !== "number") {
        if (item.type === "text" && typeof item.text === "string") {
          parts.push(item.text);
        }
      }
    }
    return parts.join("\n");
  }
  return jsonStringify(system) !== null ? jsonStringify(system) : "";
}

function cleanJsonSchema(schema) {
  if (schema === null || typeof schema !== "object" || Array.isArray(schema)) {
    return schema;
  }
  const clean = {};
  for (const key of Object.keys(schema)) {
    if (key === "$schema" || key === "title" || key === "examples") {
      continue;
    }
    const value = schema[key];
    if (value !== null && typeof value === "object" && !Array.isArray(value)) {
      clean[key] = cleanJsonSchema(value);
    } else if (Array.isArray(value)) {
      clean[key] = value.map(cleanJsonSchema);
    } else {
      clean[key] = value;
    }
  }
  return clean;
}

function buildClaudeMsg(reasoningParts, toolCalls, toolResults, orderedContent, role) {
  const om = { role };
  if (orderedContent.length > 0) {
    om.content = orderedContent;
  } else if (toolCalls.length === 0) {
    om.content = "";
  }
  if (reasoningParts.length > 0) {
    om.reasoning_content = reasoningParts.join("\n");
  }
  if (toolCalls.length > 0) {
    om.tool_calls = toolCalls;
  }
  return { om, toolResults };
}

function claudeToOpenAIMessages(claudeMsgs, system) {
  const messages = [];
  const sysText = extractClaudeSystemText(system);
  if (sysText !== "") {
    messages.push({ role: "system", content: sysText });
  }
  for (const msg of claudeMsgs) {
    const content = msg.content;
    if (typeof content === "string") {
      messages.push({ role: msg.role, content });
      continue;
    }
    if (Array.isArray(content)) {
      const orderedContent = [];
      const reasoningParts = [];
      const toolCalls = [];
      const toolResults = [];

      for (const item of content) {
        if (item === null || typeof item !== "object" || Array.isArray(item)) {
          continue;
        }
        const blockType = item.type;
        if (blockType === "text") {
          if (typeof item.text === "string" && item.text !== "") {
            orderedContent.push({ type: "text", text: item.text });
          }
        } else if (blockType === "image") {
          const source = item.source;
          if (source != null && typeof source === "object") {
            const srcType = source.type;
            const mediaType = source.media_type;
            const data = source.data;
            const url = source.url;
            if (srcType === "url" && url) {
              orderedContent.push({ type: "image_url", image_url: { url } });
            }
            if (srcType === "base64" && data) {
              const mt = mediaType || "image/png";
              orderedContent.push({ type: "image_url", image_url: { url: `data:${mt};base64,${data}` } });
            }
          }
        } else if (blockType === "thinking") {
          if (typeof item.thinking === "string" && item.thinking !== "") {
            reasoningParts.push(item.thinking);
          }
        } else if (blockType === "tool_use") {
          const id = item.id;
          const name = item.name;
          let args = "";
          if (typeof item.input === "string") {
            args = item.input;
          } else if (item.input !== null && item.input !== undefined) {
            args = jsonStringify(item.input);
          }
          if (args === "") {
            args = "{}";
          }
          toolCalls.push({ id, type: "function", function: { name, arguments: args } });
        } else if (blockType === "tool_result") {
          const toolUseID = item.tool_use_id;
          let resultText = "";
          if (typeof item.content === "string") {
            resultText = item.content;
          } else if (Array.isArray(item.content)) {
            const parts = [];
            for (const p of item.content) {
              if (p !== null && typeof p === "object" && p.type === "text" && typeof p.text === "string") {
                parts.push(p.text);
              }
            }
            resultText = parts.join("\n");
          } else if (item.content !== null && item.content !== undefined) {
            resultText = jsonStringify(item.content);
          }
          if (item.is_error === true) {
            resultText = "Error: " + resultText;
          }
          toolResults.push({ role: "tool", tool_call_id: toolUseID, content: resultText });
        }
      }

      const om = { role: msg.role };
      if (orderedContent.length > 0) {
        om.content = orderedContent;
      } else if (toolCalls.length === 0) {
        om.content = "";
      }
      if (reasoningParts.length > 0) {
        om.reasoning_content = reasoningParts.join("\n");
      }
      if (toolCalls.length > 0) {
        om.tool_calls = toolCalls;
      }

      if (msg.role === "user") {
        messages.push(...toolResults);
      }
      if (orderedContent.length > 0 || reasoningParts.length > 0 || toolCalls.length > 0 || toolResults.length === 0) {
        messages.push(om);
      }
      if (msg.role !== "user") {
        messages.push(...toolResults);
      }
      continue;
    }
    const json = jsonStringify(content);
    messages.push({ role: msg.role, content: json !== null ? json : "" });
  }
  return messages;
}

function claudeToOpenAITools(claudeTools) {
  const tools = [];
  for (const ct of claudeTools) {
    let params = ct.input_schema;
    if (params === null || params === undefined) {
      params = { type: "object", properties: {} };
    }
    params = cleanJsonSchema(params);
    let paramsMap = params;
    if (paramsMap === null || typeof paramsMap !== "object" || Array.isArray(paramsMap)) {
      paramsMap = { type: "object", properties: {} };
    }
    tools.push({
      type: "function",
      function: {
        name: ct.name,
        description: ct.description,
        parameters: paramsMap,
      },
    });
  }
  return tools;
}

function convertClaudeToolChoice(choice) {
  if (choice === null || typeof choice !== "object" || Array.isArray(choice)) {
    return choice;
  }
  switch (choice.type) {
    case "auto":
      return "auto";
    case "any":
      return "required";
    case "none":
      return "none";
    case "tool":
      if (typeof choice.name === "string" && choice.name !== "") {
        return { type: "function", function: { name: choice.name } };
      }
  }
  return choice;
}

function convertClaudeRequest(req) {
  const out = {
    model: req.model,
    messages: claudeToOpenAIMessages(req.messages || [], req.system),
    stream: Boolean(req.stream),
    temperature: req.temperature,
    max_tokens: req.max_tokens,
    top_p: req.top_p,
    tools: claudeToOpenAITools(req.tools || []),
    tool_choice: convertClaudeToolChoice(req.tool_choice),
    reasoning_effort: req.reasoning_effort,
    thinking: req.thinking,
    extra_body: {},
  };
  if (req.top_k !== undefined && req.top_k !== null) {
    out.extra_body.top_k = req.top_k;
  }
  if (Array.isArray(req.stop_sequences) && req.stop_sequences.length > 0) {
    out.extra_body.stop = req.stop_sequences.slice();
  }
  if (req.metadata != null && typeof req.metadata === "object") {
    if (typeof req.metadata.user_id === "string" && req.metadata.user_id !== "") {
      out.extra_body.user = req.metadata.user_id;
    }
  }
  return out;
}

// ======================== Chat -> Claude Messages（响应） ========================

function normalizeFinishReason(reason) {
  switch (reason) {
    case "end_turn":
    case "stop_sequence":
    case "stop":
      return "stop";
    case "max_tokens":
    case "length":
      return "length";
    case "tool_use":
    case "tool_calls":
    case "function_call":
      return "tool_calls";
    case "refusal":
    case "content_filter":
      return "content_filter";
    default:
      return reason;
  }
}

function anthropicUsageToChat(usage) {
  if (usage === null || usage === undefined) {
    return null;
  }
  const out = {};
  for (const key of Object.keys(usage)) {
    out[key] = usage[key];
  }
  if (Object.prototype.hasOwnProperty.call(usage, "input_tokens")) {
    out.prompt_tokens = usage.input_tokens;
  }
  if (Object.prototype.hasOwnProperty.call(usage, "output_tokens")) {
    out.completion_tokens = usage.output_tokens;
  }
  const p = toFloat64(out.prompt_tokens);
  const c = toFloat64(out.completion_tokens);
  if (typeof out.prompt_tokens === "number" && typeof out.completion_tokens === "number") {
    out.total_tokens = p + c;
  }
  delete out.input_tokens;
  delete out.output_tokens;
  return out;
}

function usageMapField(fields, key) {
  if (fields === null || fields === undefined) {
    return null;
  }
  const value = fields[key];
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value;
  }
  return null;
}

function buildClaudeUsageCore(upstreamUsage) {
  if (upstreamUsage === null || upstreamUsage === undefined || typeof upstreamUsage !== "object") {
    return null;
  }
  if (Object.keys(upstreamUsage).length === 0) {
    return null;
  }
  const usage = {};
  let u = upstreamUsage.prompt_tokens;
  if (u !== undefined) usage.input_tokens = toFloat64(u);
  else {
    u = upstreamUsage.input_tokens;
    if (u !== undefined) usage.input_tokens = toFloat64(u);
  }
  u = upstreamUsage.completion_tokens;
  if (u !== undefined) usage.output_tokens = toFloat64(u);
  else {
    u = upstreamUsage.output_tokens;
    if (u !== undefined) usage.output_tokens = toFloat64(u);
  }

  const cacheCreation = upstreamUsage.cache_creation_input_tokens;
  if (cacheCreation !== undefined) {
    usage.cache_creation_input_tokens = toFloat64(cacheCreation);
  }
  const cacheRead = upstreamUsage.cache_read_input_tokens;
  if (cacheRead !== undefined) {
    usage.cache_read_input_tokens = toFloat64(cacheRead);
  } else {
    const promptDetails = usageMapField(upstreamUsage, "prompt_tokens_details");
    if (promptDetails) {
      const cached = promptDetails.cached_tokens;
      if (cached !== undefined) {
        usage.cache_read_input_tokens = toFloat64(cached);
      }
    }
  }

  let outputDetails = usageMapField(upstreamUsage, "output_tokens_details");
  if (!outputDetails) {
    outputDetails = usageMapField(upstreamUsage, "completion_tokens_details");
  }
  if (outputDetails) {
    usage.output_tokens_details = outputDetails;
  }
  let serverToolUse = usageMapField(upstreamUsage, "server_tool_use");
  if (serverToolUse) {
    usage.server_tool_use = serverToolUse;
  }
  return usage;
}

function buildClaudeMessageUsage(upstreamUsage) {
  const usage = buildClaudeUsageCore(upstreamUsage) || {};
  const cacheCreation = usageMapField(upstreamUsage, "cache_creation");
  if (cacheCreation) {
    usage.cache_creation = cacheCreation;
  }
  if (upstreamUsage !== null && upstreamUsage !== undefined && typeof upstreamUsage.service_tier === "string" && upstreamUsage.service_tier !== "") {
    usage.service_tier = upstreamUsage.service_tier;
  }
  if (upstreamUsage !== null && upstreamUsage !== undefined && typeof upstreamUsage.inference_geo === "string" && upstreamUsage.inference_geo !== "") {
    usage.inference_geo = upstreamUsage.inference_geo;
  }
  if (!Object.prototype.hasOwnProperty.call(usage, "input_tokens")) {
    usage.input_tokens = 0;
  }
  if (!Object.prototype.hasOwnProperty.call(usage, "output_tokens")) {
    usage.output_tokens = 0;
  }
  return usage;
}

function buildClaudeDeltaUsage(upstreamUsage) {
  const usage = buildClaudeUsageCore(upstreamUsage) || {};
  if (!Object.prototype.hasOwnProperty.call(usage, "output_tokens")) {
    usage.output_tokens = 0;
  }
  return usage;
}

function openAIToClaudeResponse(chatBody, model, wantReasoning) {
  let chat = {
    id: undefined,
    model: undefined,
    created: undefined,
    choices: [],
    usage: null,
  };
  try {
    chat = JSON.parse(chatBody);
  } catch {
    // fall back to defaults
  }
  if (chat === null || typeof chat !== "object") {
    chat = {};
  }

  const content = [];
  let stopReason = "end_turn";

  if (Array.isArray(chat.choices) && chat.choices.length > 0) {
    const choice = chat.choices[0] || {};
    const msg = choice.message || {};
    const fr = choice.finish_reason;

    if (wantReasoning && typeof msg.reasoning_content === "string" && msg.reasoning_content !== "") {
      content.push({ type: "thinking", thinking: msg.reasoning_content });
    }
    if (typeof msg.content === "string" && msg.content !== "") {
      content.push({ type: "text", text: msg.content });
    }
    if (Array.isArray(msg.tool_calls)) {
      for (const tc of msg.tool_calls) {
        let input;
        try {
          input = JSON.parse(tc.function.arguments);
        } catch {
          input = null;
        }
        if (input === null) {
          input = {};
        }
        content.push({ type: "tool_use", id: tc.id, name: tc.function.name, input });
      }
    }
    switch (fr) {
      case "stop":
        stopReason = "end_turn";
        break;
      case "length":
        stopReason = "max_tokens";
        break;
      case "tool_calls":
      case "function_call":
        stopReason = "tool_use";
        break;
      case "content_filter":
        stopReason = "refusal";
        break;
    }
  }

  if (content.length === 0) {
    content.push({ type: "text", text: "" });
  }

  const resp = {
    id: "msg_" + randomString(24),
    type: "message",
    role: "assistant",
    content,
    model,
    stop_reason: stopReason,
  };
  if (chat.usage && typeof chat.usage === "object") {
    resp.usage = buildClaudeMessageUsage(chat.usage);
  }
  return jsonStringify(resp);
}

// ======================== Claude Messages SSE（响应） ========================

function claudeStreamHandler(respBody, model, keepReasoning) {
  const enc = new TextEncoder();
  return new ReadableStream({
    async start(controller) {
      const msgID = "msg_" + randomString(24);
      let blockIndex = 0;
      let thinkingBlockOpen = false;
      let textBlockOpen = false;
      const toolCallAccumulator = new Map();
      const toolBlockIndices = new Map();
      const toolCallOrder = [];
      let messageStartSent = false;
      let fullUsage = null;
      let finalStopReason = "end_turn";
      let finishSeen = false;

      const emit = (event, data) => {
        controller.enqueue(enc.encode(`event: ${event}\n`));
        controller.enqueue(enc.encode(`data: ${jsonStringify(data)}\n\n`));
      };
      const closeThinkingBlock = () => {
        if (!thinkingBlockOpen) {
          return;
        }
        emit("content_block_stop", {
          type: "content_block_stop",
          index: blockIndex - 1,
          content_block: { type: "thinking" },
        });
        thinkingBlockOpen = false;
      };
      const closeTextBlock = () => {
        if (!textBlockOpen) {
          return;
        }
        emit("content_block_stop", {
          type: "content_block_stop",
          index: blockIndex - 1,
          content_block: { type: "text" },
        });
        textBlockOpen = false;
      };

      try {
        for await (const rawLine of sseBodyLines(respBody)) {
          if (!rawLine.startsWith("data: ")) {
            continue;
          }
          const dataStr = rawLine.slice(6).trim();
          if (dataStr === "[DONE]") {
            break;
          }
          let chunk;
          try {
            chunk = JSON.parse(dataStr);
          } catch {
            continue;
          }
          if (chunk.usage != null && typeof chunk.usage === "object") {
            fullUsage = chunk.usage;
          }

          const choices = chunk.choices;
          if (!Array.isArray(choices) || choices.length === 0) {
            continue;
          }
          const choice = choices[0] || {};
          const delta = choice.delta || {};
          const finishReason = choice.finish_reason;

          if (!messageStartSent) {
            messageStartSent = true;
            emit("message_start", {
              type: "message_start",
              message: {
                id: msgID,
                type: "message",
                role: "assistant",
                content: [],
                model,
                stop_reason: null,
                usage: buildClaudeMessageUsage(fullUsage),
              },
            });
            emit("ping", { type: "ping" });
          }

          if (delta.reasoning_content != null && keepReasoning) {
            const rcStr = typeof delta.reasoning_content === "string" ? delta.reasoning_content : "";
            if (rcStr !== "") {
              closeTextBlock();
              if (!thinkingBlockOpen) {
                emit("content_block_start", {
                  type: "content_block_start",
                  index: blockIndex,
                  content_block: { type: "thinking", thinking: "" },
                });
                thinkingBlockOpen = true;
                blockIndex++;
              }
              emit("content_block_delta", {
                type: "content_block_delta",
                index: blockIndex - 1,
                delta: { type: "thinking_delta", thinking: rcStr },
              });
            }
          }

          if (delta.content != null) {
            const contentStr = typeof delta.content === "string" ? delta.content : "";
            if (contentStr !== "") {
              closeThinkingBlock();
              if (!textBlockOpen) {
                emit("content_block_start", {
                  type: "content_block_start",
                  index: blockIndex,
                  content_block: { type: "text", text: "" },
                });
                textBlockOpen = true;
                blockIndex++;
              }
              emit("content_block_delta", {
                type: "content_block_delta",
                index: blockIndex - 1,
                delta: { type: "text_delta", text: contentStr },
              });
            }
          }

          if (Array.isArray(delta.tool_calls)) {
            for (const rawTC of delta.tool_calls) {
              const tc = rawTC || {};
              const upstreamIndex = typeof tc.index === "number" ? tc.index : 0;

              closeThinkingBlock();
              closeTextBlock();

              if (!toolCallAccumulator.has(upstreamIndex)) {
                let callID = tc.id;
                if (!callID) {
                  callID = "toolu_" + randomString(12);
                }
                const fn = tc.function || {};
                const name = typeof fn.name === "string" ? fn.name : "";
                toolCallAccumulator.set(upstreamIndex, { id: callID, name, args: "" });
                toolCallOrder.push(upstreamIndex);
                toolBlockIndices.set(upstreamIndex, blockIndex);
                emit("content_block_start", {
                  type: "content_block_start",
                  index: blockIndex,
                  content_block: { type: "tool_use", id: callID, name, input: {} },
                });
                blockIndex++;
              }

              const fn = tc.function || {};
              if (typeof fn.arguments === "string" && fn.arguments !== "") {
                toolCallAccumulator.get(upstreamIndex).args += fn.arguments;
                emit("content_block_delta", {
                  type: "content_block_delta",
                  index: toolBlockIndices.get(upstreamIndex),
                  delta: { type: "input_json_delta", partial_json: fn.arguments },
                });
              }
            }
          }

          if (
            finishReason === "stop"
            || finishReason === "length"
            || finishReason === "tool_calls"
            || finishReason === "function_call"
            || finishReason === "content_filter"
          ) {
            if (finishSeen) {
              continue;
            }
            finishSeen = true;
            closeThinkingBlock();
            closeTextBlock();

            for (const idx of toolCallOrder) {
              const acc = toolCallAccumulator.get(idx);
              emit("content_block_stop", {
                type: "content_block_stop",
                index: toolBlockIndices.get(idx),
                content_block: { type: "tool_use", id: acc.id, name: acc.name, input: {} },
              });
            }

            switch (finishReason) {
              case "length":
                finalStopReason = "max_tokens";
                break;
              case "tool_calls":
              case "function_call":
                finalStopReason = "tool_use";
                break;
              case "content_filter":
                finalStopReason = "refusal";
                break;
            }
          }
        }

        closeThinkingBlock();
        closeTextBlock();
        emit("message_delta", {
          type: "message_delta",
          delta: { stop_reason: finalStopReason },
          usage: buildClaudeDeltaUsage(fullUsage),
        });
        emit("message_stop", { type: "message_stop" });
      } finally {
        try {
          controller.close();
        } catch {
          // controller already closed
        }
      }
    },
  });
}

// ======================== Responses API 请求转换 ========================

function collectFunctionOutputs(items) {
  const outputs = {};
  for (const item of items) {
    if (item === null || typeof item !== "object" || Array.isArray(item)) {
      continue;
    }
    const itemType = item.type;
    if (
      itemType !== "function_call_output"
      && itemType !== "apply_patch_call_output"
      && itemType !== "shell_call_output"
    ) {
      continue;
    }
    const callID = typeof item.call_id === "string" ? item.call_id : "";
    if (callID === "") {
      continue;
    }
    if (typeof item.output === "string") {
      outputs[callID] = item.output;
    } else {
      const json = jsonStringify(item.output);
      outputs[callID] = json !== null ? json : "";
    }
  }
  return outputs;
}

function buildBuiltInToolCallArguments(itemType, elem) {
  if (typeof elem.arguments === "string" && elem.arguments !== "") {
    return elem.arguments;
  }
  const payload = {};
  if (itemType === "apply_patch_call") {
    if (typeof elem.input === "string" && elem.input !== "") {
      payload.input = elem.input;
    }
    if (elem.operation !== null && elem.operation !== undefined) {
      payload.operation = elem.operation;
    }
  } else if (itemType === "shell_call") {
    for (const key of ["command", "timeout_ms", "working_directory", "max_output_tokens"]) {
      if (elem[key] !== undefined && elem[key] !== null) {
        payload[key] = elem[key];
      }
    }
  }
  if (Object.keys(payload).length === 0) {
    Object.assign(payload, elem);
  }
  const json = jsonStringify(payload);
  return json !== null ? json : "{}";
}

function responsesInputToMessages(input, instructions) {
  const messages = [];
  if (instructions !== "") {
    messages.push({ role: "system", content: instructions });
  }
  if (typeof input === "string") {
    messages.push({ role: "user", content: input });
    return messages;
  }
  if (Array.isArray(input)) {
    const functionOutputs = collectFunctionOutputs(input);
    for (const item of input) {
      if (typeof item === "string") {
        messages.push({ role: "user", content: item });
        continue;
      }
      if (item === null || typeof item !== "object" || Array.isArray(item)) {
        const json = jsonStringify(item);
        messages.push({ role: "user", content: json !== null ? json : "" });
        continue;
      }
      const itemType = item.type || "";
      switch (itemType) {
        case "function_call":
        case "tool_call":
        case "apply_patch_call":
        case "shell_call": {
          let callID = typeof item.call_id === "string" ? item.call_id : "";
          if (callID === "") {
            callID = typeof item.id === "string" ? item.id : "";
          }
          let name = typeof item.name === "string" ? item.name : "";
          if (name === "") {
            if (itemType === "apply_patch_call") {
              name = "apply_patch";
            } else if (itemType === "shell_call") {
              name = "shell";
            }
          }
          let args = typeof item.arguments === "string" ? item.arguments : "";
          if (name === "" && item.tool_use != null && typeof item.tool_use === "object") {
            name = item.tool_use.name;
            callID = item.tool_use.id;
            if (typeof item.tool_use.arguments === "string") {
              args = item.tool_use.arguments;
            } else if (item.tool_use.input !== undefined) {
              const json = jsonStringify(item.tool_use.input);
              args = json !== null ? json : "";
            }
          }
          if (args === "") {
            args = buildBuiltInToolCallArguments(itemType, item);
          }
          if (args === "") {
            args = "{}";
          }
          messages.push({
            role: "assistant",
            content: "",
            tool_calls: [{ id: callID, type: "function", function: { name, arguments: args } }],
          });
          if (callID !== "") {
            let output = functionOutputs[callID];
            if (output === undefined || output === "") {
              output = "[tool output missing]";
            }
            messages.push({ role: "tool", tool_call_id: callID, content: output });
          }
          break;
        }
        case "function_call_output":
        case "tool_result":
        case "apply_patch_call_output":
        case "shell_call_output": {
          let callID = typeof item.call_id === "string" ? item.call_id : "";
          if (callID === "") {
            callID = typeof item.tool_use_id === "string" ? item.tool_use_id : "";
          }
          if (callID !== "") {
            let output = functionOutputs[callID];
            if (output === undefined || output === "") {
              if (typeof item.output === "string") {
                output = item.output;
              } else if (item.output !== null && item.output !== undefined) {
                const json = jsonStringify(item.output);
                output = json !== null ? json : "";
              }
            }
            if (output === undefined || output === "") {
              const json = jsonStringify(item);
              output = json !== null ? json : "";
            }
            if (output === "") {
              output = "[tool output missing]";
            }
            messages.push({ role: "tool", tool_call_id: callID, content: output });
          }
          break;
        }
        case "reasoning": {
          const text = extractTextFromContentParts(item.summary);
          if (text !== "") {
            messages.push({ role: "assistant", content: "", reasoning_content: text });
          }
          break;
        }
        case "message":
        case "": {
          let role = typeof item.role === "string" && item.role !== "" ? item.role : "user";
          if (role === "developer") {
            role = "system";
          }
          const content = responsesContentToMessageContent(item.content);
          messages.push({ role, content });
          break;
        }
        default: {
          let role = typeof item.role === "string" && item.role !== "" ? item.role : "user";
          let content = responsesContentToMessageContent(item.content);
          let emptyContent = false;
          if (content === null) {
            emptyContent = true;
          } else if (typeof content === "string") {
            emptyContent = content === "";
          } else if (Array.isArray(content)) {
            emptyContent = content.length === 0;
          }
          if (emptyContent) {
            const json = jsonStringify(item);
            if (json !== null) {
              content = json;
            } else {
              continue;
            }
          }
          messages.push({ role, content });
        }
      }
    }
    return messages;
  }
  const json = jsonStringify(input);
  messages.push({ role: "user", content: json !== null ? json : "" });
  return messages;
}

function extractTextFromContentParts(content) {
  if (Array.isArray(content)) {
    const texts = [];
    for (const part of content) {
      if (part !== null && typeof part === "object") {
        if (part.type === "input_text" || part.type === "output_text") {
          if (typeof part.text === "string") {
            texts.push(part.text);
          }
        }
      }
    }
    return texts.join("\n");
  }
  if (typeof content === "string") {
    return content;
  }
  return "";
}

function convertResponsesContentPart(part) {
  const partType = part.type;
  switch (partType) {
    case "input_text":
    case "output_text":
    case "text": {
      const text = typeof part.text === "string" ? part.text : "";
      if (text === "") {
        return null;
      }
      return { type: "text", text };
    }
    case "input_image": {
      const imageURL = typeof part.image_url === "string" ? part.image_url : "";
      if (imageURL === "") {
        return null;
      }
      const imageURLValue = { url: imageURL };
      if (typeof part.detail === "string" && part.detail !== "") {
        imageURLValue.detail = part.detail;
      }
      return { type: "image_url", image_url: imageURLValue };
    }
    default:
      return null;
  }
}

function responsesContentToMessageContent(content) {
  if (content === null || content === undefined) {
    return null;
  }
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    const json = jsonStringify(content);
    return json !== null ? json : null;
  }

  const convertedParts = [];
  const texts = [];
  let onlyTextParts = true;

  for (const rawPart of content) {
    if (rawPart === null || typeof rawPart !== "object" || Array.isArray(rawPart)) {
      continue;
    }
    const convertedPart = convertResponsesContentPart(rawPart);
    if (!convertedPart) {
      let text = extractTextFromContentParts([rawPart]);
      if (text === "") {
        const json = jsonStringify(rawPart);
        text = json !== null ? json : "";
      }
      convertedParts.push({ type: "text", text });
      texts.push(text);
      continue;
    }
    if (convertedPart.type !== "text") {
      onlyTextParts = false;
    }
    if (typeof convertedPart.text === "string" && convertedPart.text !== "") {
      texts.push(convertedPart.text);
    }
    convertedParts.push(convertedPart);
  }

  if (convertedParts.length === 0) {
    return "";
  }
  if (onlyTextParts) {
    return texts.join("\n");
  }
  return convertedParts;
}

function convertResponsesTools(tools) {
  const converted = [];
  for (const tool of tools) {
    const fn = responsesToolFunction(tool);
    if (fn === null) {
      continue;
    }
    converted.push({ type: "function", function: fn });
  }
  return converted;
}

function responsesToolFunction(tool) {
  switch (tool.type) {
    case "function": {
      let fn = {
        name: tool.name,
        description: tool.description,
        parameters: tool.parameters,
      };
      if (tool.function !== null && tool.function !== undefined) {
        fn = tool.function;
      }
      if (fn.parameters === null || fn.parameters === undefined) {
        fn = { ...fn, parameters: { type: "object", properties: {} } };
      }
      return fn;
    }
    case "apply_patch":
      return {
        name: "apply_patch",
        description: "Create, update, or delete files using a structured patch operation or unified diff.",
        parameters: {
          type: "object",
          properties: {
            input: { type: "string", description: "Patch diff or patch instructions to apply." },
            operation: { type: "object", description: "Structured patch operation, including file action and diff payload." },
          },
        },
      };
    case "shell":
      return {
        name: "shell",
        description: "Run a shell command in the local workspace and return stdout, stderr, and exit details.",
        parameters: {
          type: "object",
          properties: {
            command: { type: "string", description: "Shell command to execute." },
            timeout_ms: { type: "integer", description: "Optional timeout in milliseconds." },
            working_directory: { type: "string", description: "Optional working directory for the command." },
            max_output_tokens: { type: "integer", description: "Optional output budget hint." },
          },
          required: ["command"],
        },
      };
    default:
      return null;
  }
}

function responsesToolName(tool) {
  switch (tool.type) {
    case "function":
      if (tool.function !== null && tool.function !== undefined && tool.function.name) {
        return tool.function.name;
      }
      return tool.name;
    case "apply_patch":
      return "apply_patch";
    case "shell":
      return "shell";
    default:
      return "";
  }
}

function responsesToolKindMap(tools) {
  const kinds = {};
  for (const tool of tools) {
    const name = responsesToolName(tool);
    if (name === "") {
      continue;
    }
    kinds[name] = tool.type;
  }
  return kinds;
}

function toolCallOutputType(name, kinds) {
  switch (kinds[name]) {
    case "apply_patch":
      return "apply_patch_call";
    case "shell":
      return "shell_call";
    default:
      return "function_call";
  }
}

function convertResponsesToolChoice(choice) {
  if (choice === null || typeof choice !== "object" || Array.isArray(choice)) {
    return choice;
  }
  if (choice.type === "function" && typeof choice.name === "string" && choice.name !== "") {
    return { type: "function", function: { name: choice.name } };
  }
  const choiceType = typeof choice.type === "string" ? choice.type : "";
  if (choiceType === "apply_patch" || choiceType === "shell") {
    return { type: "function", function: { name: choiceType } };
  }
  return choice;
}

function parseJSONString(input) {
  if (input === "") {
    return null;
  }
  try {
    return JSON.parse(input);
  } catch {
    return null;
  }
}

function buildResponseToolCallItem(tc, outputType) {
  if (outputType === "apply_patch_call" || outputType === "shell_call") {
    const item = {
      id: (outputType === "apply_patch_call" ? "apc_" : "shc_") + tc.id,
      type: outputType,
      status: "completed",
      call_id: tc.id,
    };
    const parsed = parseJSONString(tc.function.arguments);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      Object.assign(item, parsed);
    } else if (tc.function.arguments !== "") {
      item.arguments = tc.function.arguments;
    }
    return item;
  }
  return {
    id: "fc_" + tc.id,
    type: "function_call",
    status: "completed",
    arguments: tc.function.arguments,
    call_id: tc.id,
    name: tc.function.name,
  };
}

function chatContentToResponsesContent(content) {
  if (content === null || content === undefined) {
    return null;
  }
  if (typeof content === "string") {
    if (content === "") {
      return [];
    }
    return [{ type: "output_text", text: content, annotations: [], logprobs: [] }];
  }
  if (Array.isArray(content)) {
    const parts = [];
    const texts = [];
    for (const rawPart of content) {
      if (rawPart === null || typeof rawPart !== "object" || Array.isArray(rawPart)) {
        continue;
      }
      const partType = rawPart.type;
      if (partType === "text" || partType === "input_text" || partType === "output_text") {
        const text = typeof rawPart.text === "string" ? rawPart.text : "";
        if (text === "") {
          continue;
        }
        let annotations = rawPart.annotations;
        if (annotations === undefined) {
          annotations = [];
        }
        let logprobs = rawPart.logprobs;
        if (logprobs === undefined) {
          logprobs = [];
        }
        texts.push(text);
        parts.push({ type: "output_text", text, annotations, logprobs });
      }
    }
    return parts;
  }
  const json = jsonStringify(content);
  const text = json !== null ? json : "";
  return [{ type: "output_text", text, annotations: [], logprobs: [] }];
}

function responsesOutcome(finishReason) {
  if (finishReason === "length") {
    return {
      status: "incomplete",
      event: "response.incomplete",
      incomplete_details: { reason: "max_output_tokens" },
    };
  }
  return { status: "completed", event: "response.completed", incomplete_details: null };
}

function applyResponsesRequestEcho(response, req) {
  if (req.metadata !== null && req.metadata !== undefined) {
    response.metadata = jsonClone(req.metadata);
  }
  if (req.reasoning != null && typeof req.reasoning === "object" && req.reasoning.effort) {
    response.reasoning = { effort: req.reasoning.effort };
  }
  if (req.parallel_tool_calls !== undefined && req.parallel_tool_calls !== null) {
    response.parallel_tool_calls = req.parallel_tool_calls;
  }
  if (req.temperature !== undefined && req.temperature !== null) {
    response.temperature = req.temperature;
  }
  if (req.top_p !== undefined && req.top_p !== null) {
    response.top_p = req.top_p;
  }
  if (req.max_output_tokens !== undefined && req.max_output_tokens !== null) {
    response.max_output_tokens = req.max_output_tokens;
  }
  if (req.store !== undefined && req.store !== null) {
    response.store = req.store;
  }
}

class OutputIndexAllocator {
  constructor() {
    this.next = 0;
  }
  allocate() {
    return this.next++;
  }
  get len() {
    return this.next;
  }
}

// ======================== Responses 非流式转换 ========================

function convertChatToResponses(chatBody, model, wantReasoning, tools, toolChoice) {
  let chat;
  try {
    chat = JSON.parse(chatBody);
  } catch {
    chat = {};
  }
  if (chat === null || typeof chat !== "object") {
    chat = {};
  }

  let reasoning = "";
  let finishReason = "";
  let toolCalls = [];
  let messageContent = null;
  const toolKinds = responsesToolKindMap(tools || []);

  if (Array.isArray(chat.choices) && chat.choices.length > 0) {
    const choice = chat.choices[0] || {};
    const msg = choice.message || {};
    messageContent = chatContentToResponsesContent(msg.content);
    if (typeof msg.refusal === "string" && msg.refusal !== "") {
      messageContent = [{ type: "refusal", refusal: msg.refusal }];
    }
    if (wantReasoning) {
      reasoning = typeof msg.reasoning_content === "string" ? msg.reasoning_content : "";
    }
    toolCalls = Array.isArray(msg.tool_calls) ? msg.tool_calls : [];
    finishReason = choice.finish_reason;
  }

  const outcome = responsesOutcome(finishReason);
  const status = outcome.status;
  const responses = {
    id: chat.id,
    object: "response",
    status,
    background: false,
    error: null,
    incomplete_details: outcome.incomplete_details,
    model,
    created_at: chat.created,
  };
  if (Array.isArray(tools) && tools.length > 0) {
    responses.tools = tools;
  }
  if (toolChoice !== null && toolChoice !== undefined) {
    responses.tool_choice = toolChoice;
  }
  const outputID = "msg_" + chat.id + "_0";
  const output = [];
  if (reasoning !== "") {
    output.push({
      id: "rs_" + chat.id,
      type: "reasoning",
      encrypted_content: "",
      summary: [{ type: "summary_text", text: reasoning }],
    });
  }
  if (messageContent != null && messageContent.length > 0) {
    output.push({
      id: outputID,
      type: "message",
      status,
      role: "assistant",
      content: messageContent,
    });
  }
  for (const tc of toolCalls) {
    const item = buildResponseToolCallItem(tc, toolCallOutputType(tc.function.name, toolKinds));
    item.status = status;
    output.push(item);
  }
  responses.output = output;

  if (chat.usage && typeof chat.usage === "object") {
    const usage = {};
    const u = chat.usage;
    if (u.prompt_tokens !== undefined) {
      usage.input_tokens = u.prompt_tokens;
    }
    if (u.prompt_tokens_details !== undefined) {
      usage.input_tokens_details = u.prompt_tokens_details;
    } else {
      usage.input_tokens_details = { cached_tokens: 0 };
    }
    if (u.completion_tokens !== undefined) {
      usage.output_tokens = u.completion_tokens;
    }
    if (u.completion_tokens_details !== undefined) {
      usage.output_tokens_details = u.completion_tokens_details;
    }
    if (u.total_tokens !== undefined) {
      usage.total_tokens = u.total_tokens;
    }
    if (u.input_tokens !== undefined && usage.input_tokens === undefined) {
      usage.input_tokens = u.input_tokens;
    }
    if (u.output_tokens !== undefined && usage.output_tokens === undefined) {
      usage.output_tokens = u.output_tokens;
    }
    responses.usage = usage;
  }
  return jsonStringify(responses);
}

// ======================== Responses SSE（流） ========================

function responsesStreamHandler(respBody, model, wantReasoning, tools, toolChoice, originalReq) {
  const enc = new TextEncoder();
  return new ReadableStream({
    async start(controller) {
      const toolKinds = responsesToolKindMap(tools || []);
      const allocator = new OutputIndexAllocator();

      let responseID = "resp_" + Date.now() + "_" + randomString(8);
      let reasoningID = "rs_" + responseID;
      const msgID = "msg_" + responseID + "_0";
      let createdAt = Math.floor(Date.now() / 1000);
      let seq = 0;

      let reasoningStarted = false;
      let reasoningDone = false;
      let messageStarted = false;
      let messageDone = false;
      let fullReasoning = "";
      let fullText = "";
      let totalUsage = null;
      let createdSent = false;
      let terminalStatus = "completed";
      let terminalEvent = "response.completed";
      let itemStatus = "completed";
      const toolCalls = new Map();
      const toolOrder = [];
      let reasoningOutputIndex = -1;
      let messageIndex = -1;

      const emit = (event, data) => {
        controller.enqueue(enc.encode(`event: ${event}\n`));
        controller.enqueue(enc.encode(`data: ${jsonStringify(data)}\n\n`));
      };

      const messageOutputIndex = () => {
        if (messageIndex < 0) {
          messageIndex = allocator.allocate();
        }
        return messageIndex;
      };
      const reasoningItem = (status) => {
        const item = { id: reasoningID, type: "reasoning", summary: [] };
        if (status !== "") {
          item.status = status;
        }
        if (status === "completed") {
          item.encrypted_content = "";
        }
        if (fullReasoning !== "") {
          item.summary = [{ type: "summary_text", text: fullReasoning }];
        }
        return item;
      };
      const messageItem = (status) => ({
        id: msgID,
        type: "message",
        status,
        content: [{ type: "output_text", annotations: [], logprobs: [], text: fullText }],
        role: "assistant",
      });

      const emitReasoningDone = () => {
        if (!reasoningStarted || reasoningDone) {
          return;
        }
        seq++;
        emit("response.reasoning_summary_text.done", {
          type: "response.reasoning_summary_text.done",
          sequence_number: seq,
          item_id: reasoningID,
          output_index: reasoningOutputIndex,
          summary_index: 0,
          text: fullReasoning,
        });
        seq++;
        emit("response.reasoning_summary_part.done", {
          type: "response.reasoning_summary_part.done",
          sequence_number: seq,
          item_id: reasoningID,
          output_index: reasoningOutputIndex,
          summary_index: 0,
          part: { type: "summary_text", text: fullReasoning },
        });
        seq++;
        emit("response.output_item.done", {
          type: "response.output_item.done",
          sequence_number: seq,
          output_index: reasoningOutputIndex,
          item: reasoningItem(itemStatus),
        });
        reasoningDone = true;
      };

      const emitMessageDone = () => {
        if (!messageStarted || messageDone) {
          return;
        }
        const idx = messageOutputIndex();
        seq++;
        emit("response.output_text.done", {
          type: "response.output_text.done",
          sequence_number: seq,
          item_id: msgID,
          output_index: idx,
          content_index: 0,
          text: fullText,
          logprobs: [],
        });
        seq++;
        emit("response.content_part.done", {
          type: "response.content_part.done",
          sequence_number: seq,
          item_id: msgID,
          output_index: idx,
          content_index: 0,
          part: { type: "output_text", annotations: [], logprobs: [], text: fullText },
        });
        seq++;
        emit("response.output_item.done", {
          type: "response.output_item.done",
          sequence_number: seq,
          output_index: idx,
          item: messageItem(itemStatus),
        });
        messageDone = true;
      };

      const emitToolCallDone = (idx, call) => {
        if (call.done) {
          return;
        }
        call.done = true;
        const itemID = call.item_id;
        const callID = call.call_id;
        const name = call.name;
        const args = call.arguments;
        seq++;
        emit("response.function_call_arguments.done", {
          type: "response.function_call_arguments.done",
          sequence_number: seq,
          item_id: itemID,
          output_index: idx,
          name,
          arguments: args,
        });
        seq++;
        const itemType = call.item_type || "function_call";
        const item = buildResponseToolCallItem(
          { id: callID, function: { name, arguments: args } },
          itemType,
        );
        item.status = itemStatus;
        emit("response.output_item.done", {
          type: "response.output_item.done",
          sequence_number: seq,
          output_index: idx,
          item,
        });
      };

      const terminal = () => {
        const output = new Array(allocator.len);
        if (reasoningStarted) {
          output[reasoningOutputIndex] = reasoningItem(itemStatus);
        }
        if (messageStarted) {
          output[messageIndex] = messageItem(itemStatus);
        }
        for (const idx of toolOrder) {
          const call = toolCalls.get(idx);
          const itemType = call.item_type || "function_call";
          const item = buildResponseToolCallItem(
            { id: call.call_id, function: { name: call.name, arguments: call.arguments } },
            itemType,
          );
          item.status = itemStatus;
          output[call.output_index] = item;
        }

        const completedResponse = {
          id: responseID,
          object: "response",
          created_at: createdAt,
          status: terminalStatus,
          background: false,
          error: null,
          incomplete_details: null,
          model,
          output,
        };
        if (terminalStatus === "incomplete") {
          completedResponse.incomplete_details = { reason: "max_output_tokens" };
        }
        applyResponsesRequestEcho(completedResponse, originalReq);
        if (Array.isArray(tools) && tools.length > 0) {
          completedResponse.tools = tools;
        }
        if (toolChoice !== null && toolChoice !== undefined) {
          completedResponse.tool_choice = toolChoice;
        }

        if (totalUsage !== null && typeof totalUsage === "object") {
          const usage = {};
          if (totalUsage.prompt_tokens !== undefined) {
            usage.input_tokens = totalUsage.prompt_tokens;
          }
          if (totalUsage.prompt_tokens_details !== undefined) {
            usage.input_tokens_details = totalUsage.prompt_tokens_details;
          } else {
            usage.input_tokens_details = { cached_tokens: 0 };
          }
          if (totalUsage.completion_tokens !== undefined) {
            usage.output_tokens = totalUsage.completion_tokens;
          }
          if (totalUsage.completion_tokens_details !== undefined) {
            usage.output_tokens_details = totalUsage.completion_tokens_details;
          }
          if (totalUsage.total_tokens !== undefined) {
            usage.total_tokens = totalUsage.total_tokens;
          }
          if (totalUsage.input_tokens !== undefined && usage.input_tokens === undefined) {
            usage.input_tokens = totalUsage.input_tokens;
          }
          if (totalUsage.output_tokens !== undefined && usage.output_tokens === undefined) {
            usage.output_tokens = totalUsage.output_tokens;
          }
          completedResponse.usage = usage;
        }

        seq++;
        emit(terminalEvent, {
          type: terminalEvent,
          sequence_number: seq,
          response: completedResponse,
        });
      };

      try {
        for await (const rawLine of sseBodyLines(respBody)) {
          if (!rawLine.startsWith("data: ")) {
            continue;
          }
          const dataStr = rawLine.slice(6).trim();
          if (dataStr === "[DONE]") {
            break;
          }
          let chunk;
          try {
            chunk = JSON.parse(dataStr);
          } catch {
            continue;
          }

          if (!createdSent) {
            if (typeof chunk.id === "string" && chunk.id !== "") {
              responseID = chunk.id;
              reasoningID = "rs_" + responseID;
            }
            if (typeof chunk.created === "number") {
              createdAt = chunk.created;
            }
            seq++;
            emit("response.created", {
              type: "response.created",
              sequence_number: seq,
              response: {
                id: responseID,
                object: "response",
                created_at: createdAt,
                status: "in_progress",
                background: false,
                error: null,
                output: [],
              },
            });
            seq++;
            emit("response.in_progress", {
              type: "response.in_progress",
              sequence_number: seq,
              response: { id: responseID, object: "response", created_at: createdAt, status: "in_progress" },
            });
            createdSent = true;
          }

          if (chunk.usage != null && typeof chunk.usage === "object") {
            totalUsage = chunk.usage;
          }

          const choices = chunk.choices;
          if (!Array.isArray(choices) || choices.length === 0) {
            continue;
          }
          const choice = choices[0] || {};
          const delta = choice.delta || {};
          const finishReason = choice.finish_reason;

          if (delta.reasoning_content != null && wantReasoning) {
            const rcStr = typeof delta.reasoning_content === "string" ? delta.reasoning_content : "";
            if (rcStr !== "") {
              if (!reasoningStarted) {
                reasoningOutputIndex = allocator.allocate();
                seq++;
                emit("response.output_item.added", {
                  type: "response.output_item.added",
                  sequence_number: seq,
                  output_index: reasoningOutputIndex,
                  item: reasoningItem("in_progress"),
                });
                seq++;
                emit("response.reasoning_summary_part.added", {
                  type: "response.reasoning_summary_part.added",
                  sequence_number: seq,
                  item_id: reasoningID,
                  output_index: reasoningOutputIndex,
                  summary_index: 0,
                  part: { type: "summary_text", text: "" },
                });
                reasoningStarted = true;
              }
              fullReasoning += rcStr;
              seq++;
              emit("response.reasoning_summary_text.delta", {
                type: "response.reasoning_summary_text.delta",
                sequence_number: seq,
                item_id: reasoningID,
                output_index: reasoningOutputIndex,
                summary_index: 0,
                delta: rcStr,
              });
            }
          }

          const contentStr = typeof delta.content === "string" ? delta.content : "";
          if (contentStr !== "") {
            if (!messageStarted) {
              const idx = messageOutputIndex();
              seq++;
              emit("response.output_item.added", {
                type: "response.output_item.added",
                sequence_number: seq,
                output_index: idx,
                item: { id: msgID, type: "message", status: "in_progress", content: [], role: "assistant" },
              });
              seq++;
              emit("response.content_part.added", {
                type: "response.content_part.added",
                sequence_number: seq,
                item_id: msgID,
                output_index: idx,
                content_index: 0,
                part: { type: "output_text", annotations: [], logprobs: [], text: "" },
              });
              messageStarted = true;
            }
            fullText += contentStr;
            seq++;
            emit("response.output_text.delta", {
              type: "response.output_text.delta",
              sequence_number: seq,
              item_id: msgID,
              output_index: messageOutputIndex(),
              content_index: 0,
              delta: contentStr,
              logprobs: [],
            });
          }

          if (Array.isArray(delta.tool_calls)) {
            for (const rawToolCall of delta.tool_calls) {
              const tc = rawToolCall || {};
              const upstreamIndex = typeof tc.index === "number" ? tc.index : 0;
              let call = toolCalls.get(upstreamIndex);
              if (!call) {
                const outputIndex = allocator.allocate();
                let callID = tc.id;
                if (!callID) {
                  callID = "call_" + randomString(12);
                }
                const fn = tc.function || {};
                const name = typeof fn.name === "string" ? fn.name : "";
                const itemType = toolCallOutputType(name, toolKinds);
                call = {
                  output_index: outputIndex,
                  item_id: "fc_" + callID,
                  call_id: callID,
                  name,
                  arguments: "",
                  done: false,
                  item_type: itemType,
                };
                toolCalls.set(upstreamIndex, call);
                toolOrder.push(upstreamIndex);
                seq++;
                emit("response.output_item.added", {
                  type: "response.output_item.added",
                  sequence_number: seq,
                  output_index: outputIndex,
                  item: {
                    id: call.item_id,
                    type: itemType,
                    status: "in_progress",
                    arguments: "",
                    call_id: callID,
                    name,
                  },
                });
              }
              const fn = tc.function || {};
              if (typeof fn.name === "string" && fn.name !== "") {
                call.name = fn.name;
                if (call.item_type === "function_call") {
                  call.item_type = toolCallOutputType(fn.name, toolKinds);
                }
              }
              if (typeof fn.arguments === "string" && fn.arguments !== "") {
                call.arguments += fn.arguments;
                seq++;
                emit("response.function_call_arguments.delta", {
                  type: "response.function_call_arguments.delta",
                  sequence_number: seq,
                  item_id: call.item_id,
                  output_index: call.output_index,
                  delta: fn.arguments,
                });
              }
            }
          }

          if (finishReason === "stop" || finishReason === "length" || finishReason === "content_filter") {
            if (finishReason === "length") {
              terminalStatus = "incomplete";
              terminalEvent = "response.incomplete";
              itemStatus = "incomplete";
            }
            emitReasoningDone();
            if (!messageStarted && toolCalls.size === 0) {
              const idx = messageOutputIndex();
              seq++;
              emit("response.output_item.added", {
                type: "response.output_item.added",
                sequence_number: seq,
                output_index: idx,
                item: { id: msgID, type: "message", status: "in_progress", content: [], role: "assistant" },
              });
              seq++;
              emit("response.content_part.added", {
                type: "response.content_part.added",
                sequence_number: seq,
                item_id: msgID,
                output_index: idx,
                content_index: 0,
                part: { type: "output_text", annotations: [], logprobs: [], text: "" },
              });
              messageStarted = true;
            }
            emitMessageDone();
            for (const idx of toolOrder) {
              emitToolCallDone(toolCalls.get(idx).output_index, toolCalls.get(idx));
            }
          }
        }

        emitReasoningDone();
        emitMessageDone();
        for (const idx of toolOrder) {
          emitToolCallDone(toolCalls.get(idx).output_index, toolCalls.get(idx));
        }
        terminal();
      } finally {
        try {
          controller.close();
        } catch {
          // already closed
        }
      }
    },
  });
}

// ======================== HTTP 入口 ========================

async function handleMessages(request) {
  let body;
  try {
    body = JSON.parse(await request.text());
  } catch {
    return jsonResponseBody(400, { type: "error", error: { type: "invalid_request_error", message: "Invalid JSON" } });
  }
  if (body === null || typeof body !== "object" || Array.isArray(body)) {
    return jsonResponseBody(400, { type: "error", error: { type: "invalid_request_error", message: "Invalid JSON" } });
  }

  const req = {
    model: toUpstreamModelId(body.model),
    messages: Array.isArray(body.messages) ? body.messages : [],
    system: body.system,
    max_tokens: body.max_tokens,
    temperature: body.temperature,
    top_p: body.top_p,
    top_k: body.top_k,
    stream: body.stream === true,
    tools: Array.isArray(body.tools) ? body.tools : undefined,
    tool_choice: body.tool_choice,
    thinking: body.thinking,
    metadata: body.metadata,
    stop_sequences: body.stop_sequences || [],
  };

  const chatReq = convertClaudeRequest(req);
  chatReq.messages = fixToolCallGaps(chatReq.messages);
  const wantReasoning = !isThinkingDisabled(req.thinking);
  chatReq.messages = ensureReasoningContent(chatReq.messages, wantReasoning);

  const bodyObj = openAIRequestToBody(chatReq);
  if (chatReq.stream) {
    bodyObj.stream_options = { include_usage: true };
  }
  const init = chatRequestInit(request, bodyObj);
  const upstream = await fetch(chatUpstreamUrl(request.url), init);

  if (chatReq.stream) {
    if (!upstream.ok) {
      return upstream;
    }
    const publicModel = publicModelFromUpstream(body.model);
    return sseResponse(claudeStreamHandler(upstream.body, publicModel, wantReasoning));
  }

  const raw = await upstream.arrayBuffer();
  const text = new TextDecoder().decode(raw);
  const publicModel = publicModelFromUpstream(body.model);
  const claudeBody = openAIToClaudeResponse(text, publicModel, wantReasoning);
  const response = new Response(claudeBody, {
    status: upstream.status || 200,
    statusText: upstream.statusText,
    headers: transformResponseHeaders(upstream.headers),
  });
  response.headers.set("content-type", "application/json; charset=utf-8");
  return response;
}

function publicModelFromUpstream(upstreamModel) {
  if (typeof upstreamModel === "string" && upstreamModel.endsWith(FREE_MODEL_SUFFIX)) {
    return upstreamModel.slice(0, -FREE_MODEL_SUFFIX.length);
  }
  return upstreamModel;
}

async function handleResponses(request) {
  let req;
  try {
    req = JSON.parse(await request.text());
  } catch {
    return jsonResponseBody(400, { error: { message: "Invalid JSON" } });
  }
  if (req === null || typeof req !== "object" || Array.isArray(req)) {
    return jsonResponseBody(400, { error: { message: "Invalid JSON" } });
  }

  const originalReq = req;
  const model = typeof req.model === "string" && req.model !== "" ? req.model : "deepseek-v4-flash";
  const upstreamModel = toUpstreamModelId(model);

  const messages = Array.isArray(req.messages) ? req.messages.slice() : [];
  if (messages.length === 0) {
    messages.push(...responsesInputToMessages(req.input, req.instructions || ""));
  } else if (req.instructions) {
    messages.unshift({ role: "system", content: req.instructions });
  }

  const chatReq = {
    model: upstreamModel,
    messages,
    stream: req.stream === true,
    temperature: req.temperature,
    max_tokens: req.max_output_tokens,
    top_p: req.top_p,
    tool_choice: req.tool_choice !== undefined ? convertResponsesToolChoice(req.tool_choice) : undefined,
    tools: !Array.isArray(req.tools) || req.tools.length === 0 ? undefined : convertResponsesTools(req.tools),
    reasoning_effort: req.reasoning != null && typeof req.reasoning === "object" && req.reasoning.effort !== "none" ? req.reasoning.effort : "",
    thinking: req.thinking,
    extra_body: {},
  };

  if (req.parallel_tool_calls !== undefined && req.parallel_tool_calls !== null) {
    chatReq.extra_body.parallel_tool_calls = req.parallel_tool_calls;
  }
  if (req.stop !== undefined && req.stop !== null) {
    chatReq.extra_body.stop = req.stop;
  }
  if (req.frequency_penalty !== undefined && req.frequency_penalty !== null) {
    chatReq.extra_body.frequency_penalty = req.frequency_penalty;
  }
  if (req.presence_penalty !== undefined && req.presence_penalty !== null) {
    chatReq.extra_body.presence_penalty = req.presence_penalty;
  }
  if (typeof req.user === "string" && req.user !== "") {
    chatReq.extra_body.user = req.user;
  }
  if (req.stream) {
    chatReq.extra_body.stream_options = { include_usage: true };
  }

  chatReq.messages = fixToolCallGaps(chatReq.messages);
  const keepReasoning = wantsReasoning(chatReq);
  chatReq.messages = ensureReasoningContent(chatReq.messages, keepReasoning);

  const bodyObj = openAIRequestToBody(chatReq);
  const init = chatRequestInit(request, bodyObj);
  const upstream = await fetch(chatUpstreamUrl(request.url), init);

  if (chatReq.stream) {
    if (!upstream.ok) {
      return upstream;
    }
    return sseResponse(
      responsesStreamHandler(
        upstream.body,
        model,
        !isThinkingDisabled(req.thinking),
        Array.isArray(req.tools) ? req.tools : [],
        req.tool_choice,
        originalReq,
      ),
    );
  }

  const raw = await upstream.arrayBuffer();
  const text = new TextDecoder().decode(raw);
  let responsesBody = convertChatToResponses(text, model, keepReasoning, Array.isArray(req.tools) ? req.tools : [], req.tool_choice);
  let responseMap;
  try {
    responseMap = JSON.parse(responsesBody);
  } catch {
    responseMap = null;
  }
  if (responseMap) {
    applyResponsesRequestEcho(responseMap, originalReq);
    if (Array.isArray(req.tools) && req.tools.length > 0) {
      responseMap.tools = req.tools;
    }
    if (req.tool_choice !== undefined) {
      responseMap.tool_choice = req.tool_choice;
    }
    responsesBody = jsonStringify(responseMap);
  }
  const response = new Response(responsesBody, {
    status: upstream.status || 200,
    statusText: upstream.statusText,
    headers: transformResponseHeaders(upstream.headers),
  });
  response.headers.set("content-type", "application/json; charset=utf-8");
  return response;
}

export default async function onRequest(context) {
  try {
    const request = context.request;
    const url = new URL(request.url);
    const pathname = url.pathname;
    const method = request.method.toUpperCase();

    if (method === "GET" && pathname === MODEL_LIST_PATH) {
      const response = await fetch(upstreamUrl(request.url), await requestInit(request));
      return await filterModelsResponse(response);
    }

    if (method === "POST" && pathname === MESSAGES_PATH) {
      return await handleMessages(request);
    }

    if (method === "POST" && pathname === RESPONSES_PATH) {
      return await handleResponses(request);
    }

    // 透传（GET/其它路径/无法转换的推理请求；/v1/chat/completions 本就是 Chat，
    // 无需格式转换，保持透传以支持原生流式而不过早消费上游 body）
    const response = await fetch(upstreamUrl(request.url), await requestInit(request));
    return response;
  } catch {
    return new Response("Bad Gateway", {
      status: 502,
      headers: {
        "content-type": "text/plain; charset=utf-8",
      },
    });
  }
}

async function mapRequestBody(request) {
  if (request.body === null) {
    return null;
  }

  const rawBody = await request.text();

  try {
    const payload = JSON.parse(rawBody);
    if (
      payload !== null
      && typeof payload === "object"
      && !Array.isArray(payload)
    ) {
      let changed = false;

      if (typeof payload.model === "string" && payload.model.length > 0) {
        const mappedModel = toUpstreamModelId(payload.model);
        if (mappedModel !== payload.model) {
          payload.model = mappedModel;
          changed = true;
        }
      }

      const cleanedTools = sanitizeTools(payload.tools);
      if (cleanedTools !== payload.tools) {
        if (cleanedTools.length === 0) {
          delete payload.tools;
        } else {
          payload.tools = cleanedTools;
        }
        changed = true;
      }

      if (changed) {
        return JSON.stringify(payload);
      }
    }
  } catch {
    // Let the upstream return its native error for malformed JSON.
  }

  return rawBody;
}

async function requestInit(request) {
  const method = request.method.toUpperCase();
  const pathname = new URL(request.url).pathname;
  const init = {
    method,
    headers: forwardHeaders(request.headers),
    redirect: "manual",
    eo: {
      timeoutSetting: {
        connectTimeout: MAX_TIMEOUT_MS,
        readTimeout: MAX_TIMEOUT_MS,
        writeTimeout: MAX_TIMEOUT_MS,
      },
    },
  };

  if (method !== "GET" && method !== "HEAD") {
    init.body = MODEL_REQUEST_PATHS.has(pathname)
      ? await mapRequestBody(request)
      : request.body;
  }

  return init;
}

async function filterModelsResponse(response) {
  if (!response.ok) {
    return response;
  }

  let payload;
  try {
    payload = await response.clone().json();
  } catch {
    return response;
  }

  if (payload === null || typeof payload !== "object" || !Array.isArray(payload.data)) {
    return response;
  }

  const data = [];
  for (const model of payload.data) {
    if (model === null || typeof model !== "object" || Array.isArray(model)) {
      continue;
    }

    const publicId = toPublicModelId(model.id);
    if (publicId !== null) {
      data.push({ ...model, id: publicId });
    }
  }

  const headers = new Headers(response.headers);
  for (const header of TRANSFORMED_RESPONSE_HEADERS) {
    headers.delete(header);
  }
  headers.set("content-type", "application/json; charset=utf-8");

  return new Response(JSON.stringify({ ...payload, data }), {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export {
  FREE_MODEL_SUFFIX,
  HOP_BY_HOP_HEADERS,
  MAX_TIMEOUT_MS,
  MODEL_LIST_PATH,
  MODEL_REQUEST_PATHS,
  PUBLIC_AUTHORIZATION,
  UPSTREAM_ORIGIN,
  UPSTREAM_PREFIX,
  anthropicUsageToChat,
  applyResponsesRequestEcho,
  buildClaudeDeltaUsage,
  buildClaudeMessageUsage,
  buildResponseToolCallItem,
  chatContentToResponsesContent,
  chatUpstreamUrl,
  cleanJsonSchema,
  claudeStreamHandler,
  claudeToOpenAIMessages,
  claudeToOpenAITools,
  collectFunctionOutputs,
  convertChatToResponses,
  convertClaudeRequest,
  convertClaudeToolChoice,
  convertResponse,
  convertResponsesToolChoice,
  convertResponsesTools,
  convertStreamChunkWithUsage,
  ensureReasoningContent,
  extractClaudeSystemText,
  filterModelsResponse,
  fixToolCallGaps,
  forwardHeaders,
  isThinkingDisabled,
  isThinkingEnabled,
  mapRequestBody,
  normalizeFinishReason,
  openAIToClaudeResponse,
  parseJSONString,
  requestInit,
  responsesContentToMessageContent,
  responsesInputToMessages,
  responsesOutcome,
  responsesStreamHandler,
  responsesToolFunction,
  responsesToolKindMap,
  sanitizeTools,
  sseResponse,
  toFloat64,
  toolCallOutputType,
  toPublicModelId,
  toUpstreamModelId,
  transformResponseHeaders,
  upstreamUrl,
  wantsReasoning,
};
