import assert from "node:assert/strict";
import { test } from "node:test";

import onRequest, {
  MAX_TIMEOUT_MS,
  PUBLIC_AUTHORIZATION,
  anthropicUsageToChat,
  buildClaudeMessageUsage,
  buildClaudeDeltaUsage,
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
} from "../edge-functions/v1/[[default]].js";

const encoder = new TextEncoder();

function contextFor(path, init = {}) {
  return {
    request: new Request(`https://proxy.example${path}`, init),
  };
}

async function withMockFetch(mockFetch, callback) {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = mockFetch;

  try {
    return await callback();
  } finally {
    globalThis.fetch = originalFetch;
  }
}

// Reads a ReadableStream to completion and parses SSE event/data pairs.
// The handlers enqueue `event:` and `data:` lines in separate chunks, so we
// accumulate the full text before pairing them.
async function collectSSEEvents(stream) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let full = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    full += decoder.decode(value, { stream: true });
  }
  const events = [];
  let currentEvent = null;
  for (const line of full.split("\n")) {
    if (line.startsWith("event: ")) {
      currentEvent = line.slice(7);
    } else if (line.startsWith("data: ") && currentEvent !== null) {
      const raw = line.slice(6).trim();
      if (raw === "[DONE]") {
        currentEvent = null;
        continue;
      }
      try {
        events.push({ event: currentEvent, data: JSON.parse(raw) });
      } catch {
        // ignore malformed payloads
      }
      currentEvent = null;
    }
  }
  return events;
}

test("maps supported Zen paths and query strings without duplicating /v1", async () => {
  const paths = [
    "/v1/models",
    "/v1/responses",
    "/v1/chat/completions",
    "/v1/messages",
  ];

  for (const path of paths) {
    assert.equal(
      upstreamUrl(`https://proxy.example${path}?stream=true`),
      `https://opencode.ai/zen${path}?stream=true`,
    );
  }
});

test("exposes only free models and strips the -free suffix", async () => {
  const upstreamResponse = new Response(JSON.stringify({
    object: "list",
    data: [
      { id: "deepseek-v4-flash-free", object: "model", owned_by: "opencode" },
      { id: "gpt-5.6-sol", object: "model", owned_by: "opencode" },
      { id: "mimo-v2.5-free", object: "model", owned_by: "opencode" },
      { id: "-free", object: "model", owned_by: "invalid" },
      null,
    ],
  }), {
    status: 200,
    headers: {
      "content-type": "application/json",
      "content-encoding": "gzip",
      "content-length": "999",
      etag: "upstream-etag",
      "x-upstream": "preserved",
    },
  });

  const response = await filterModelsResponse(upstreamResponse);
  const payload = await response.json();

  assert.deepEqual(payload, {
    object: "list",
    data: [
      { id: "deepseek-v4-flash", object: "model", owned_by: "opencode" },
      { id: "mimo-v2.5", object: "model", owned_by: "opencode" },
    ],
  });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("x-upstream"), "preserved");
  assert.equal(response.headers.get("content-encoding"), null);
  assert.equal(response.headers.get("content-length"), null);
  assert.equal(response.headers.get("etag"), null);
});

test("GET /v1/models uses public auth and filters the upstream response", async () => {
  let capturedUrl;
  let capturedInit;

  const response = await withMockFetch(async (url, init) => {
    capturedUrl = url;
    capturedInit = init;
    return new Response(JSON.stringify({
      object: "list",
      data: [
        { id: "deepseek-v4-flash-free", object: "model" },
        { id: "deepseek-v4-pro", object: "model" },
      ],
    }), { headers: { "content-type": "application/json" } });
  }, async () => onRequest(contextFor("/v1/models?source=live", {
    headers: {
      authorization: "Bearer client",
      "x-api-key": "client-value",
    },
  })));

  assert.equal(capturedUrl, "https://opencode.ai/zen/v1/models?source=live");
  assert.equal(capturedInit.headers.get("authorization"), PUBLIC_AUTHORIZATION);
  assert.equal(capturedInit.headers.has("x-api-key"), false);
  assert.deepEqual(await response.json(), {
    object: "list",
    data: [{ id: "deepseek-v4-flash", object: "model" }],
  });
});

test("maps the top-level model for all three inference protocols", async () => {
  // /v1/messages: convertClaudeRequest transforms the request, so the upstream body
  // will be Chat format, not the original Claude format. We verify the upstream
  // receives a Chat body and the client receives a Claude-formatted response.
  {
    let capturedInit;
    const chatResponseBody = JSON.stringify({
      id: "chatcmpl-1",
      model: "deepseek-v4-flash-free",
      choices: [{ message: { role: "assistant", content: "hi" }, finish_reason: "stop", index: 0 }],
      usage: { prompt_tokens: 5, completion_tokens: 2, total_tokens: 7 },
    });
    const upstreamResponse = new Response(chatResponseBody, {
      status: 200,
      headers: { "content-type": "application/json" },
    });

    const response = await withMockFetch(async (_url, init) => {
      capturedInit = init;
      return upstreamResponse;
    }, async () => onRequest(contextFor("/v1/messages", {
      method: "POST",
      headers: {
        authorization: "Bearer ignored",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "deepseek-v4-flash",
        max_tokens: 64,
        messages: [{ role: "user", content: "hello" }],
      }),
    })));

    // The upstream should have received a Chat-format body with -free model
    const upstreamBody = JSON.parse(capturedInit.body);
    assert.equal(upstreamBody.model, "deepseek-v4-flash-free");
    assert.ok(Array.isArray(upstreamBody.messages), "upstream body has messages array");

    // The client should have received a Claude-formatted response (not raw chat)
    const clientBody = await response.json();
    assert.equal(clientBody.type, "message");
    assert.equal(clientBody.role, "assistant");
    assert.ok(Array.isArray(clientBody.content), "Claude response has content array");
    assert.equal(clientBody.stop_reason, "end_turn");
  }

  // /v1/responses: convertChatToResponses transforms the response back to Responses format
  {
    let capturedInit;
    const chatResponseBody = JSON.stringify({
      id: "chatcmpl-2",
      model: "deepseek-v4-flash-free",
      created: 1234567890,
      choices: [{ message: { role: "assistant", content: "world" }, finish_reason: "stop", index: 0 }],
      usage: { prompt_tokens: 3, completion_tokens: 1, total_tokens: 4 },
    });
    const upstreamResponse = new Response(chatResponseBody, {
      status: 200,
      headers: { "content-type": "application/json" },
    });

    const response = await withMockFetch(async (_url, init) => {
      capturedInit = init;
      return upstreamResponse;
    }, async () => onRequest(contextFor("/v1/responses", {
      method: "POST",
      headers: {
        authorization: "Bearer ignored",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "deepseek-v4-flash",
        input: [{ role: "user", content: [{ type: "input_text", text: "hello" }] }],
      }),
    })));

    // Upstream gets Chat format with -free model
    const upstreamBody = JSON.parse(capturedInit.body);
    assert.equal(upstreamBody.model, "deepseek-v4-flash-free");
    assert.ok(Array.isArray(upstreamBody.messages), "upstream body has messages");

    // Client gets Responses format
    const clientBody = await response.json();
    assert.equal(clientBody.object, "response");
    assert.equal(clientBody.status, "completed");
    assert.ok(Array.isArray(clientBody.output), "Responses format has output array");
  }

  // /v1/chat/completions: still passes through as-is (chat format upstream, chat format client)
  {
    let capturedInit;
    const chatResponseBody = JSON.stringify({
      id: "chatcmpl-3",
      choices: [{ message: { role: "assistant", content: "yes" } }],
    });
    const upstreamResponse = new Response(chatResponseBody, {
      status: 200,
      headers: { "content-type": "application/json" },
    });

    const response = await withMockFetch(async (_url, init) => {
      capturedInit = init;
      return upstreamResponse;
    }, async () => onRequest(contextFor("/v1/chat/completions", {
      method: "POST",
      headers: {
        authorization: "Bearer ignored",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: "deepseek-v4-flash",
        messages: [{ role: "user", content: "hello" }],
      }),
    })));

    assert.equal(response, upstreamResponse, "chat/completions passes through unchanged");
  }
});

test("does not add the free suffix twice", async () => {
  const request = contextFor("/v1/chat/completions", {
    method: "POST",
    body: JSON.stringify({ model: "mimo-v2.5-free", messages: [] }),
  }).request;

  const init = await requestInit(request);

  assert.deepEqual(JSON.parse(init.body), {
    model: "mimo-v2.5-free",
    messages: [],
  });
});

test("drops nameless function tools before forwarding to the upstream", async () => {
  const tools = [
    { type: "function", function: {} },
    { type: "function", name: "top-level-name", function: {} },
    { type: "function", function: { name: "bash", description: "run shells" } },
    { type: "custom", value: 1 },
  ];

  assert.deepEqual(sanitizeTools(tools), [
    { type: "function", name: "top-level-name", function: {} },
    { type: "function", function: { name: "bash", description: "run shells" } },
    { type: "custom", value: 1 },
  ]);

  const request = contextFor("/v1/chat/completions", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: "mimo-v2.5", tools }),
  }).request;

  const init = await requestInit(request);
  assert.deepEqual(JSON.parse(init.body), {
    model: "mimo-v2.5-free",
    tools: [
      { type: "function", name: "top-level-name", function: {} },
      { type: "function", function: { name: "bash", description: "run shells" } },
      { type: "custom", value: 1 },
    ],
  });
});

test("deletes the tools field when every function tool is nameless", async () => {
  const request = contextFor("/v1/chat/completions", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      model: "mimo-v2.5",
      messages: [],
      tools: [{ type: "function", function: {} }],
    }),
  }).request;

  const init = await requestInit(request);
  assert.deepEqual(JSON.parse(init.body), {
    model: "mimo-v2.5-free",
    messages: [],
  });
});

test("passes malformed JSON and requests without a model to the upstream unchanged", async () => {
  for (const body of ["{not-json", "{ \"messages\": [] }"]) {
    const request = contextFor("/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
    }).request;

    const init = await requestInit(request);
    assert.equal(init.body, body);
  }
});

test("does not attach bodies to GET or HEAD requests", async () => {
  for (const method of ["GET", "HEAD"]) {
    const init = await requestInit(contextFor("/v1/models", { method }).request);
    assert.equal(init.method, method);
    assert.equal(init.body, undefined);
  }
});

test("removes hop-by-hop headers and replaces client authentication", async () => {
  const init = await requestInit(contextFor("/v1/models", {
    headers: {
      authorization: "Bearer keep-me",
      "x-api-key": "also-remove-me",
      host: "proxy.example",
      connection: "keep-alive, x-hop-token",
      "content-length": "0",
      "proxy-connection": "keep-alive",
      "x-hop-token": "remove-me",
      "x-client-trace": "trace-123",
    },
  }).request);

  for (const header of [
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-connection",
    "x-api-key",
    "x-hop-token",
  ]) {
    assert.equal(init.headers.has(header), false, header);
  }

  assert.equal(init.headers.get("authorization"), PUBLIC_AUTHORIZATION);
  assert.equal(init.headers.get("x-client-trace"), "trace-123");
  assert.deepEqual(init.eo, {
    timeoutSetting: {
      connectTimeout: MAX_TIMEOUT_MS,
      readTimeout: MAX_TIMEOUT_MS,
      writeTimeout: MAX_TIMEOUT_MS,
    },
  });
  assert.equal(init.redirect, "manual");
});

test("returns inference streams immediately without reading the upstream body", async () => {
  let streamOpen = true;
  const upstreamResponse = new Response(new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode("data: first\\n\\n"));
    },
    cancel() {
      streamOpen = false;
    },
  }), {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });

  const response = await withMockFetch(async () => upstreamResponse, async () => {
    return onRequest(contextFor("/v1/chat/completions", {
      method: "POST",
      body: JSON.stringify({ model: "deepseek-v4-flash", messages: [] }),
    }));
  });

  assert.equal(response, upstreamResponse);
  assert.equal(response.bodyUsed, false);

  const reader = response.body.getReader();
  const firstChunk = await reader.read();
  assert.equal(new TextDecoder().decode(firstChunk.value), "data: first\\n\\n");
  assert.equal(firstChunk.done, false);
  assert.equal(streamOpen, true);
  await reader.cancel();
});

test("passes upstream model-list errors through unchanged", async () => {
  const upstreamResponse = new Response("rate limited", {
    status: 429,
    headers: { "retry-after": "2" },
  });

  const response = await filterModelsResponse(upstreamResponse);

  assert.equal(response, upstreamResponse);
});

test("maps upstream network failures to a generic 502", async () => {
  const response = await withMockFetch(async () => {
    throw new Error("secret upstream failure");
  }, async () => onRequest(contextFor("/v1/models")));

  assert.equal(response.status, 502);
  assert.equal(await response.text(), "Bad Gateway");
});

test("model ID helpers only expose and target free models", () => {
  assert.equal(toUpstreamModelId("deepseek-v4-flash"), "deepseek-v4-flash-free");
  assert.equal(toUpstreamModelId("deepseek-v4-flash-free"), "deepseek-v4-flash-free");
  assert.equal(toPublicModelId("deepseek-v4-flash-free"), "deepseek-v4-flash");
  assert.equal(toPublicModelId("deepseek-v4-flash"), null);
  assert.equal(toPublicModelId("-free"), null);
});

// ======================== A. Request conversion (pure function tests) ========================

test("convertClaudeRequest converts Claude messages to Chat format", () => {
  const claudeReq = {
    model: "claude-sonnet",
    messages: [
      { role: "system", content: "You are a helpful assistant." },
      { role: "user", content: "Hello" },
      { role: "assistant", content: [
        { type: "thinking", thinking: "Let me think about this." },
        { type: "tool_use", id: "call_1", name: "get_weather", input: { location: "NYC" } },
      ]},
      { role: "user", content: [
        { type: "tool_result", tool_use_id: "call_1", content: "Sunny, 72F" },
      ]},
    ],
    tools: [
      { name: "get_weather", description: "Get the weather", input_schema: { type: "object", properties: { location: { type: "string" } } } },
    ],
    stream: false,
    temperature: 0.7,
  };

  const result = convertClaudeRequest(claudeReq);

  assert.equal(result.model, "claude-sonnet");
  assert.equal(result.stream, false);
  assert.equal(result.temperature, 0.7);
  assert.ok(Array.isArray(result.messages));

  // System message is first
  assert.equal(result.messages[0].role, "system");
  assert.equal(result.messages[0].content, "You are a helpful assistant.");

  // User text becomes user message
  assert.equal(result.messages[1].role, "user");
  assert.equal(result.messages[1].content, "Hello");

  // tool_use becomes assistant message with tool_calls and reasoning_content
  const assistantMsg = result.messages.find(m => m.role === "assistant" && m.tool_calls);
  assert.ok(assistantMsg, "has assistant message with tool_calls");
  assert.equal(assistantMsg.tool_calls[0].id, "call_1");
  assert.equal(assistantMsg.tool_calls[0].type, "function");
  assert.equal(assistantMsg.tool_calls[0].function.name, "get_weather");
  assert.equal(assistantMsg.reasoning_content, "Let me think about this.");

  // tool_result becomes tool role message (appears after assistant for user role)
  const toolMsg = result.messages.find(m => m.role === "tool");
  assert.ok(toolMsg, "has tool message");
  assert.equal(toolMsg.tool_call_id, "call_1");
  assert.equal(toolMsg.content, "Sunny, 72F");

  // tools converted
  assert.ok(Array.isArray(result.tools));
  assert.equal(result.tools[0].type, "function");
  assert.equal(result.tools[0].function.name, "get_weather");
});

test("convertClaudeToolChoice maps Claude choice types to Chat types", () => {
  assert.deepEqual(convertClaudeToolChoice({ type: "tool", name: "my_func" }), {
    type: "function",
    function: { name: "my_func" },
  });
  assert.equal(convertClaudeToolChoice({ type: "any" }), "required");
  assert.equal(convertClaudeToolChoice({ type: "auto" }), "auto");
  assert.equal(convertClaudeToolChoice({ type: "none" }), "none");
  assert.equal(convertClaudeToolChoice(null), null);
});

test("responsesInputToMessages converts input items to messages", () => {
  const input = [
    { type: "function_call", name: "get_weather", arguments: '{"location":"NYC"}', call_id: "call_1" },
    { type: "function_call_output", call_id: "call_1", output: "Sunny" },
    { type: "message", role: "user", content: [{ type: "input_text", text: "What is the weather?" }] },
  ];

  const messages = responsesInputToMessages(input, "");

  // function_call -> assistant message with tool_calls
  const assistantMsg = messages.find(m => m.role === "assistant" && m.tool_calls);
  assert.ok(assistantMsg, "has assistant message with tool_calls");
  assert.equal(assistantMsg.tool_calls[0].function.name, "get_weather");

  // function_call_output produces tool messages (via collectFunctionOutputs + handler)
  const toolMessages = messages.filter(m => m.role === "tool");
  assert.ok(toolMessages.length >= 1, "has at least one tool message");
  const toolMsg = toolMessages.find(m => m.tool_call_id === "call_1");
  assert.ok(toolMsg, "has tool message for call_1");
  assert.equal(toolMsg.content, "Sunny");

  // message item -> user message
  const userMsg = messages.find(m => m.role === "user");
  assert.ok(userMsg, "has user message");
});

test("responsesInputToMessages converts developer role to system", () => {
  const input = [
    { type: "message", role: "developer", content: [{ type: "input_text", text: "You are a helpful assistant." }] },
  ];

  const messages = responsesInputToMessages(input, "");
  assert.equal(messages[0].role, "system");
  assert.equal(messages[0].content, "You are a helpful assistant.");
});

test("responsesToolFunction generates schema for function type", () => {
  const tool = {
    type: "function",
    name: "get_weather",
    description: "Get weather info",
    parameters: { type: "object", properties: { location: { type: "string" } } },
  };

  const fn = responsesToolFunction(tool);
  assert.equal(fn.name, "get_weather");
  assert.equal(fn.description, "Get weather info");
  assert.equal(fn.parameters.type, "object");
});

test("responsesToolFunction generates built-in apply_patch schema", () => {
  const fn = responsesToolFunction({ type: "apply_patch" });
  assert.equal(fn.name, "apply_patch");
  assert.ok(fn.parameters.properties.input);
});

test("responsesToolFunction generates built-in shell schema", () => {
  const fn = responsesToolFunction({ type: "shell" });
  assert.equal(fn.name, "shell");
  assert.ok(fn.parameters.properties.command);
  assert.ok(fn.parameters.required.includes("command"));
});

// ======================== B. Response conversion (pure function tests) ========================

test("openAIToClaudeResponse converts Chat response to Claude format", () => {
  const chatBody = JSON.stringify({
    id: "chatcmpl-test",
    model: "deepseek-v4-flash-free",
    choices: [{
      message: {
        role: "assistant",
        content: "Here is the answer.",
        reasoning_content: "Let me think step by step.",
        tool_calls: [
          { id: "call_1", type: "function", function: { name: "get_weather", arguments: '{"location":"NYC"}' } },
        ],
      },
      finish_reason: "tool_calls",
      index: 0,
    }],
    usage: { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 },
  });

  const result = JSON.parse(openAIToClaudeResponse(chatBody, "deepseek-v4-flash", true));

  assert.equal(result.role, "assistant");
  assert.ok(Array.isArray(result.content));

  // thinking block first
  assert.equal(result.content[0].type, "thinking");
  assert.equal(result.content[0].thinking, "Let me think step by step.");

  // text block second
  assert.equal(result.content[1].type, "text");
  assert.equal(result.content[1].text, "Here is the answer.");

  // tool_use block third
  assert.equal(result.content[2].type, "tool_use");
  assert.equal(result.content[2].id, "call_1");
  assert.equal(result.content[2].name, "get_weather");

  // stop_reason mapped correctly
  assert.equal(result.stop_reason, "tool_use");

  // usage mapped
  assert.equal(result.usage.input_tokens, 10);
  assert.equal(result.usage.output_tokens, 5);
});

test("convertChatToResponses converts Chat response to Responses format", () => {
  const chatBody = JSON.stringify({
    id: "chatcmpl-test",
    model: "deepseek-v4-flash-free",
    created: 1234567890,
    choices: [{
      message: {
        role: "assistant",
        content: "Hello world",
        tool_calls: [
          { id: "call_1", type: "function", function: { name: "get_weather", arguments: '{"location":"NYC"}' } },
        ],
      },
      finish_reason: "tool_calls",
      index: 0,
    }],
    usage: { prompt_tokens: 8, completion_tokens: 4, total_tokens: 12 },
  });

  const result = JSON.parse(convertChatToResponses(chatBody, "deepseek-v4-flash", true, [], null));

  assert.equal(result.object, "response");
  assert.equal(result.status, "completed");
  assert.equal(result.model, "deepseek-v4-flash");
  assert.ok(Array.isArray(result.output));

  // Find the message output
  const msgOutput = result.output.find(o => o.type === "message");
  assert.ok(msgOutput, "has message output");
  assert.equal(msgOutput.role, "assistant");
  assert.ok(Array.isArray(msgOutput.content));
  assert.equal(msgOutput.content[0].type, "output_text");
  assert.equal(msgOutput.content[0].text, "Hello world");

  // Find the function_call output
  const fnOutput = result.output.find(o => o.type === "function_call");
  assert.ok(fnOutput, "has function_call output");
  assert.equal(fnOutput.name, "get_weather");
  assert.equal(fnOutput.arguments, '{"location":"NYC"}');

  // usage mapped
  assert.equal(result.usage.input_tokens, 8);
  assert.equal(result.usage.output_tokens, 4);
});

// ======================== C. SSE stream conversion tests ========================

test("claudeStreamHandler parses Chat SSE and emits Claude events", async () => {
  const upstreamSse = [
    "data: " + JSON.stringify({ id: "chatcmpl-1", usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8 }, choices: [{ delta: { content: "Hello" }, finish_reason: null, index: 0 }] }) + "\n\n",
    "data: " + JSON.stringify({ choices: [{ delta: { content: " world" }, finish_reason: null, index: 0 }] }) + "\n\n",
    "data: " + JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop", index: 0 }] }) + "\n\n",
    "data: [DONE]\n\n",
  ].join("");

  const stream = claudeStreamHandler(
    new Response(upstreamSse).body,
    "deepseek-v4-flash",
    false,
  );

  const events = await collectSSEEvents(stream);

  // Check key event names
  const eventNames = events.map(e => e.event);
  assert.ok(eventNames.includes("message_start"), "has message_start");
  assert.ok(eventNames.includes("content_block_start"), "has content_block_start");
  assert.ok(eventNames.includes("content_block_delta"), "has content_block_delta");
  assert.ok(eventNames.includes("content_block_stop"), "has content_block_stop");
  assert.ok(eventNames.includes("message_delta"), "has message_delta");
  assert.ok(eventNames.includes("message_stop"), "has message_stop");

  // message_start has model
  const msgStart = events.find(e => e.event === "message_start");
  assert.equal(msgStart.data.message.model, "deepseek-v4-flash");
  assert.equal(msgStart.data.message.role, "assistant");

  // message_delta has stop_reason
  const msgDelta = events.find(e => e.event === "message_delta");
  assert.equal(msgDelta.data.delta.stop_reason, "end_turn");

  // message_stop terminates
  const msgStop = events.find(e => e.event === "message_stop");
  assert.ok(msgStop, "has message_stop");
});

test("claudeStreamHandler aggregates thinking blocks when keepReasoning is true", async () => {
  const upstreamSse = [
    "data: " + JSON.stringify({ id: "chatcmpl-1", choices: [{ delta: { reasoning_content: "Let me think." }, finish_reason: null, index: 0 }], usage: { prompt_tokens: 2, completion_tokens: 1, total_tokens: 3 } }) + "\n\n",
    "data: " + JSON.stringify({ choices: [{ delta: { reasoning_content: " more thoughts." }, finish_reason: null, index: 0 }] }) + "\n\n",
    "data: " + JSON.stringify({ choices: [{ delta: { content: "Answer." }, finish_reason: null, index: 0 }] }) + "\n\n",
    "data: " + JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop", index: 0 }] }) + "\n\n",
    "data: [DONE]\n\n",
  ].join("");

  const stream = claudeStreamHandler(
    new Response(upstreamSse).body,
    "deepseek-v4-flash",
    true,
  );

  const events = await collectSSEEvents(stream);

  // Should have thinking content_block_start and deltas
  const thinkingStarts = events.filter(e => e.event === "content_block_start" && e.data.content_block?.type === "thinking");
  assert.equal(thinkingStarts.length, 1, "one thinking block start");

  const thinkingDeltas = events.filter(e => e.event === "content_block_delta" && e.data.delta?.type === "thinking_delta");
  assert.equal(thinkingDeltas.length, 2, "two thinking deltas");

  // Text block also present
  const textStarts = events.filter(e => e.event === "content_block_start" && e.data.content_block?.type === "text");
  assert.equal(textStarts.length, 1, "one text block start");
});

test("responsesStreamHandler parses Chat SSE and emits Responses events", async () => {
  const upstreamSse = [
    "data: " + JSON.stringify({ id: "chatcmpl-1", created: 1234567890, usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8 }, choices: [{ delta: { content: "Hello" }, finish_reason: null, index: 0 }] }) + "\n\n",
    "data: " + JSON.stringify({ choices: [{ delta: { content: " world" }, finish_reason: null, index: 0 }] }) + "\n\n",
    "data: " + JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop", index: 0 }] }) + "\n\n",
    "data: [DONE]\n\n",
  ].join("");

  const stream = responsesStreamHandler(
    new Response(upstreamSse).body,
    "deepseek-v4-flash",
    false,
    [],
    null,
    {},
  );

  const events = await collectSSEEvents(stream);

  const eventNames = events.map(e => e.event);
  assert.ok(eventNames.includes("response.created"), "has response.created");
  assert.ok(eventNames.includes("response.in_progress"), "has response.in_progress");
  assert.ok(eventNames.includes("response.output_item.added"), "has response.output_item.added");
  assert.ok(eventNames.includes("response.output_text.delta"), "has response.output_text.delta");
  assert.ok(eventNames.includes("response.output_text.done"), "has response.output_text.done");
  assert.ok(eventNames.includes("response.completed"), "has response.completed");

  // sequence_number should be incrementing
  for (let i = 1; i < events.length; i++) {
    assert.ok(
      events[i].data.sequence_number >= events[i - 1].data.sequence_number,
      `sequence_number non-decreasing at index ${i}`,
    );
  }

  // output_index for message should be 0
  const outputItemAdded = events.find(e => e.event === "response.output_item.added");
  assert.equal(outputItemAdded.data.output_index, 0, "first output item has index 0");
});

test("responsesStreamHandler handles tool_calls in Chat SSE", async () => {
  const upstreamSse = [
    "data: " + JSON.stringify({ id: "chatcmpl-tools", created: 1234567890, usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8 }, choices: [{ delta: { tool_calls: [{ index: 0, id: "call_1", function: { name: "get_weather", arguments: '{"location"' } }] }, finish_reason: null, index: 0 }] }) + "\n\n",
    "data: " + JSON.stringify({ choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: ':"NYC"}' } }] }, finish_reason: null, index: 0 }] }) + "\n\n",
    "data: " + JSON.stringify({ choices: [{ delta: {}, finish_reason: "tool_calls", index: 0 }] }) + "\n\n",
    "data: [DONE]\n\n",
  ].join("");

  const stream = responsesStreamHandler(
    new Response(upstreamSse).body,
    "deepseek-v4-flash",
    false,
    [],
    null,
    {},
  );

  const events = await collectSSEEvents(stream);

  const eventNames = events.map(e => e.event);
  assert.ok(eventNames.includes("response.output_item.added"), "has output_item.added for tool");
  assert.ok(eventNames.includes("response.function_call_arguments.delta"), "has function_call_arguments.delta");
  assert.ok(eventNames.includes("response.function_call_arguments.done"), "has function_call_arguments.done");
  assert.ok(eventNames.includes("response.output_item.done"), "has output_item.done for tool");
  assert.ok(eventNames.includes("response.completed"), "has response.completed");
});

// ======================== D. End-to-end (mock fetch) tests ========================

test("end-to-end Claude messages entry converts request and response", async () => {
  let capturedUrl;
  let capturedBody;

  const chatResponseBody = JSON.stringify({
    id: "chatcmpl-e2e",
    model: "deepseek-v4-flash-free",
    choices: [{ message: { role: "assistant", content: "Translated" }, finish_reason: "stop", index: 0 }],
    usage: { prompt_tokens: 5, completion_tokens: 2, total_tokens: 7 },
  });

  const response = await withMockFetch(async (url, init) => {
    capturedUrl = url;
    capturedBody = JSON.parse(init.body);
    return new Response(chatResponseBody, { headers: { "content-type": "application/json" } });
  }, async () => onRequest(contextFor("/v1/messages", {
    method: "POST",
    headers: {
      authorization: "Bearer ignored",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "deepseek-v4-flash",
      messages: [{ role: "user", content: "hello" }],
    }),
  })));

  // Upstream URL is the Chat Completions endpoint
  assert.equal(capturedUrl, "https://opencode.ai/zen/v1/chat/completions");
  // Model has -free suffix
  assert.equal(capturedBody.model, "deepseek-v4-flash-free");
  // Messages are in Chat format
  assert.ok(Array.isArray(capturedBody.messages));
  assert.equal(capturedBody.messages[0].role, "user");
  assert.equal(capturedBody.messages[0].content, "hello");

  // Client receives Claude-formatted response
  const clientBody = await response.json();
  assert.equal(clientBody.type, "message");
  assert.equal(clientBody.role, "assistant");
  assert.equal(clientBody.content[0].type, "text");
  assert.equal(clientBody.content[0].text, "Translated");
  assert.equal(clientBody.stop_reason, "end_turn");
});

test("end-to-end Responses entry converts request and response", async () => {
  let capturedUrl;
  let capturedBody;

  const chatResponseBody = JSON.stringify({
    id: "chatcmpl-e2e-resp",
    model: "deepseek-v4-flash-free",
    created: 1234567890,
    choices: [{ message: { role: "assistant", content: "Done" }, finish_reason: "stop", index: 0 }],
    usage: { prompt_tokens: 3, completion_tokens: 1, total_tokens: 4 },
  });

  const response = await withMockFetch(async (url, init) => {
    capturedUrl = url;
    capturedBody = JSON.parse(init.body);
    return new Response(chatResponseBody, { headers: { "content-type": "application/json" } });
  }, async () => onRequest(contextFor("/v1/responses", {
    method: "POST",
    headers: {
      authorization: "Bearer ignored",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "deepseek-v4-flash",
      input: [{ role: "user", content: [{ type: "input_text", text: "hello" }] }],
    }),
  })));

  // Upstream URL is the Chat Completions endpoint
  assert.equal(capturedUrl, "https://opencode.ai/zen/v1/chat/completions");
  // Model has -free suffix
  assert.equal(capturedBody.model, "deepseek-v4-flash-free");
  // Input was converted to messages
  assert.ok(Array.isArray(capturedBody.messages));
  assert.equal(capturedBody.messages[0].role, "user");

  // Client receives Responses-formatted response
  const clientBody = await response.json();
  assert.equal(clientBody.object, "response");
  assert.equal(clientBody.status, "completed");
  assert.ok(Array.isArray(clientBody.output));
});

test("end-to-end preserves -free model mapping and tool sanitization", async () => {
  let capturedUrl;
  let capturedBody;

  const chatResponseBody = JSON.stringify({
    id: "chatcmpl-tools",
    model: "deepseek-v4-flash-free",
    choices: [{ message: { role: "assistant", content: "" }, finish_reason: "stop", index: 0 }],
  });

  const response = await withMockFetch(async (url, init) => {
    capturedUrl = url;
    capturedBody = JSON.parse(init.body);
    return new Response(chatResponseBody, { headers: { "content-type": "application/json" } });
  }, async () => onRequest(contextFor("/v1/chat/completions", {
    method: "POST",
    headers: {
      authorization: "Bearer ignored",
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "deepseek-v4-flash",
      messages: [{ role: "user", content: "run command" }],
      tools: [
        { type: "function", function: {} },
        { type: "function", name: "bash", function: { description: "run shells" } },
      ],
    }),
  })));

  assert.equal(capturedUrl, "https://opencode.ai/zen/v1/chat/completions");
  assert.equal(capturedBody.model, "deepseek-v4-flash-free");
  // Nameless function tool removed, bash kept
  assert.equal(capturedBody.tools.length, 1);
  assert.equal(capturedBody.tools[0].name, "bash");
});
