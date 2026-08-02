import assert from "node:assert/strict";
import { test } from "node:test";

import onRequest, {
  MAX_TIMEOUT_MS,
  PUBLIC_AUTHORIZATION,
  filterModelsResponse,
  requestInit,
  toPublicModelId,
  toUpstreamModelId,
  upstreamUrl,
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
  const cases = [
    {
      path: "/v1/responses",
      payload: {
        model: "deepseek-v4-flash",
        input: [{ role: "user", content: [{ type: "input_text", text: "hello" }] }],
        stream: true,
        metadata: { trace: "responses" },
      },
    },
    {
      path: "/v1/chat/completions",
      payload: {
        model: "deepseek-v4-flash",
        messages: [{ role: "user", content: "hello" }],
        temperature: 0.25,
        stream: true,
      },
    },
    {
      path: "/v1/messages",
      payload: {
        model: "deepseek-v4-flash",
        max_tokens: 64,
        messages: [{ role: "user", content: "hello" }],
        system: "Keep this field unchanged",
      },
    },
  ];

  for (const { path, payload } of cases) {
    let capturedInit;
    const upstreamResponse = new Response("upstream", {
      status: 202,
      headers: { "x-upstream": "unchanged" },
    });

    const response = await withMockFetch(async (_url, init) => {
      capturedInit = init;
      return upstreamResponse;
    }, async () => onRequest(contextFor(`${path}?beta=true`, {
      method: "POST",
      headers: {
        authorization: "Bearer ignored",
        "content-type": "application/json",
        "x-client-trace": path,
      },
      body: JSON.stringify(payload),
    })));

    assert.equal(response, upstreamResponse);
    assert.equal(capturedInit.headers.get("authorization"), PUBLIC_AUTHORIZATION);
    assert.equal(capturedInit.headers.get("content-type"), "application/json");
    assert.equal(capturedInit.headers.get("x-client-trace"), path);
    assert.deepEqual(JSON.parse(capturedInit.body), {
      ...payload,
      model: "deepseek-v4-flash-free",
    });
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
