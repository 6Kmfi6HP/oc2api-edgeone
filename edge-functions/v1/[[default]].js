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

function upstreamUrl(requestUrl) {
  const incomingUrl = new URL(requestUrl);
  return `${UPSTREAM_ORIGIN}${UPSTREAM_PREFIX}${incomingUrl.pathname}${incomingUrl.search}`;
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

export default async function onRequest(context) {
  try {
    const request = context.request;
    const pathname = new URL(request.url).pathname;
    const response = await fetch(upstreamUrl(request.url), await requestInit(request));

    if (request.method.toUpperCase() === "GET" && pathname === MODEL_LIST_PATH) {
      return await filterModelsResponse(response);
    }

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

export {
  FREE_MODEL_SUFFIX,
  HOP_BY_HOP_HEADERS,
  MAX_TIMEOUT_MS,
  MODEL_LIST_PATH,
  MODEL_REQUEST_PATHS,
  PUBLIC_AUTHORIZATION,
  UPSTREAM_ORIGIN,
  UPSTREAM_PREFIX,
  filterModelsResponse,
  forwardHeaders,
  mapRequestBody,
  requestInit,
  sanitizeTools,
  toPublicModelId,
  toUpstreamModelId,
  upstreamUrl,
};
