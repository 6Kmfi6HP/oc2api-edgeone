// Raw pass-through of the upstream OpenCode Zen API surface, exposed under
// /zen/v1/*. Unlike the client-facing /v1/* routes, requests here are relayed
// to https://opencode.ai/zen/v1/* verbatim: no free-model filtering, no model
// suffix mapping, and no protocol conversion. The main handler already routes
// these paths through its generic pass-through branch (upstreamUrl is
// prefix-aware), so this file only needs to register the route and delegate.
import onRequest from "../../v1/[[default]].js";

export default onRequest;