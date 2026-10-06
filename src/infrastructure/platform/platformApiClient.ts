import { createHash } from "node:crypto";
import type { CallbackRequestDto } from "../../shared/dto/callbackRequestDto.js";
import { BurstResponseDto } from "../../shared/dto/burstResponseDto.js";
import { CallbackResponseDto } from "../../shared/dto/callbackResponseDto.js";
import { EnrichmentResponseDto } from "../../shared/dto/enrichmentResponseDto.js";
import { RegisterRequestDto } from "../../shared/dto/registerRequestDto.js";
import { RegisterResponseDto } from "../../shared/dto/registerResponseDto.js";

export interface PlatformCredentials {
  get(): Promise<RegisterResponseDto>;
}

export type FetchImplementation = typeof fetch;

export class PlatformApiError extends Error {
  readonly statusCode: number | undefined;
  readonly retryAfterMs: number | undefined;
  readonly code: string;

  constructor(message: string, options: { statusCode?: number; retryAfterMs?: number; code?: string } = {}) {
    super(message);
    this.name = "PlatformApiError";
    this.statusCode = options.statusCode;
    this.retryAfterMs = options.retryAfterMs;
    this.code = options.code ?? (options.statusCode === undefined ? "PLATFORM_ERROR" : `HTTP_${options.statusCode}`);
  }
}

export class PlatformApiClient {
  private readonly fetchImplementation: FetchImplementation;

  constructor(private readonly dependencies: {
    baseUrl: string;
    credentials: PlatformCredentials;
    fetchImplementation?: FetchImplementation;
    timeoutMs?: number;
  }) {
    this.fetchImplementation = dependencies.fetchImplementation ?? fetch;
  }

  async register(request: RegisterRequestDto): Promise<RegisterResponseDto> {
    const value = await this.request("/register", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: request.name, webhook: request.webhook })
    });
    if (!isObject(value) || typeof value.cid !== "string" || typeof value.token !== "string") {
      throw new PlatformApiError("Registration response is invalid", { code: "INVALID_PLATFORM_RESPONSE" });
    }
    return new RegisterResponseDto(value.cid, value.token);
  }

  async burst(cid: string, token: string): Promise<BurstResponseDto> {
    const value = await this.request(`/burst/${encodeURIComponent(cid)}`, {
      method: "POST",
      headers: { "x-token": token }
    });
    return BurstResponseDto.from(value);
  }

  async fetch(sku: string): Promise<EnrichmentResponseDto> {
    const credentials = await this.dependencies.credentials.get();
    const value = await this.request(`/enrich/${encodeURIComponent(sku)}`, {
      method: "GET",
      headers: { "x-cid": credentials.cid, "x-token": credentials.token }
    });
    try {
      return EnrichmentResponseDto.from(value, sku);
    } catch {
      throw new PlatformApiError("Enrichment response is invalid", { code: "INVALID_PLATFORM_RESPONSE" });
    }
  }

  async sendCallback(payload: CallbackRequestDto): Promise<CallbackResponseDto> {
    const credentials = await this.dependencies.credentials.get();
    const response = await this.request("/callback", {
      method: "POST",
      headers: { "content-type": "application/json", "x-token": credentials.token },
      body: JSON.stringify({
        cid: payload.cid,
        run_id: payload.run_id,
        result: payload.result.map(({ seq, sku, price, stock }) => ({ seq, sku, price, stock }))
      })
    }, "text");
    if (typeof response !== "string") {
      throw new PlatformApiError("Callback response could not be captured", { code: "INVALID_PLATFORM_RESPONSE" });
    }
    return CallbackResponseDto.fromText(response);
  }

  private async request(path: string, init: RequestInit, responseMode: "json" | "text" = "json"): Promise<unknown> {
    let response: Response;
    try {
      response = await this.fetchImplementation(new URL(path, `${this.dependencies.baseUrl.replace(/\/$/, "")}/`), {
        ...init,
        signal: AbortSignal.timeout(this.dependencies.timeoutMs ?? 5000)
      });
    } catch (failure) {
      throw normalizeNetworkFailure(failure);
    }
    if (!response.ok) {
      const retryAfterMs = parseRetryAfter(response.headers.get("retry-after"));
      throw new PlatformApiError("Platform request failed", {
        statusCode: response.status,
        ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
        code: `HTTP_${response.status}`
      });
    }
    const body = responseMode === "text" ? await response.text().catch(() => "") : await response.text();
    if (responseMode === "text") {
      return body;
    }
    if (!body) {
      throw new PlatformApiError("Platform response body is empty", { code: "INVALID_PLATFORM_RESPONSE" });
    }
    try {
      return JSON.parse(body) as unknown;
    } catch {
      throw new PlatformApiError("Platform response is not valid JSON", { code: "INVALID_PLATFORM_RESPONSE" });
    }
  }
}

function parseRetryAfter(value: string | null): number | undefined {
  if (value === null) {
    return undefined;
  }
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.ceil(seconds * 1000);
  }
  const timestamp = Date.parse(value);
  return Number.isNaN(timestamp) ? undefined : Math.max(0, timestamp - Date.now());
}

function normalizeNetworkFailure(failure: unknown): PlatformApiError {
  const source = typeof failure === "object" && failure !== null ? failure as Record<string, unknown> : {};
  const cause = typeof source.cause === "object" && source.cause !== null ? source.cause as Record<string, unknown> : {};
  const sourceCode = typeof source.code === "string" ? source.code : typeof cause.code === "string" ? cause.code : undefined;
  const code = source.name === "TimeoutError" || source.name === "AbortError" ? "ETIMEDOUT" : mapNetworkCode(sourceCode);
  return new PlatformApiError("Platform network request failed", { code });
}

function mapNetworkCode(code: string | undefined): string {
  if (code === "ECONNRESET" || code === "ECONNREFUSED" || code === "EAI_AGAIN") {
    return code;
  }
  if (code === "UND_ERR_CONNECT_TIMEOUT" || code === "UND_ERR_SOCKET") {
    return code;
  }
  return "NETWORK_ERROR";
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function deadLetterJobId(message: { run_id: string; seq: number }): string {
  const identity = createHash("sha256").update(JSON.stringify([message.run_id, message.seq])).digest("hex");
  return `failure-${identity}`;
}
