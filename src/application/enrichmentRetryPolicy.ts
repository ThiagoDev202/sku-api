type FailureDetails = {
  statusCode?: number;
  retryAfterMs?: number;
  code?: string;
};

export type RetryPolicyOptions = {
  sleep?: (milliseconds: number) => Promise<void>;
  random?: () => number;
  baseDelayMs?: number;
};

export class EnrichmentRetryPolicy {
  private readonly sleep: (milliseconds: number) => Promise<void>;
  private readonly random: () => number;
  private readonly baseDelayMs: number;

  constructor(options: RetryPolicyOptions = {}) {
    this.sleep = options.sleep ?? delay;
    this.random = options.random ?? Math.random;
    this.baseDelayMs = options.baseDelayMs ?? 500;
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    let attempts = 0;
    let transientFailures = 0;
    while (attempts < 3) {
      try {
        const result = await operation();
        return result;
      } catch (failure) {
        const details = getFailureDetails(failure);
        if (details.code === "CIRCUIT_OPEN") {
          const retryAfterMs = Math.max(1, details.retryAfterMs ?? 30_000);
          await this.sleep(retryAfterMs);
          continue;
        }
        attempts += 1;
        if (!isRetryable(details) || attempts >= 3) {
          throw failure;
        }
        transientFailures += 1;
        const exponentialDelay = this.baseDelayMs * (2 ** (transientFailures - 1));
        const jitterDelay = Math.floor(this.random() * exponentialDelay * 0.25);
        const retryAfterMs = details.statusCode === 429 ? details.retryAfterMs ?? 0 : 0;
        await this.sleep(Math.max(retryAfterMs, exponentialDelay + jitterDelay));
      }
    }
    throw new Error("Enrichment retry limit reached");
  }
}

function isRetryable(details: FailureDetails): boolean {
  return details.statusCode === 429 ||
    (typeof details.statusCode === "number" && details.statusCode >= 500) ||
    details.code === "ETIMEDOUT" ||
    details.code === "ECONNRESET" ||
    details.code === "ECONNREFUSED" ||
    details.code === "EAI_AGAIN" ||
    details.code === "UND_ERR_CONNECT_TIMEOUT" ||
    details.code === "UND_ERR_SOCKET";
}

function getFailureDetails(failure: unknown): FailureDetails {
  if (typeof failure !== "object" || failure === null) {
    return {};
  }
  const candidate = failure as Record<string, unknown>;
  const details: FailureDetails = {};
  if (typeof candidate.statusCode === "number") {
    details.statusCode = candidate.statusCode;
  }
  if (typeof candidate.retryAfterMs === "number") {
    details.retryAfterMs = candidate.retryAfterMs;
  }
  if (typeof candidate.code === "string") {
    details.code = candidate.code;
  }
  return details;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
