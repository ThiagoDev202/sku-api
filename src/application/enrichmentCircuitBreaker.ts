type FailureDetails = {
  statusCode?: number;
  code?: string;
};

type CircuitBreakerOptions = {
  now?: () => number;
  failureThreshold?: number;
  openDurationMs?: number;
};

export class EnrichmentCircuitBreaker {
  private readonly now: () => number;
  private readonly failureThreshold: number;
  private readonly openDurationMs: number;
  private failureCount = 0;
  private openedAt: number | null = null;
  private probeInProgress = false;

  constructor(options: CircuitBreakerOptions = {}) {
    this.now = options.now ?? Date.now;
    this.failureThreshold = options.failureThreshold ?? 5;
    this.openDurationMs = options.openDurationMs ?? 30_000;
  }

  async execute<T>(operation: () => Promise<T>): Promise<T> {
    const isProbe = this.tryAcquire();
    try {
      const result = await operation();
      this.reset();
      return result;
    } catch (failure) {
      if (isTransient(failure)) {
        this.recordTransientFailure(isProbe);
      } else if (isProbe) {
        this.reset();
      }
      throw failure;
    }
  }

  private tryAcquire(): boolean {
    if (this.openedAt === null) {
      return false;
    }
    const remainingMs = this.openDurationMs - (this.now() - this.openedAt);
    if (remainingMs > 0 || this.probeInProgress) {
      const failure = new Error("Enrichment circuit is open") as Error & { code: string; retryAfterMs: number };
      failure.code = "CIRCUIT_OPEN";
      failure.retryAfterMs = Math.max(this.probeInProgress ? 250 : 1, remainingMs);
      throw failure;
    }
    this.probeInProgress = true;
    return true;
  }

  private recordTransientFailure(isProbe: boolean): void {
    this.probeInProgress = false;
    this.failureCount += 1;
    if (isProbe || this.failureCount >= this.failureThreshold) {
      this.openedAt = this.now();
    }
  }

  private reset(): void {
    this.failureCount = 0;
    this.openedAt = null;
    this.probeInProgress = false;
  }
}

function isTransient(failure: unknown): boolean {
  if (typeof failure !== "object" || failure === null) {
    return false;
  }
  const candidate = failure as FailureDetails;
  return (typeof candidate.statusCode === "number" && candidate.statusCode >= 500) ||
    candidate.code === "ETIMEDOUT" ||
    candidate.code === "ECONNRESET" ||
    candidate.code === "ECONNREFUSED" ||
    candidate.code === "EAI_AGAIN" ||
    candidate.code === "UND_ERR_CONNECT_TIMEOUT" ||
    candidate.code === "UND_ERR_SOCKET";
}
