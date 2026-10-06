import { describe, expect, it, vi } from "vitest";
import { EnrichmentCircuitBreaker } from "../../src/application/enrichmentCircuitBreaker.js";
import { EnrichmentRetryPolicy } from "../../src/application/enrichmentRetryPolicy.js";
import { EnrichmentWorker } from "../../src/worker/enrichmentWorker.js";

type PlatformFailure = Error & {
  statusCode?: number;
  retryAfterMs?: number;
  code?: string;
};

function httpFailure(statusCode: number, retryAfterMs?: number): PlatformFailure {
  const failure = new Error(`platform returned ${statusCode}`) as PlatformFailure;
  failure.statusCode = statusCode;
  if (retryAfterMs !== undefined) {
    failure.retryAfterMs = retryAfterMs;
  }
  return failure;
}

function networkFailure(code = "ECONNRESET"): PlatformFailure {
  const failure = new Error("network connection failed") as PlatformFailure;
  failure.code = code;
  return failure;
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

describe("EnrichmentRetryPolicy", () => {
  it("waits at least retry-after before retrying a rate limited request", async () => {
    const waits: number[] = [];
    const policy = new EnrichmentRetryPolicy({
      random: () => 0.5,
      sleep: async (milliseconds: number) => {
        waits.push(milliseconds);
      }
    });
    const operation = vi.fn().mockRejectedValueOnce(httpFailure(429, 2400)).mockResolvedValue("enriched");

    await expect(policy.execute(operation)).resolves.toBe("enriched");

    expect(operation).toHaveBeenCalledTimes(2);
    expect(waits).toHaveLength(1);
    expect(waits[0]).toBeGreaterThanOrEqual(2400);
  });

  it("retries a transient 500 and stops after three total attempts", async () => {
    const waits: number[] = [];
    const policy = new EnrichmentRetryPolicy({
      random: () => 0.5,
      sleep: async (milliseconds: number) => {
        waits.push(milliseconds);
      }
    });
    const failure = httpFailure(500);
    const operation = vi.fn().mockRejectedValue(failure);

    await expect(policy.execute(operation)).rejects.toBe(failure);

    expect(operation).toHaveBeenCalledTimes(3);
    expect(waits).toHaveLength(2);
    expect(waits[0]).toBeGreaterThan(0);
    expect(waits[1]).toBeGreaterThan(waits[0]!);
  });

  it("retries network failures but does not exceed three total attempts", async () => {
    const policy = new EnrichmentRetryPolicy({ sleep: async () => undefined });
    const failure = networkFailure();
    const operation = vi.fn().mockRejectedValue(failure);

    await expect(policy.execute(operation)).rejects.toBe(failure);

    expect(operation).toHaveBeenCalledTimes(3);
  });

  it.each([404, 401])("does not retry permanent status %i", async (statusCode) => {
    const policy = new EnrichmentRetryPolicy({ sleep: async () => undefined });
    const failure = httpFailure(statusCode);
    const operation = vi.fn().mockRejectedValue(failure);

    await expect(policy.execute(operation)).rejects.toBe(failure);

    expect(operation).toHaveBeenCalledTimes(1);
  });
});

describe("EnrichmentCircuitBreaker", () => {
  it("opens after five consecutive transient failures and permits one probe after thirty seconds", async () => {
    let now = 0;
    const breaker = new EnrichmentCircuitBreaker({ now: () => now });
    const transientFailure = httpFailure(503);

    for (let attempt = 0; attempt < 5; attempt += 1) {
      await expect(breaker.execute(async () => {
        throw transientFailure;
      })).rejects.toBe(transientFailure);
    }

    const blockedOperation = vi.fn().mockResolvedValue("should not run");
    await expect(breaker.execute(blockedOperation)).rejects.toMatchObject({ code: "CIRCUIT_OPEN" });
    expect(blockedOperation).not.toHaveBeenCalled();

    now += 30_000;
    const probeResult = deferred<string>();
    const probeOperation = vi.fn(() => probeResult.promise);
    const probe = breaker.execute(probeOperation);
    await Promise.resolve();

    const concurrentOperation = vi.fn().mockResolvedValue("should remain blocked");
    await expect(breaker.execute(concurrentOperation)).rejects.toMatchObject({ code: "CIRCUIT_OPEN" });
    expect(concurrentOperation).not.toHaveBeenCalled();

    probeResult.resolve("recovered");
    await expect(probe).resolves.toBe("recovered");
    await expect(breaker.execute(async () => "closed again")).resolves.toBe("closed again");
  });

  it.each([httpFailure(404), httpFailure(401), httpFailure(429, 1000)])(
    "does not count non-transient failures toward opening the circuit",
    async (failure) => {
      let now = 0;
      const breaker = new EnrichmentCircuitBreaker({ now: () => now });

      for (let attempt = 0; attempt < 5; attempt += 1) {
        await expect(breaker.execute(async () => {
          throw failure;
        })).rejects.toBe(failure);
      }

      await expect(breaker.execute(async () => "still closed")).resolves.toBe("still closed");
      now += 30_000;
    }
  );

  it("resets the consecutive failure count after a successful request", async () => {
    const breaker = new EnrichmentCircuitBreaker({ now: () => 0 });
    const transientFailure = httpFailure(500);

    for (let attempt = 0; attempt < 4; attempt += 1) {
      await expect(breaker.execute(async () => {
        throw transientFailure;
      })).rejects.toBe(transientFailure);
    }

    await expect(breaker.execute(async () => "success")).resolves.toBe("success");

    for (let attempt = 0; attempt < 4; attempt += 1) {
      await expect(breaker.execute(async () => {
        throw transientFailure;
      })).rejects.toBe(transientFailure);
    }

    await expect(breaker.execute(async () => "still closed")).resolves.toBe("still closed");
  });
});

describe("EnrichmentWorker dead letter handling", () => {
  it("moves an exhausted transient failure to the DLQ once without automatic replay", async () => {
    const message = { run_id: "run-1", seq: 3, sku: "sku-003" };
    const failure = networkFailure();
    const enrichmentClient = { fetch: vi.fn().mockRejectedValue(failure) };
    const retryPolicy = new EnrichmentRetryPolicy({ sleep: async () => undefined });
    const circuitBreaker = new EnrichmentCircuitBreaker({ now: () => 0 });
    const runStateRepository = {
      recordSuccess: vi.fn(async () => undefined),
      recordTerminalFailure: vi.fn(async () => undefined)
    };
    const deadLetterPublisher = { publish: vi.fn(async () => undefined) };
    const worker = new EnrichmentWorker({
      enrichmentClient,
      retryPolicy,
      circuitBreaker,
      runStateRepository,
      deadLetterPublisher
    });

    await worker.process(message);

    expect(enrichmentClient.fetch).toHaveBeenCalledTimes(3);
    expect(runStateRepository.recordTerminalFailure).toHaveBeenCalledTimes(1);
    expect(deadLetterPublisher.publish).toHaveBeenCalledTimes(1);
    expect(deadLetterPublisher.publish).toHaveBeenCalledWith(message, failure);
    expect(runStateRepository.recordSuccess).not.toHaveBeenCalled();
  });

  it("sends a permanent 404 to the DLQ without retrying the SKU", async () => {
    const message = { run_id: "run-1", seq: 3, sku: "missing-sku" };
    const failure = httpFailure(404);
    const enrichmentClient = { fetch: vi.fn().mockRejectedValue(failure) };
    const retryPolicy = new EnrichmentRetryPolicy({ sleep: async () => undefined });
    const circuitBreaker = new EnrichmentCircuitBreaker({ now: () => 0 });
    const runStateRepository = {
      recordSuccess: vi.fn(async () => undefined),
      recordTerminalFailure: vi.fn(async () => undefined)
    };
    const deadLetterPublisher = { publish: vi.fn(async () => undefined) };
    const worker = new EnrichmentWorker({
      enrichmentClient,
      retryPolicy,
      circuitBreaker,
      runStateRepository,
      deadLetterPublisher
    });

    await worker.process(message);

    expect(enrichmentClient.fetch).toHaveBeenCalledTimes(1);
    expect(deadLetterPublisher.publish).toHaveBeenCalledTimes(1);
    expect(runStateRepository.recordTerminalFailure).toHaveBeenCalledTimes(1);
  });
});
