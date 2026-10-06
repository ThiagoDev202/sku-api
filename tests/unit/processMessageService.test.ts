import { describe, expect, it, vi } from "vitest";
import { ProcessMessageService } from "../../src/application/processMessageService.js";

const message = { run_id: "run-1", seq: 7, sku: "sku-007" };

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

describe("ProcessMessageService", () => {
  it("does not complete the request before the queue accepts the message", async () => {
    const queueAcceptance = deferred<void>();
    const publisher = { enqueue: vi.fn(() => queueAcceptance.promise) };
    const service = new ProcessMessageService({ publisher });
    let requestCompleted = false;

    const request = service.accept(message).then(() => {
      requestCompleted = true;
    });

    await Promise.resolve();
    expect(requestCompleted).toBe(false);

    queueAcceptance.resolve(undefined);
    await request;

    expect(requestCompleted).toBe(true);
    expect(publisher.enqueue).toHaveBeenCalledWith(message);
  });

  it("rejects when the queue does not accept the message", async () => {
    const queueFailure = new Error("queue unavailable");
    const publisher = { enqueue: vi.fn().mockRejectedValue(queueFailure) };
    const service = new ProcessMessageService({ publisher });

    await expect(service.accept(message)).rejects.toBe(queueFailure);
  });

  it("completes after queue acceptance while enrichment remains pending", async () => {
    const enrichment = deferred<void>();
    let enrichmentCompleted = false;
    const publisher = {
      enqueue: vi.fn(async () => {
        void enrichment.promise.then(() => {
          enrichmentCompleted = true;
        });
      })
    };
    const service = new ProcessMessageService({ publisher });

    await service.accept(message);

    expect(enrichmentCompleted).toBe(false);

    enrichment.resolve(undefined);
    await enrichment.promise;
    await Promise.resolve();

    expect(enrichmentCompleted).toBe(true);
  });
});
