import { describe, expect, it, vi } from "vitest";
import { RunFinalizer } from "../../src/application/runFinalizer.js";

type SuccessfulResult = {
  seq: number;
  sku: string;
  price: number;
  stock: number;
};

type RunSnapshot = {
  run_id: string;
  cid: string;
  total: number | null;
  results: SuccessfulResult[];
  terminalFailures: Array<{ seq: number; reason: string }>;
};

type CallbackPayload = {
  cid: string;
  run_id: string;
  result: SuccessfulResult[];
};

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function createFinalizer(snapshot: RunSnapshot, reserveCallback = vi.fn(async () => true)) {
  const runStateRepository = {
    getSnapshot: vi.fn(async () => snapshot),
    reserveCallback,
    markFinalizationState: vi.fn(async () => undefined)
  };
  const platformClient = { sendCallback: vi.fn(async (_payload: CallbackPayload): Promise<void> => undefined) };
  const finalizer = new RunFinalizer({ runStateRepository, platformClient });
  return { finalizer, runStateRepository, platformClient };
}

describe("RunFinalizer", () => {
  it("does not send progress callbacks while expected items are still missing", async () => {
    const snapshot: RunSnapshot = {
      run_id: "run-1",
      cid: "cid-1",
      total: 2,
      results: [{ seq: 4, sku: "sku-004", price: 4.5, stock: 2 }],
      terminalFailures: []
    };
    const { finalizer, runStateRepository, platformClient } = createFinalizer(snapshot);

    await finalizer.finalize(snapshot.run_id);

    expect(platformClient.sendCallback).not.toHaveBeenCalled();
    expect(runStateRepository.reserveCallback).not.toHaveBeenCalled();
  });

  it("sends one successful callback with results ordered by seq and public fields only", async () => {
    const snapshot = {
      run_id: "run-1",
      cid: "cid-1",
      total: 3,
      results: [
        { seq: 9, sku: "sku-009", price: 9.5, stock: 3, token: "private" },
        { seq: 2, sku: "sku-002", price: 2.5, stock: 8, internalState: "SUCCEEDED" },
        { seq: 5, sku: "sku-005", price: 5.5, stock: 1, retryCount: 2 }
      ],
      terminalFailures: []
    };
    const { finalizer, platformClient } = createFinalizer(snapshot);

    await finalizer.finalize(snapshot.run_id);

    expect(platformClient.sendCallback).toHaveBeenCalledTimes(1);
    expect(platformClient.sendCallback).toHaveBeenCalledWith({
      cid: "cid-1",
      run_id: "run-1",
      result: [
        { seq: 2, sku: "sku-002", price: 2.5, stock: 8 },
        { seq: 5, sku: "sku-005", price: 5.5, stock: 1 },
        { seq: 9, sku: "sku-009", price: 9.5, stock: 3 }
      ]
    });
    expect(Object.keys(platformClient.sendCallback.mock.calls[0]![0]!)).toEqual(["cid", "run_id", "result"]);
    for (const result of platformClient.sendCallback.mock.calls[0]![0]!.result) {
      expect(Object.keys(result)).toEqual(["seq", "sku", "price", "stock"]);
    }
  });

  it("allows only one concurrent finalizer to send the callback", async () => {
    const snapshot: RunSnapshot = {
      run_id: "run-1",
      cid: "cid-1",
      total: 1,
      results: [{ seq: 10, sku: "sku-010", price: 10, stock: 2 }],
      terminalFailures: []
    };
    let reservations = 0;
    const reserveCallback = vi.fn(async () => {
      reservations += 1;
      return reservations === 1;
    });
    const { finalizer, platformClient } = createFinalizer(snapshot, reserveCallback);
    const callbackAcceptance = deferred<void>();
    const callbackStarted = deferred<void>();
    platformClient.sendCallback.mockImplementation(() => {
      callbackStarted.resolve(undefined);
      return callbackAcceptance.promise;
    });

    const firstFinalization = finalizer.finalize(snapshot.run_id);
    const secondFinalization = finalizer.finalize(snapshot.run_id);
    await callbackStarted.promise;

    expect(platformClient.sendCallback).toHaveBeenCalledTimes(1);

    callbackAcceptance.resolve(undefined);
    await Promise.all([firstFinalization, secondFinalization]);

    expect(platformClient.sendCallback).toHaveBeenCalledTimes(1);
  });

  it("blocks callback and preserves results and failures when any item has a terminal failure", async () => {
    const snapshot: RunSnapshot = {
      run_id: "run-1",
      cid: "cid-1",
      total: 2,
      results: [{ seq: 1, sku: "sku-001", price: 1, stock: 7 }],
      terminalFailures: [{ seq: 2, reason: "not_found" }]
    };
    const { finalizer, runStateRepository, platformClient } = createFinalizer(snapshot);

    await finalizer.finalize(snapshot.run_id);

    expect(platformClient.sendCallback).not.toHaveBeenCalled();
    expect(runStateRepository.reserveCallback).not.toHaveBeenCalled();
    expect(runStateRepository.markFinalizationState).toHaveBeenCalledWith("run-1", "BLOCKED_FOR_POLICY");
    expect(snapshot.results).toEqual([{ seq: 1, sku: "sku-001", price: 1, stock: 7 }]);
    expect(snapshot.terminalFailures).toEqual([{ seq: 2, reason: "not_found" }]);
  });

  it("does not retry a callback after an ambiguous outcome", async () => {
    const snapshot: RunSnapshot = {
      run_id: "run-1",
      cid: "cid-1",
      total: 1,
      results: [{ seq: 1, sku: "sku-001", price: 1, stock: 7 }],
      terminalFailures: []
    };
    const reserveCallback = vi.fn().mockResolvedValueOnce(true).mockResolvedValue(false);
    const { finalizer, runStateRepository, platformClient } = createFinalizer(snapshot, reserveCallback);
    const ambiguousFailure = new Error("callback request timed out after send");
    platformClient.sendCallback.mockRejectedValue(ambiguousFailure);

    await finalizer.finalize(snapshot.run_id).catch(() => undefined);
    await finalizer.finalize(snapshot.run_id);

    expect(platformClient.sendCallback).toHaveBeenCalledTimes(1);
    expect(runStateRepository.markFinalizationState).toHaveBeenCalledWith("run-1", "UNKNOWN");
  });

  it("waits for burst metadata before deciding that a run is complete", async () => {
    const snapshot: RunSnapshot = {
      run_id: "run-1",
      cid: "cid-1",
      total: null,
      results: [{ seq: 1, sku: "sku-001", price: 1, stock: 7 }],
      terminalFailures: []
    };
    const { finalizer, runStateRepository, platformClient } = createFinalizer(snapshot);

    await finalizer.finalize(snapshot.run_id);

    expect(platformClient.sendCallback).not.toHaveBeenCalled();
    expect(runStateRepository.reserveCallback).not.toHaveBeenCalled();
  });
});
