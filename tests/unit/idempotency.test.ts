import { describe, expect, it } from "vitest";
import { ProcessMessagePublisher } from "../../src/infrastructure/queue/processMessagePublisher.js";

type Message = { run_id: string; seq: number; sku: string };
type QueueJobOptions = {
  jobId: string;
  removeOnComplete: false;
  removeOnFail: false;
};
type QueueAddCall = { message: Message; options: QueueJobOptions };

function messageKey(message: Pick<Message, "run_id" | "seq">): string {
  return JSON.stringify([message.run_id, message.seq]);
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

class InMemoryAcceptedMarkerStore {
  private readonly accepted = new Set<string>();
  readonly events: string[];
  nextWriteFailure: Error | undefined;

  constructor(events: string[] = []) {
    this.events = events;
  }

  async findAccepted(message: Pick<Message, "run_id" | "seq">): Promise<boolean> {
    this.events.push("marker.lookup");
    return this.accepted.has(messageKey(message));
  }

  async markAccepted(message: Pick<Message, "run_id" | "seq">): Promise<void> {
    this.events.push("marker.write");
    if (this.nextWriteFailure) {
      const failure = this.nextWriteFailure;
      this.nextWriteFailure = undefined;
      throw failure;
    }
    this.accepted.add(messageKey(message));
  }

  hasAccepted(message: Pick<Message, "run_id" | "seq">): boolean {
    return this.accepted.has(messageKey(message));
  }
}

class DedupeQueueFake {
  readonly addCalls: QueueAddCall[] = [];
  readonly events: string[];
  private readonly acceptedJobs = new Map<string, Message>();
  private readonly pendingJobs = new Map<string, Promise<void>>();

  constructor(
    private readonly acceptJob: (message: Message, options: QueueJobOptions) => Promise<void> = async () => undefined,
    events: string[] = []
  ) {
    this.events = events;
  }

  add(message: Message, options: QueueJobOptions): Promise<void> {
    this.events.push("queue.add");
    this.addCalls.push({ message, options });

    const acceptedJob = this.acceptedJobs.get(options.jobId);
    if (acceptedJob) {
      return Promise.resolve();
    }

    const pendingJob = this.pendingJobs.get(options.jobId);
    if (pendingJob) {
      return pendingJob;
    }

    const acceptance = Promise.resolve()
      .then(() => this.acceptJob(message, options))
      .then(
        () => {
          this.events.push("queue.accepted");
          this.acceptedJobs.set(options.jobId, message);
          this.pendingJobs.delete(options.jobId);
        },
        (failure: unknown) => {
          this.pendingJobs.delete(options.jobId);
          throw failure;
        }
      );

    this.pendingJobs.set(options.jobId, acceptance);
    return acceptance;
  }

  get acceptedJobCount(): number {
    return this.acceptedJobs.size;
  }

  acceptedMessage(jobId: string): Message | undefined {
    return this.acceptedJobs.get(jobId);
  }
}

describe("ProcessMessagePublisher idempotency", () => {
  it("checks accepted state, waits for queue acceptance, then records the marker", async () => {
    const events: string[] = [];
    const queueAcceptance = deferred<void>();
    const records = new InMemoryAcceptedMarkerStore(events);
    const queue = new DedupeQueueFake(async () => queueAcceptance.promise, events);
    const publisher = new ProcessMessagePublisher({ queue, records });
    const message = { run_id: "run-1", seq: 4, sku: "sku-004" };
    let enqueueCompleted = false;

    const enqueue = publisher.enqueue(message).then(() => {
      enqueueCompleted = true;
    });
    await Promise.resolve();

    expect(enqueueCompleted).toBe(false);
    expect(records.hasAccepted(message)).toBe(false);
    expect(events).toEqual(["marker.lookup", "queue.add"]);
    expect(queue.addCalls[0]?.options.removeOnComplete).toBe(false);
    expect(queue.addCalls[0]?.options.removeOnFail).toBe(false);

    queueAcceptance.resolve(undefined);
    await enqueue;

    expect(events).toEqual(["marker.lookup", "queue.add", "queue.accepted", "marker.write"]);
    expect(records.hasAccepted(message)).toBe(true);
    expect(queue.acceptedJobCount).toBe(1);
  });

  it("lets concurrent duplicates wait on the same accepted job", async () => {
    const queueAcceptance = deferred<void>();
    const records = new InMemoryAcceptedMarkerStore();
    const queue = new DedupeQueueFake(async () => queueAcceptance.promise);
    const publisher = new ProcessMessagePublisher({ queue, records });
    const message = { run_id: "run-1", seq: 4, sku: "sku-004" };
    let firstCompleted = false;
    let duplicateCompleted = false;

    const firstEnqueue = publisher.enqueue(message).then(() => {
      firstCompleted = true;
    });
    const duplicateEnqueue = publisher.enqueue(message).then(() => {
      duplicateCompleted = true;
    });
    await Promise.resolve();

    expect(firstCompleted).toBe(false);
    expect(duplicateCompleted).toBe(false);
    expect(queue.acceptedJobCount).toBe(0);

    queueAcceptance.resolve(undefined);
    await Promise.all([firstEnqueue, duplicateEnqueue]);

    expect(firstCompleted).toBe(true);
    expect(duplicateCompleted).toBe(true);
    expect(queue.acceptedJobCount).toBe(1);
    expect(records.hasAccepted(message)).toBe(true);
    expect(new Set(queue.addCalls.map((call) => call.options.jobId)).size).toBe(1);
  });

  it("uses the accepted marker to skip a later duplicate", async () => {
    const records = new InMemoryAcceptedMarkerStore();
    const queue = new DedupeQueueFake();
    const publisher = new ProcessMessagePublisher({ queue, records });
    const message = { run_id: "run-1", seq: 4, sku: "sku-004" };

    await publisher.enqueue(message);
    await publisher.enqueue(message);

    expect(queue.acceptedJobCount).toBe(1);
    expect(queue.addCalls).toHaveLength(1);
    expect(records.hasAccepted(message)).toBe(true);
  });

  it("does not mark or confirm a message when the queue rejects before acceptance", async () => {
    const queueFailure = new Error("queue rejected before acceptance");
    let shouldFail = true;
    const records = new InMemoryAcceptedMarkerStore();
    const queue = new DedupeQueueFake(async () => {
      if (shouldFail) {
        shouldFail = false;
        throw queueFailure;
      }
    });
    const publisher = new ProcessMessagePublisher({ queue, records });
    const message = { run_id: "run-1", seq: 4, sku: "sku-004" };

    await expect(publisher.enqueue(message)).rejects.toBe(queueFailure);
    expect(records.hasAccepted(message)).toBe(false);
    expect(queue.acceptedJobCount).toBe(0);

    await expect(publisher.enqueue(message)).resolves.toBeUndefined();

    expect(records.hasAccepted(message)).toBe(true);
    expect(queue.acceptedJobCount).toBe(1);
    expect(queue.addCalls[0]?.options.jobId).toBe(queue.addCalls[1]?.options.jobId);
  });

  it("does not confirm a concurrent duplicate when the original job is rejected", async () => {
    const queueAcceptance = deferred<void>();
    const queueFailure = new Error("queue rejected before acceptance");
    const records = new InMemoryAcceptedMarkerStore();
    const queue = new DedupeQueueFake(async () => queueAcceptance.promise);
    const publisher = new ProcessMessagePublisher({ queue, records });
    const message = { run_id: "run-1", seq: 4, sku: "sku-004" };
    let firstCompleted = false;
    let duplicateCompleted = false;

    const firstEnqueue = publisher.enqueue(message).then(
      () => {
        firstCompleted = true;
      },
      () => undefined
    );
    const duplicateEnqueue = publisher.enqueue(message).then(
      () => {
        duplicateCompleted = true;
      },
      () => undefined
    );
    await Promise.resolve();

    expect(firstCompleted).toBe(false);
    expect(duplicateCompleted).toBe(false);
    expect(records.hasAccepted(message)).toBe(false);

    queueAcceptance.reject(queueFailure);
    await Promise.all([firstEnqueue, duplicateEnqueue]);

    expect(firstCompleted).toBe(false);
    expect(duplicateCompleted).toBe(false);
    expect(records.hasAccepted(message)).toBe(false);
    expect(queue.acceptedJobCount).toBe(0);
  });

  it("does not create a second job when marker persistence fails after queue acceptance", async () => {
    const markerFailure = new Error("process stopped before marker persistence");
    const records = new InMemoryAcceptedMarkerStore();
    records.nextWriteFailure = markerFailure;
    const queue = new DedupeQueueFake();
    const publisher = new ProcessMessagePublisher({ queue, records });
    const firstMessage = { run_id: "run-1", seq: 4, sku: "sku-first" };
    const redelivery = { run_id: "run-1", seq: 4, sku: "sku-replacement" };

    await publisher.enqueue(firstMessage).catch(() => undefined);

    expect(queue.acceptedJobCount).toBe(1);
    expect(records.hasAccepted(firstMessage)).toBe(false);

    await expect(publisher.enqueue(redelivery)).resolves.toBeUndefined();

    expect(queue.acceptedJobCount).toBe(1);
    expect(records.hasAccepted(firstMessage)).toBe(true);
    expect(queue.addCalls).toHaveLength(2);
    expect(queue.addCalls[0]?.options.jobId).toBe(queue.addCalls[1]?.options.jobId);
    expect(queue.acceptedMessage(queue.addCalls[0]!.options.jobId)).toEqual(firstMessage);
  });

  it("retains deterministic job identities for distinct run and sequence pairs", async () => {
    const records = new InMemoryAcceptedMarkerStore();
    const queue = new DedupeQueueFake();
    const publisher = new ProcessMessagePublisher({ queue, records });

    await publisher.enqueue({ run_id: "run-1", seq: 4, sku: "sku-004" });
    await publisher.enqueue({ run_id: "run-2", seq: 4, sku: "sku-004" });
    await publisher.enqueue({ run_id: "run-1", seq: 5, sku: "sku-005" });

    const jobIds = queue.addCalls.map((call) => call.options.jobId);
    expect(jobIds.every((jobId) => jobId.length > 0)).toBe(true);
    expect(new Set(jobIds).size).toBe(3);
    expect(queue.addCalls.every((call) => call.options.removeOnComplete === false)).toBe(true);
    expect(queue.addCalls.every((call) => call.options.removeOnFail === false)).toBe(true);
  });
});
