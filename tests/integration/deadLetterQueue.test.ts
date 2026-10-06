import { Queue } from "bullmq";
import { randomUUID, createHash } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { EnrichmentCircuitBreaker } from "../../src/application/enrichmentCircuitBreaker.js";
import { EnrichmentRetryPolicy } from "../../src/application/enrichmentRetryPolicy.js";
import { DeadLetterQueue } from "../../src/infrastructure/queue/deadLetterQueue.js";
import { DEAD_LETTER_QUEUE_NAME } from "../../src/infrastructure/queue/enrichmentQueue.js";
import { EnrichmentWorker } from "../../src/worker/enrichmentWorker.js";
import { RunStateRepository } from "../../src/infrastructure/redis/runStateRepository.js";
import { ProcessMessageDto } from "../../src/shared/dto/processMessageDto.js";
import { createTestRedis } from "./redisTestSupport.js";

describe("Redis-backed dead letter handling", () => {
  const redis = createTestRedis();

  afterAll(async () => {
    await redis.quit();
  });

  it("retains a terminal failure without replaying the enrichment on redelivery", async () => {
    const inspectionRedis = createTestRedis();
    const inspectionQueue = new Queue(DEAD_LETTER_QUEUE_NAME, { connection: inspectionRedis });
    const deadLetterQueue = new DeadLetterQueue(redis);
    const message = new ProcessMessageDto(randomUUID(), 6, "missing-sku");
    const runIdHash = createHash("sha256").update(message.run_id).digest("hex");
    const expectedFailure = Object.assign(new Error("platform returned 404"), { statusCode: 404, code: "HTTP_404" });
    const runStateRepository = new RunStateRepository(redis);
    const enrichmentClient = { fetch: async (_sku: string) => { throw expectedFailure; } };
    let enrichmentCalls = 0;
    const worker = new EnrichmentWorker({
      enrichmentClient: {
        fetch: async (sku: string) => {
          enrichmentCalls += 1;
          return enrichmentClient.fetch(sku);
        }
      },
      retryPolicy: new EnrichmentRetryPolicy({ sleep: async () => undefined }),
      circuitBreaker: new EnrichmentCircuitBreaker({ now: () => 0 }),
      runStateRepository,
      deadLetterPublisher: deadLetterQueue
    });

    try {
      await inspectionQueue.waitUntilReady();
      await worker.process(message);
      await worker.process(message);

      const waitingJobs = await inspectionQueue.getJobs(["waiting"]);
      const snapshot = await runStateRepository.getSnapshot(message.run_id);

      expect(enrichmentCalls).toBe(1);
      expect(waitingJobs).toHaveLength(1);
      expect(waitingJobs[0]?.data).toMatchObject({ run_id: message.run_id, seq: 6, sku: "missing-sku" });
      expect(waitingJobs[0]?.data).not.toHaveProperty("token");
      expect(snapshot.terminalFailures).toEqual([{ seq: 6, reason: "HTTP_404" }]);
    } finally {
      const jobs = await inspectionQueue.getJobs(["waiting", "active", "delayed", "failed", "completed"]);
      await Promise.all(jobs.map((job) => job.remove()));
      await inspectionQueue.close();
      await deadLetterQueue.close();
      await redis.del(`run:${runIdHash}:state`, `run:${runIdHash}:status`, `run:${runIdHash}:results`, `run:${runIdHash}:failures`);
      await inspectionRedis.quit();
    }
  });
});
