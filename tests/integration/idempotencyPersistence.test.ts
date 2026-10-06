import { Queue, QueueEvents, Worker } from "bullmq";
import { createHash, randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { JobsOptions } from "bullmq";
import { ProcessMessagePublisher, type ProcessMessageJobOptions } from "../../src/infrastructure/queue/processMessagePublisher.js";
import { ProcessRecordRepository } from "../../src/infrastructure/redis/processRecordRepository.js";
import { ProcessMessageDto } from "../../src/shared/dto/processMessageDto.js";
import { createTestRedis, uniqueQueueName, waitForEvent } from "./redisTestSupport.js";

type ProcessPayload = { run_id: string; seq: number; sku: string };

describe("Redis-backed process idempotency", () => {
  it("deduplicates a redelivery after worker completion using a new publisher connection", async () => {
    const queueName = uniqueQueueName("idempotency");
    const queueRedis = createTestRedis();
    const restartedQueueRedis = createTestRedis();
    const recordsRedis = createTestRedis();
    const restartedRecordsRedis = createTestRedis();
    const workerRedis = createTestRedis();
    const eventsRedis = createTestRedis();
    const queue = new Queue<ProcessPayload>(queueName, { connection: queueRedis });
    const restartedQueue = new Queue<ProcessPayload>(queueName, { connection: restartedQueueRedis });
    const events = new QueueEvents(queueName, { connection: eventsRedis });
    let enrichmentCalls = 0;
    const worker = new Worker<ProcessPayload>(queueName, async () => {
      enrichmentCalls += 1;
    }, { connection: workerRedis, concurrency: 1 });
    const originalQueueAdapter = {
      add: async (message: ProcessMessageDto, options: ProcessMessageJobOptions) => {
        await queue.add("process-message", message, options as JobsOptions);
      }
    };
    const restartedQueueAdapter = {
      add: async (message: ProcessMessageDto, options: ProcessMessageJobOptions) => {
        await restartedQueue.add("process-message", message, options as JobsOptions);
      }
    };
    const originalPublisher = new ProcessMessagePublisher({
      queue: originalQueueAdapter,
      records: new ProcessRecordRepository(recordsRedis)
    });
    const restartedPublisher = new ProcessMessagePublisher({
      queue: restartedQueueAdapter,
      records: new ProcessRecordRepository(restartedRecordsRedis)
    });
    const message = new ProcessMessageDto(randomUUID(), 17, "sku-017");
    const markerId = createHash("sha256")
      .update(JSON.stringify([message.run_id, message.seq]))
      .digest("hex");

    try {
      await Promise.all([queue.waitUntilReady(), events.waitUntilReady(), worker.waitUntilReady()]);
      const completed = waitForEvent<void>((resolve) => {
        events.once("completed", () => resolve(undefined));
      });
      await originalPublisher.enqueue(message);
      await completed;
      await restartedPublisher.enqueue(message);

      expect(enrichmentCalls).toBe(1);
      expect(await queue.getJobCounts("completed")).toMatchObject({ completed: 1 });
      expect(await new ProcessRecordRepository(restartedRecordsRedis).findAccepted(message)).toBe(true);
    } finally {
      await worker.close();
      await events.close();
      await queue.obliterate({ force: true });
      await queue.close();
      await restartedQueue.close();
      await recordsRedis.del(`process-record:${markerId}`);
      await Promise.all([queueRedis.quit(), restartedQueueRedis.quit(), recordsRedis.quit(), restartedRecordsRedis.quit(), workerRedis.quit(), eventsRedis.quit()]);
    }
  });
});
