import { Queue, QueueEvents, Worker } from "bullmq";
import { afterEach, describe, expect, it } from "vitest";
import { createTestRedis, uniqueQueueName, waitForEvent } from "./redisTestSupport.js";

describe("BullMQ global worker concurrency", () => {
  it("keeps calls in flight at or below three across two workers", async () => {
    const queueName = uniqueQueueName("concurrency");
    const queueRedis = createTestRedis();
    const workerRedisOne = createTestRedis();
    const workerRedisTwo = createTestRedis();
    const eventsRedis = createTestRedis();
    const queue = new Queue<{ seq: number }>(queueName, { connection: queueRedis });
    const events = new QueueEvents(queueName, { connection: eventsRedis });
    let inFlight = 0;
    let maximumInFlight = 0;
    let completedCount = 0;
    const workerTask = async (): Promise<void> => {
      inFlight += 1;
      maximumInFlight = Math.max(maximumInFlight, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 80));
      inFlight -= 1;
    };
    const workers = [
      new Worker<{ seq: number }>(queueName, workerTask, { connection: workerRedisOne, concurrency: 5 }),
      new Worker<{ seq: number }>(queueName, workerTask, { connection: workerRedisTwo, concurrency: 5 })
    ];

    try {
      await Promise.all([queue.waitUntilReady(), events.waitUntilReady(), ...workers.map((worker) => worker.waitUntilReady())]);
      await queue.setGlobalConcurrency(3);
      const allCompleted = waitForEvent<void>((resolve) => {
        events.on("completed", () => {
          completedCount += 1;
          if (completedCount === 9) {
            resolve(undefined);
          }
        });
      });
      await queue.addBulk(Array.from({ length: 9 }, (_, index) => ({
        name: "test-enrichment",
        data: { seq: index }
      })));
      await allCompleted;

      expect(maximumInFlight).toBe(3);
      expect(maximumInFlight).toBeLessThanOrEqual(3);
    } finally {
      await Promise.all(workers.map((worker) => worker.close()));
      await events.close();
      await queue.obliterate({ force: true });
      await queue.close();
      await Promise.all([queueRedis.quit(), workerRedisOne.quit(), workerRedisTwo.quit(), eventsRedis.quit()]);
    }
  });
});
