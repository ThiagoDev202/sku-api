import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";

export const redisUrl = process.env.TEST_REDIS_URL ?? process.env.REDIS_URL ?? "redis://127.0.0.1:6379";

export function createTestRedis(): Redis {
  return new Redis(redisUrl, { maxRetriesPerRequest: null, connectTimeout: 2000 });
}

export function uniqueQueueName(prefix: string): string {
  return `${prefix}-${randomUUID()}`;
}

export function waitForEvent<T>(register: (resolve: (value: T) => void) => void, timeoutMs = 10000): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Timed out waiting for integration event")), timeoutMs);
    register((value) => {
      clearTimeout(timeout);
      resolve(value);
    });
  });
}
