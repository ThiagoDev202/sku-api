import { Queue, type JobsOptions } from "bullmq";
import type { ProcessMessageDto } from "../../shared/dto/processMessageDto.js";
import type { Redis } from "ioredis";

export const ENRICHMENT_QUEUE_NAME = "sku-enrichment";
export const DEAD_LETTER_QUEUE_NAME = "enrichment-dlq";

export class EnrichmentQueue {
  readonly queue: Queue<ProcessMessageDto>;

  constructor(connection: Redis) {
    this.queue = new Queue<ProcessMessageDto>(ENRICHMENT_QUEUE_NAME, {
      connection,
      defaultJobOptions: {
        removeOnComplete: false,
        removeOnFail: false,
        attempts: 1000,
        backoff: { type: "fixed", delay: 2000 }
      }
    });
  }

  async setGlobalConcurrency(limit: number): Promise<void> {
    await this.queue.setGlobalConcurrency(limit);
  }

  async add(message: ProcessMessageDto, options: JobsOptions): Promise<void> {
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.queue.add("enrich-sku", message, options),
        new Promise<never>((_, reject) => {
          timeout = setTimeout(() => reject(new Error("Queue acceptance timed out")), 350);
        })
      ]);
    } finally {
      if (timeout) {
        clearTimeout(timeout);
      }
    }
  }

  async close(): Promise<void> {
    await this.queue.close();
  }
}
