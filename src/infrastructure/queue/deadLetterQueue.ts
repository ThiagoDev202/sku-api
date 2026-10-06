import { Queue } from "bullmq";
import type { Redis } from "ioredis";
import type { ProcessMessageDto } from "../../shared/dto/processMessageDto.js";
import { DEAD_LETTER_QUEUE_NAME } from "./enrichmentQueue.js";
import { deadLetterJobId } from "../platform/platformApiClient.js";

export class DeadLetterQueue {
  private readonly queue: Queue;

  constructor(connection: Redis) {
    this.queue = new Queue(DEAD_LETTER_QUEUE_NAME, { connection });
  }

  async publish(message: ProcessMessageDto, failure: unknown): Promise<void> {
    const details = describeFailure(failure);
    await this.queue.add("terminal-enrichment-failure", {
      run_id: message.run_id,
      seq: message.seq,
      sku: message.sku,
      failure: details
    }, {
      jobId: deadLetterJobId(message),
      removeOnComplete: false,
      removeOnFail: false
    });
  }

  async close(): Promise<void> {
    await this.queue.close();
  }
}

function describeFailure(failure: unknown): { statusCode: number | null; errorCode: string; message: string } {
  if (typeof failure !== "object" || failure === null) {
    return { statusCode: null, errorCode: "UNKNOWN", message: "Enrichment failed" };
  }
  const candidate = failure as Record<string, unknown>;
  return {
    statusCode: typeof candidate.statusCode === "number" ? candidate.statusCode : null,
    errorCode: typeof candidate.code === "string" ? candidate.code : "ENRICHMENT_FAILED",
    message: "Enrichment failed"
  };
}
