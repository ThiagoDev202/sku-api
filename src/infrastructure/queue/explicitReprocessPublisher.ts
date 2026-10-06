import { createHash } from "node:crypto";
import type { Queue } from "bullmq";
import type { ProcessMessageDto } from "../../shared/dto/processMessageDto.js";

export const EXPLICIT_REPROCESS_JOB_NAME = "explicit-reprocess-sku";

export class ExplicitReprocessPublisher {
  constructor(private readonly queue: Queue<ProcessMessageDto>) {}

  async enqueueExplicitReprocess(message: ProcessMessageDto, replayTicket: string): Promise<void> {
    const identity = createHash("sha256")
      .update(JSON.stringify([message.run_id, message.seq]))
      .digest("hex");
    await this.queue.add(EXPLICIT_REPROCESS_JOB_NAME, message, {
      jobId: `reprocess-${identity}-${replayTicket}`,
      removeOnComplete: false,
      removeOnFail: false
    });
  }
}
