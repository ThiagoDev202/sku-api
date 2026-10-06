import { createHash } from "node:crypto";
import type { ProcessMessageDto } from "../../shared/dto/processMessageDto.js";

export type ProcessMessageIdentity = Pick<ProcessMessageDto, "run_id" | "seq">;
export type ProcessMessageJobOptions = {
  jobId: string;
  removeOnComplete: false;
  removeOnFail: false;
};

export interface ProcessMarkerStore {
  findAccepted(message: ProcessMessageIdentity): Promise<boolean>;
  markAccepted(message: ProcessMessageIdentity): Promise<void>;
}

export interface ProcessQueue {
  add(message: ProcessMessageDto, options: ProcessMessageJobOptions): Promise<void>;
}

export class ProcessMessagePublisher {
  constructor(private readonly dependencies: { queue: ProcessQueue; records: ProcessMarkerStore }) {}

  async enqueue(message: ProcessMessageDto): Promise<void> {
    if (await this.dependencies.records.findAccepted(message)) {
      return;
    }

    await this.dependencies.queue.add(message, {
      jobId: this.createJobId(message),
      removeOnComplete: false,
      removeOnFail: false
    });
    await this.dependencies.records.markAccepted(message);
  }

  private createJobId(message: ProcessMessageIdentity): string {
    return createHash("sha256")
      .update(JSON.stringify([message.run_id, message.seq]))
      .digest("hex");
  }
}
