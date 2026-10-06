import { createHash } from "node:crypto";
import type { Redis } from "ioredis";
import type { ProcessMessageIdentity, ProcessMarkerStore } from "../queue/processMessagePublisher.js";

export class ProcessRecordRepository implements ProcessMarkerStore {
  constructor(private readonly redis: Redis) {}

  async findAccepted(message: ProcessMessageIdentity): Promise<boolean> {
    return (await this.redis.exists(this.keyFor(message))) === 1;
  }

  async markAccepted(message: ProcessMessageIdentity): Promise<void> {
    await this.redis.set(this.keyFor(message), "accepted", "NX");
  }

  private keyFor(message: ProcessMessageIdentity): string {
    const identity = createHash("sha256")
      .update(JSON.stringify([message.run_id, message.seq]))
      .digest("hex");
    return `process-record:${identity}`;
  }
}
