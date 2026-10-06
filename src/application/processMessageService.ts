import type { ProcessMessageDto } from "../shared/dto/processMessageDto.js";

export interface ProcessMessageEnqueuer {
  enqueue(message: ProcessMessageDto): Promise<void>;
}

export class ProcessMessageService {
  constructor(private readonly dependencies: { publisher: ProcessMessageEnqueuer }) {}

  async accept(message: ProcessMessageDto): Promise<void> {
    await this.dependencies.publisher.enqueue(message);
  }
}
