import type { RunFinalizationState } from "../infrastructure/redis/runStateRepository.js";
import type { ProcessMessageDto } from "../shared/dto/processMessageDto.js";

type ReprocessRunSnapshot = {
  finalizationState?: RunFinalizationState;
  terminalFailures: Array<{ seq: number; reason: string }>;
};

export interface FailedSkuReprocessState {
  getSnapshot(runId: string): Promise<ReprocessRunSnapshot>;
  getOriginalFailedMessage(runId: string, seq: number): Promise<ProcessMessageDto | null>;
  reserveFailedSkuReprocess(runId: string, seq: number): Promise<string | null>;
}

export interface ExplicitReprocessPublisher {
  enqueueExplicitReprocess(message: ProcessMessageDto, replayTicket: string): Promise<void>;
}

export class ReprocessFailedSkuService {
  constructor(private readonly dependencies: {
    runStateRepository: FailedSkuReprocessState;
    publisher: ExplicitReprocessPublisher;
  }) {}

  async execute(runId: string, seq: number): Promise<boolean> {
    const snapshot = await this.dependencies.runStateRepository.getSnapshot(runId);
    if (snapshot.finalizationState !== "BLOCKED_FOR_POLICY" ||
      !snapshot.terminalFailures.some((failure) => failure.seq === seq)) {
      return false;
    }

    const message = await this.dependencies.runStateRepository.getOriginalFailedMessage(runId, seq);
    if (!message || message.run_id !== runId || message.seq !== seq) {
      return false;
    }

    const replayTicket = await this.dependencies.runStateRepository.reserveFailedSkuReprocess(runId, seq);
    if (!replayTicket) {
      return false;
    }

    await this.dependencies.publisher.enqueueExplicitReprocess(message, replayTicket);
    return true;
  }
}
