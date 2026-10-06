import type { CallbackRequestDto, CallbackResultItem } from "../shared/dto/callbackRequestDto.js";
import type { CallbackResponseDto } from "../shared/dto/callbackResponseDto.js";
import type { RunFinalizationState, RunSnapshot } from "../infrastructure/redis/runStateRepository.js";

export interface RunStateReader {
  getSnapshot(runId: string): Promise<RunSnapshot>;
  reserveCallback(runId: string): Promise<boolean>;
  markFinalizationState(runId: string, state: RunFinalizationState): Promise<void>;
  recordCallbackResponse?(runId: string, response: CallbackResponseDto): Promise<void>;
}

export interface CallbackSender {
  sendCallback(payload: CallbackRequestDto): Promise<CallbackResponseDto | void>;
}

export class RunFinalizer {
  constructor(private readonly dependencies: { runStateRepository: RunStateReader; platformClient: CallbackSender }) {}

  async finalize(runId: string): Promise<void> {
    const snapshot = await this.dependencies.runStateRepository.getSnapshot(runId);
    if (snapshot.total === null || snapshot.cid === null) {
      return;
    }
    if (snapshot.terminalFailures.length > 0) {
      await this.dependencies.runStateRepository.markFinalizationState(runId, "BLOCKED_FOR_POLICY");
      return;
    }
    if (snapshot.results.length !== snapshot.total || snapshot.finalizationState === "BLOCKED_FOR_POLICY") {
      return;
    }

    const reserved = await this.dependencies.runStateRepository.reserveCallback(runId);
    if (!reserved) {
      return;
    }

    const result: CallbackResultItem[] = snapshot.results
      .map(({ seq, sku, price, stock }) => ({ seq, sku, price, stock }))
      .sort((left, right) => left.seq - right.seq);
    let response: CallbackResponseDto | void;
    try {
      response = await this.dependencies.platformClient.sendCallback({ cid: snapshot.cid, run_id: snapshot.run_id, result });
    } catch {
      await this.dependencies.runStateRepository.markFinalizationState(runId, "UNKNOWN");
      return;
    }
    try {
      await this.dependencies.runStateRepository.markFinalizationState(runId, "SENT");
    } catch {
      await this.dependencies.runStateRepository.markFinalizationState(runId, "UNKNOWN");
      return;
    }
    if (response && this.dependencies.runStateRepository.recordCallbackResponse) {
      try {
        await this.dependencies.runStateRepository.recordCallbackResponse(runId, response);
      } catch {
        return;
      }
    }
  }
}
