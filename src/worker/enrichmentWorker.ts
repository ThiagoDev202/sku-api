import type { EnrichmentCircuitBreaker } from "../application/enrichmentCircuitBreaker.js";
import type { EnrichmentRetryPolicy } from "../application/enrichmentRetryPolicy.js";
import type { EnrichmentResult } from "../domain/enrichmentResult.js";
import { EnrichmentResult as EnrichmentResultModel } from "../domain/enrichmentResult.js";
import { TerminalFailure } from "../domain/terminalFailure.js";
import type { ProcessMessageDto } from "../shared/dto/processMessageDto.js";
import type { EnrichmentResponseDto } from "../shared/dto/enrichmentResponseDto.js";

export interface EnrichmentClient {
  fetch(sku: string): Promise<EnrichmentResponseDto>;
}

export interface EnrichmentRunStateRepository {
  recordSuccess(message: ProcessMessageDto, result: EnrichmentResult): Promise<boolean | void>;
  recordTerminalFailure(message: ProcessMessageDto, failure: TerminalFailure): Promise<boolean | void>;
  recordExplicitReprocessSuccess?(message: ProcessMessageDto, result: EnrichmentResult): Promise<boolean | void>;
  recordExplicitReprocessFailure?(message: ProcessMessageDto, failure: TerminalFailure): Promise<boolean | void>;
  getMessageState?(message: ProcessMessageDto): Promise<
    | { state: "SUCCEEDED" }
    | { state: "FAILED_TERMINAL"; failure: { statusCode: number | null; errorCode: string; attempts: number } }
    | null
  >;
}

export interface DeadLetterPublisher {
  publish(message: ProcessMessageDto, failure: unknown): Promise<void>;
}

export interface RunCompletionFinalizer {
  finalize(runId: string): Promise<void>;
}

export class EnrichmentWorker {
  constructor(private readonly dependencies: {
    enrichmentClient: EnrichmentClient;
    retryPolicy: EnrichmentRetryPolicy;
    circuitBreaker: EnrichmentCircuitBreaker;
    runStateRepository: EnrichmentRunStateRepository;
    deadLetterPublisher: DeadLetterPublisher;
    finalizer?: RunCompletionFinalizer;
  }) {}

  async process(message: ProcessMessageDto, explicitReprocess = false): Promise<void> {
    if (explicitReprocess && (!this.dependencies.runStateRepository.recordExplicitReprocessSuccess ||
      !this.dependencies.runStateRepository.recordExplicitReprocessFailure)) {
      throw new Error("Explicit reprocess state operations are unavailable");
    }
    const existingState = await this.dependencies.runStateRepository.getMessageState?.(message);
    if (existingState?.state === "SUCCEEDED") {
      await this.dependencies.finalizer?.finalize(message.run_id);
      return;
    }
    if (existingState?.state === "FAILED_TERMINAL" && !explicitReprocess) {
      const storedFailure = Object.assign(new Error("Enrichment failed"), {
        code: existingState.failure.errorCode,
        statusCode: existingState.failure.statusCode ?? undefined
      });
      await this.dependencies.deadLetterPublisher.publish(message, storedFailure);
      await this.dependencies.finalizer?.finalize(message.run_id);
      return;
    }
    if (explicitReprocess && existingState?.state !== "FAILED_TERMINAL") {
      await this.dependencies.finalizer?.finalize(message.run_id);
      return;
    }

    let enrichment: EnrichmentResponseDto;
    let attempts = 0;
    try {
      enrichment = await this.dependencies.retryPolicy.execute(
        () => this.dependencies.circuitBreaker.execute(() => {
          attempts += 1;
          return this.dependencies.enrichmentClient.fetch(message.sku);
        })
      );
    } catch (failure) {
      const terminalFailure = new TerminalFailure(
        message.seq,
        message.sku,
        readStatusCode(failure),
        readErrorCode(failure),
        attempts
      );
      if (explicitReprocess && this.dependencies.runStateRepository.recordExplicitReprocessFailure) {
        await this.dependencies.runStateRepository.recordExplicitReprocessFailure(message, terminalFailure);
      } else {
        await this.dependencies.runStateRepository.recordTerminalFailure(message, terminalFailure);
      }
      await this.dependencies.deadLetterPublisher.publish(message, failure);
      await this.dependencies.finalizer?.finalize(message.run_id);
      return;
    }

    const result = EnrichmentResultModel.from(message.seq, enrichment);
    if (explicitReprocess && this.dependencies.runStateRepository.recordExplicitReprocessSuccess) {
      await this.dependencies.runStateRepository.recordExplicitReprocessSuccess(message, result);
    } else {
      await this.dependencies.runStateRepository.recordSuccess(message, result);
    }
    await this.dependencies.finalizer?.finalize(message.run_id);
  }
}

function readStatusCode(failure: unknown): number | null {
  if (typeof failure !== "object" || failure === null) {
    return null;
  }
  const statusCode = (failure as Record<string, unknown>).statusCode;
  return typeof statusCode === "number" ? statusCode : null;
}

function readErrorCode(failure: unknown): string {
  if (typeof failure !== "object" || failure === null) {
    return "UNKNOWN";
  }
  const code = (failure as Record<string, unknown>).code;
  if (typeof code === "string") {
    return code;
  }
  const statusCode = readStatusCode(failure);
  return statusCode === null ? "NETWORK_ERROR" : `HTTP_${statusCode}`;
}
