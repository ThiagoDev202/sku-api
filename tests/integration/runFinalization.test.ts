import { createHash, randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { RunFinalizer } from "../../src/application/runFinalizer.js";
import { EnrichmentCircuitBreaker } from "../../src/application/enrichmentCircuitBreaker.js";
import { EnrichmentRetryPolicy } from "../../src/application/enrichmentRetryPolicy.js";
import { EnrichmentWorker } from "../../src/worker/enrichmentWorker.js";
import { EnrichmentResult } from "../../src/domain/enrichmentResult.js";
import { TerminalFailure } from "../../src/domain/terminalFailure.js";
import { BurstResponseDto } from "../../src/shared/dto/burstResponseDto.js";
import { CallbackResponseDto } from "../../src/shared/dto/callbackResponseDto.js";
import { EnrichmentResponseDto } from "../../src/shared/dto/enrichmentResponseDto.js";
import { ExplicitReprocessPublisher } from "../../src/infrastructure/queue/explicitReprocessPublisher.js";
import type { ProcessMessageDto } from "../../src/shared/dto/processMessageDto.js";
import { RunStateRepository } from "../../src/infrastructure/redis/runStateRepository.js";
import { createTestRedis } from "./redisTestSupport.js";

describe("Redis-backed batch finalization", () => {
  const redis = createTestRedis();
  const runIds: string[] = [];

  afterEach(async () => {
    await Promise.all(runIds.splice(0).map((runId) => removeRun(redis, runId)));
  });

  afterAll(async () => {
    await redis.quit();
  });

  it("orders out-of-order results and reserves one callback across concurrent finalizers", async () => {
    const runId = randomUUID();
    runIds.push(runId);
    const repository = new RunStateRepository(redis);
    await repository.recordMetadata(new BurstResponseDto(runId, 3, new Date().toISOString()), "cid-integration");
    await repository.recordSuccess({ run_id: runId, seq: 9, sku: "sku-009" }, new EnrichmentResult(9, "sku-009", 9.5, 3));
    await repository.recordSuccess({ run_id: runId, seq: 2, sku: "sku-002" }, new EnrichmentResult(2, "sku-002", 2.5, 8));
    await repository.recordSuccess({ run_id: runId, seq: 5, sku: "sku-005" }, new EnrichmentResult(5, "sku-005", 5.5, 1));
    const platformClient = { sendCallback: vi.fn(async () => CallbackResponseDto.fromText('{"report_id":"report-1","metrics":{"average_ack_ms":4.63}}')) };
    const finalizer = new RunFinalizer({ runStateRepository: repository, platformClient });

    await Promise.all([finalizer.finalize(runId), finalizer.finalize(runId)]);

    expect(platformClient.sendCallback).toHaveBeenCalledTimes(1);
    expect(platformClient.sendCallback).toHaveBeenCalledWith({
      cid: "cid-integration",
      run_id: runId,
      result: [
        { seq: 2, sku: "sku-002", price: 2.5, stock: 8 },
        { seq: 5, sku: "sku-005", price: 5.5, stock: 1 },
        { seq: 9, sku: "sku-009", price: 9.5, stock: 3 }
      ]
    });
    expect((await repository.getSnapshot(runId)).finalizationState).toBe("SENT");
    const callbackResponse = await repository.getCallbackResponse(runId);
    expect(callbackResponse?.body).toBe('{"report_id":"report-1","metrics":{"average_ack_ms":4.63}}');
    expect(callbackResponse?.truncated).toBe(false);
    expect(callbackResponse?.capturedAt).toBeTruthy();
  });

  it("blocks the whole run and preserves successes and failures after a terminal SKU failure", async () => {
    const runId = randomUUID();
    runIds.push(runId);
    const repository = new RunStateRepository(redis);
    await repository.recordMetadata(new BurstResponseDto(runId, 2, new Date().toISOString()), "cid-integration");
    await repository.recordSuccess({ run_id: runId, seq: 1, sku: "sku-001" }, new EnrichmentResult(1, "sku-001", 1.5, 4));
    await repository.recordTerminalFailure(
      { run_id: runId, seq: 2, sku: "missing-sku" },
      new TerminalFailure(2, "missing-sku", 404, "HTTP_404", 1)
    );
    const platformClient = { sendCallback: vi.fn(async () => undefined) };
    const finalizer = new RunFinalizer({ runStateRepository: repository, platformClient });

    await finalizer.finalize(runId);

    expect(platformClient.sendCallback).not.toHaveBeenCalled();
    const snapshot = await repository.getSnapshot(runId);
    expect(snapshot.finalizationState).toBe("BLOCKED_FOR_POLICY");
    expect(snapshot.results).toEqual([{ seq: 1, sku: "sku-001", price: 1.5, stock: 4 }]);
    expect(snapshot.terminalFailures).toEqual([{ seq: 2, reason: "HTTP_404" }]);
  });

  it("reprocesses a failed sequence explicitly and sends one complete callback after recovery", async () => {
    const runId = randomUUID();
    runIds.push(runId);
    const repository = new RunStateRepository(redis);
    const message = { run_id: runId, seq: 2, sku: "sku-002" };
    await repository.recordMetadata(new BurstResponseDto(runId, 2, new Date().toISOString()), "cid-integration");
    await repository.recordSuccess({ run_id: runId, seq: 1, sku: "sku-001" }, new EnrichmentResult(1, "sku-001", 1.5, 4));
    await repository.recordTerminalFailure(message, new TerminalFailure(2, "sku-002", 404, "HTTP_404", 1));
    const callbackClient = { sendCallback: vi.fn(async () => undefined) };
    const finalizer = new RunFinalizer({ runStateRepository: repository, platformClient: callbackClient });
    await finalizer.finalize(runId);
    expect(callbackClient.sendCallback).not.toHaveBeenCalled();

    const ticket = await repository.reserveFailedSkuReprocess(runId, 2);
    expect(ticket).toBeTruthy();
    expect(await repository.getOriginalFailedMessage(runId, 2)).toEqual(message);

    const queue = new Queue<ProcessMessageDto>(`reprocess-test-${runId}`, { connection: redis });
    const publisher = new ExplicitReprocessPublisher(queue);
    try {
      await publisher.enqueueExplicitReprocess(message, ticket!);
      await publisher.enqueueExplicitReprocess(message, ticket!);
      const waitingJobs = await queue.getJobs(["waiting"]);
      expect(waitingJobs.filter((job) => job.data.run_id === runId)).toHaveLength(1);

      const worker = new EnrichmentWorker({
        enrichmentClient: {
          fetch: vi.fn(async (sku) => new EnrichmentResponseDto(sku, 2.5, 8))
        },
        retryPolicy: new EnrichmentRetryPolicy({ sleep: async () => undefined }),
        circuitBreaker: new EnrichmentCircuitBreaker(),
        runStateRepository: repository,
        deadLetterPublisher: { publish: vi.fn(async () => undefined) },
        finalizer
      });
      await worker.process(message, true);

      expect(callbackClient.sendCallback).toHaveBeenCalledExactlyOnceWith({
        cid: "cid-integration",
        run_id: runId,
        result: [
          { seq: 1, sku: "sku-001", price: 1.5, stock: 4 },
          { seq: 2, sku: "sku-002", price: 2.5, stock: 8 }
        ]
      });
      expect(await repository.getFailureHistory(runId)).toEqual([
        { seq: 2, sku: "sku-002", reason: "HTTP_404" }
      ]);
      expect((await repository.getSnapshot(runId)).finalizationState).toBe("SENT");
    } finally {
      await queue.obliterate({ force: true });
      await queue.close();
    }
  });

  it("marks an ambiguous callback outcome unknown and does not retry it", async () => {
    const runId = randomUUID();
    runIds.push(runId);
    const repository = new RunStateRepository(redis);
    await repository.recordMetadata(new BurstResponseDto(runId, 1, new Date().toISOString()), "cid-integration");
    await repository.recordSuccess({ run_id: runId, seq: 1, sku: "sku-001" }, new EnrichmentResult(1, "sku-001", 1.5, 4));
    const platformClient = { sendCallback: vi.fn().mockRejectedValue(new Error("callback outcome is ambiguous")) };
    const finalizer = new RunFinalizer({ runStateRepository: repository, platformClient });

    await finalizer.finalize(runId);
    await finalizer.finalize(runId);

    expect(platformClient.sendCallback).toHaveBeenCalledTimes(1);
    expect((await repository.getSnapshot(runId)).finalizationState).toBe("UNKNOWN");
  });
});

async function removeRun(redisClient: ReturnType<typeof createTestRedis>, runId: string): Promise<void> {
  const id = createHash("sha256").update(runId).digest("hex");
  await redisClient.del(`run:${id}:state`, `run:${id}:status`, `run:${id}:results`, `run:${id}:failures`, `run:${id}:failure-history`);
}
