import { describe, expect, it, vi } from "vitest";
import { ProcessMessageService } from "../../src/application/processMessageService.js";
import { ReprocessFailedSkuService } from "../../src/application/reprocessFailedSkuService.js";
import { ProcessMessageDto } from "../../src/shared/dto/processMessageDto.js";

type Snapshot = {
  run_id: string;
  cid: string | null;
  total: number | null;
  results: Array<{ seq: number; sku: string; price: number; stock: number }>;
  terminalFailures: Array<{ seq: number; reason: string }>;
  finalizationState: "COLLECTING" | "READY" | "CALLBACK_ATTEMPTED" | "SENT" | "UNKNOWN" | "BLOCKED_FOR_POLICY";
};

const failedMessage = new ProcessMessageDto("run-1", 2, "original-sku-002");

function createReprocessor(
  snapshot: Snapshot,
  replayTickets: string[] = ["replay-ticket-run-1-2"]
) {
  let reservationIndex = 0;
  const runStateRepository = {
    getSnapshot: vi.fn(async () => snapshot),
    getOriginalFailedMessage: vi.fn(async () => failedMessage),
    reserveFailedSkuReprocess: vi.fn(async () => replayTickets[reservationIndex++] ?? replayTickets.at(-1)!)
  };
  const publisher = {
    enqueueExplicitReprocess: vi.fn(async (_message: ProcessMessageDto, _replayTicket: string) => undefined)
  };
  const service = new ReprocessFailedSkuService({ runStateRepository, publisher });
  return { service, runStateRepository, publisher };
}

function blockedSnapshot(overrides: Partial<Snapshot> = {}): Snapshot {
  return {
    run_id: "run-1",
    cid: "cid-1",
    total: 3,
    results: [{ seq: 1, sku: "sku-001", price: 1.5, stock: 4 }],
    terminalFailures: [
      { seq: 2, reason: "HTTP_404" },
      { seq: 3, reason: "HTTP_500" }
    ],
    finalizationState: "BLOCKED_FOR_POLICY",
    ...overrides
  };
}

describe("ReprocessFailedSkuService", () => {
  it("explicitly requeues only the failed sequence with its original SKU and identity", async () => {
    const { service, runStateRepository, publisher } = createReprocessor(blockedSnapshot());

    const accepted = await service.execute("run-1", 2);

    expect(accepted).toBe(true);
    expect(runStateRepository.getSnapshot).toHaveBeenCalledExactlyOnceWith("run-1");
    expect(runStateRepository.getOriginalFailedMessage).toHaveBeenCalledExactlyOnceWith("run-1", 2);
    expect(runStateRepository.reserveFailedSkuReprocess).toHaveBeenCalledExactlyOnceWith("run-1", 2);
    expect(publisher.enqueueExplicitReprocess).toHaveBeenCalledExactlyOnceWith(
      failedMessage,
      "replay-ticket-run-1-2"
    );
    expect(publisher.enqueueExplicitReprocess).toHaveBeenCalledWith(
      expect.objectContaining({ run_id: "run-1", seq: 2, sku: "original-sku-002" }),
      "replay-ticket-run-1-2"
    );
  });

  it.each(["CALLBACK_ATTEMPTED", "SENT", "UNKNOWN"] as const)(
    "rejects a reprocess request when callback state is %s",
    async (finalizationState) => {
      const { service, runStateRepository, publisher } = createReprocessor(
        blockedSnapshot({ finalizationState })
      );

      const accepted = await service.execute("run-1", 2);

      expect(accepted).toBe(false);
      expect(runStateRepository.getOriginalFailedMessage).not.toHaveBeenCalled();
      expect(runStateRepository.reserveFailedSkuReprocess).not.toHaveBeenCalled();
      expect(publisher.enqueueExplicitReprocess).not.toHaveBeenCalled();
    }
  );

  it.each(["COLLECTING", "READY"] as const)(
    "rejects a reprocess request when the run is not blocked by policy (%s)",
    async (finalizationState) => {
      const { service, runStateRepository, publisher } = createReprocessor(
        blockedSnapshot({ finalizationState })
      );

      const accepted = await service.execute("run-1", 2);

      expect(accepted).toBe(false);
      expect(runStateRepository.getOriginalFailedMessage).not.toHaveBeenCalled();
      expect(runStateRepository.reserveFailedSkuReprocess).not.toHaveBeenCalled();
      expect(publisher.enqueueExplicitReprocess).not.toHaveBeenCalled();
    }
  );

  it("rejects a sequence that is not currently a terminal failure", async () => {
    const { service, runStateRepository, publisher } = createReprocessor(blockedSnapshot());

    const accepted = await service.execute("run-1", 1);

    expect(accepted).toBe(false);
    expect(runStateRepository.getOriginalFailedMessage).not.toHaveBeenCalled();
    expect(runStateRepository.reserveFailedSkuReprocess).not.toHaveBeenCalled();
    expect(publisher.enqueueExplicitReprocess).not.toHaveBeenCalled();
  });

  it("does not reprocess a terminal failure on ordinary message redelivery", async () => {
    const { runStateRepository, publisher } = createReprocessor(blockedSnapshot());
    const enqueue = vi.fn(async (_message: ProcessMessageDto) => undefined);
    const processService = new ProcessMessageService({ publisher: { enqueue } });

    await processService.accept(failedMessage);

    expect(enqueue).toHaveBeenCalledExactlyOnceWith(failedMessage);
    expect(runStateRepository.getSnapshot).not.toHaveBeenCalled();
    expect(runStateRepository.getOriginalFailedMessage).not.toHaveBeenCalled();
    expect(runStateRepository.reserveFailedSkuReprocess).not.toHaveBeenCalled();
    expect(publisher.enqueueExplicitReprocess).not.toHaveBeenCalled();
  });

  it("reuses an active replay ticket when the explicit command is repeated", async () => {
    const { service, runStateRepository, publisher } = createReprocessor(
      blockedSnapshot(),
      ["active-replay-ticket", "active-replay-ticket"]
    );

    await service.execute("run-1", 2);
    await service.execute("run-1", 2);

    expect(runStateRepository.reserveFailedSkuReprocess).toHaveBeenCalledTimes(2);
    expect(publisher.enqueueExplicitReprocess).toHaveBeenCalledTimes(2);
    expect(publisher.enqueueExplicitReprocess).toHaveBeenNthCalledWith(
      1,
      failedMessage,
      "active-replay-ticket"
    );
    expect(publisher.enqueueExplicitReprocess).toHaveBeenNthCalledWith(
      2,
      failedMessage,
      "active-replay-ticket"
    );
  });

  it("uses one stable replay ticket for concurrent requests of the same sequence", async () => {
    const { service, runStateRepository, publisher } = createReprocessor(
      blockedSnapshot(),
      ["stable-replay-ticket", "stable-replay-ticket"]
    );

    await Promise.all([
      service.execute("run-1", 2),
      service.execute("run-1", 2)
    ]);

    expect(runStateRepository.reserveFailedSkuReprocess).toHaveBeenCalledTimes(2);
    expect(publisher.enqueueExplicitReprocess).toHaveBeenCalledTimes(2);
    expect(publisher.enqueueExplicitReprocess).toHaveBeenNthCalledWith(
      1,
      failedMessage,
      "stable-replay-ticket"
    );
    expect(publisher.enqueueExplicitReprocess).toHaveBeenNthCalledWith(
      2,
      failedMessage,
      "stable-replay-ticket"
    );
  });
});
