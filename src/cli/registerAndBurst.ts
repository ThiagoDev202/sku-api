import { AppConfig } from "../shared/config/appConfig.js";
import { RunFinalizer } from "../application/runFinalizer.js";
import { BurstResponseDto } from "../shared/dto/burstResponseDto.js";
import { RegisterRequestDto } from "../shared/dto/registerRequestDto.js";
import { AppLogger } from "../shared/logging/appLogger.js";
import { PlatformApiClient } from "../infrastructure/platform/platformApiClient.js";
import { EnrichmentQueue } from "../infrastructure/queue/enrichmentQueue.js";
import { ExplicitReprocessPublisher } from "../infrastructure/queue/explicitReprocessPublisher.js";
import { IntegrationCredentialRepository } from "../infrastructure/redis/integrationCredentialRepository.js";
import { RedisConnection } from "../infrastructure/redis/redisConnection.js";
import { RunStateRepository } from "../infrastructure/redis/runStateRepository.js";
import { ReprocessFailedSkuService } from "../application/reprocessFailedSkuService.js";

const logger = new AppLogger();

async function run(): Promise<void> {
  const action = process.argv[2];
  if (action !== "register" && action !== "burst" && action !== "reprocess-failure") {
    throw new Error("Use the register, burst, or reprocess-failure command");
  }
  const config = AppConfig.fromEnvironment();
  const redis = new RedisConnection(config.redisUrl).createClient();
  const enrichmentQueue = new EnrichmentQueue(redis);
  const credentials = new IntegrationCredentialRepository(redis);
  const platformClient = new PlatformApiClient({ baseUrl: config.platformBaseUrl, credentials });
  try {
    if (action === "register") {
      const registered = await platformClient.register(new RegisterRequestDto(config.registrationName, config.publicWebhookUrl));
      await credentials.save(registered);
      logger.info("integration_registered", { cid: registered.cid });
      return;
    }

    const runStateRepository = new RunStateRepository(redis);
    if (action === "burst") {
      const integration = await credentials.get();
      const burst = await platformClient.burst(integration.cid, integration.token);
      await runStateRepository.recordMetadata(burst, integration.cid);
      logger.info("burst_requested", { runId: burst.run_id, total: burst.total });
      const finalizer = new RunFinalizer({ runStateRepository, platformClient });
      await finalizer.finalize(burst.run_id);
      return;
    }

    const runId = process.argv[3];
    const seq = Number(process.argv[4]);
    if (!runId || !Number.isInteger(seq)) {
      throw new Error("Use reprocess-failure <run_id> <seq>");
    }
    const reprocessor = new ReprocessFailedSkuService({
      runStateRepository,
      publisher: new ExplicitReprocessPublisher(enrichmentQueue.queue)
    });
    const accepted = await reprocessor.execute(runId, seq);
    if (!accepted) {
      throw new Error("The run and sequence are not eligible for explicit reprocessing");
    }
    logger.info("failed_sku_reprocess_requested", { runId, seq });
  } finally {
    await enrichmentQueue.close();
    await redis.quit();
  }
}

run().catch((failure: unknown) => {
  logger.error("command_failed", { errorCode: failure instanceof Error ? failure.name : "UNKNOWN" });
  process.exitCode = 1;
});
