import { Queue, Worker } from "bullmq";
import { EnrichmentCircuitBreaker } from "../application/enrichmentCircuitBreaker.js";
import { EnrichmentRetryPolicy } from "../application/enrichmentRetryPolicy.js";
import { RunFinalizer } from "../application/runFinalizer.js";
import { AppConfig } from "../shared/config/appConfig.js";
import type { ProcessMessageDto } from "../shared/dto/processMessageDto.js";
import { AppLogger } from "../shared/logging/appLogger.js";
import { PlatformApiClient } from "../infrastructure/platform/platformApiClient.js";
import { ENRICHMENT_QUEUE_NAME } from "../infrastructure/queue/enrichmentQueue.js";
import { EXPLICIT_REPROCESS_JOB_NAME } from "../infrastructure/queue/explicitReprocessPublisher.js";
import { DeadLetterQueue } from "../infrastructure/queue/deadLetterQueue.js";
import { IntegrationCredentialRepository } from "../infrastructure/redis/integrationCredentialRepository.js";
import { RedisConnection } from "../infrastructure/redis/redisConnection.js";
import { RunStateRepository } from "../infrastructure/redis/runStateRepository.js";
import { EnrichmentWorker } from "./enrichmentWorker.js";

const config = AppConfig.fromEnvironment();
const redisConnection = new RedisConnection(config.redisUrl);
const redis = redisConnection.createClient({ maxRetriesPerRequest: null });
const queueRedis = redisConnection.createClient({ maxRetriesPerRequest: null });
const credentials = new IntegrationCredentialRepository(redis);
const runStateRepository = new RunStateRepository(redis);
const platformClient = new PlatformApiClient({ baseUrl: config.platformBaseUrl, credentials });
const deadLetterQueue = new DeadLetterQueue(queueRedis);
const logger = new AppLogger();
const finalizer = new RunFinalizer({ runStateRepository, platformClient });
const service = new EnrichmentWorker({
  enrichmentClient: platformClient,
  retryPolicy: new EnrichmentRetryPolicy(),
  circuitBreaker: new EnrichmentCircuitBreaker(),
  runStateRepository,
  deadLetterPublisher: deadLetterQueue,
  finalizer
});
const concurrencyQueue = new Queue(ENRICHMENT_QUEUE_NAME, { connection: queueRedis });
await concurrencyQueue.setGlobalConcurrency(3);
const worker = new Worker<ProcessMessageDto>(ENRICHMENT_QUEUE_NAME, async (job) => {
  await service.process(job.data, job.name === EXPLICIT_REPROCESS_JOB_NAME);
}, {
  connection: redis,
  concurrency: 3
});

worker.on("completed", (job) => {
  logger.info("enrichment_job_completed", { runId: job.data.run_id, seq: job.data.seq, sku: job.data.sku });
});
worker.on("failed", (job, failure) => {
  if (job) {
    logger.error("enrichment_job_failed", { runId: job.data.run_id, seq: job.data.seq, sku: job.data.sku });
  }
  void failure;
});
logger.info("worker_started", { concurrency: 3 });

let closing = false;
const close = async (): Promise<void> => {
  if (closing) {
    return;
  }
  closing = true;
  await worker.close();
  await concurrencyQueue.close();
  await deadLetterQueue.close();
  await redis.quit();
  await queueRedis.quit();
};

process.once("SIGTERM", () => void close().then(() => process.exit(0)));
process.once("SIGINT", () => void close().then(() => process.exit(0)));
