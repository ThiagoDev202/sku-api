import { createApiApp } from "./app.js";
import { ProcessMessageService } from "../application/processMessageService.js";
import { AppConfig } from "../shared/config/appConfig.js";
import { AppLogger } from "../shared/logging/appLogger.js";
import { EnrichmentQueue } from "../infrastructure/queue/enrichmentQueue.js";
import { ProcessMessagePublisher } from "../infrastructure/queue/processMessagePublisher.js";
import { ProcessRecordRepository } from "../infrastructure/redis/processRecordRepository.js";
import { RedisConnection } from "../infrastructure/redis/redisConnection.js";

const config = AppConfig.fromEnvironment();
const redis = new RedisConnection(config.redisUrl).createClient();
const queue = new EnrichmentQueue(redis);
const records = new ProcessRecordRepository(redis);
const publisher = new ProcessMessagePublisher({ queue, records });
const processMessageService = new ProcessMessageService({ publisher });
const logger = new AppLogger();
const app = await createApiApp({ processMessageService, logger });

app.addHook("onClose", async () => {
  await queue.close();
  await redis.quit();
});

await app.listen({ host: "0.0.0.0", port: config.port });
logger.info("api_started", { port: config.port });
