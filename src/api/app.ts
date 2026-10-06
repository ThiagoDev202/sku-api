import Fastify, { type FastifyInstance } from "fastify";
import type { ProcessMessageService } from "../application/processMessageService.js";
import { AppLogger } from "../shared/logging/appLogger.js";
import { registerCheckRoute } from "./routes/checkRoute.js";
import { registerProcessRoute } from "./routes/processRoute.js";

export type ApiDependencies = {
  processMessageService: ProcessMessageService;
  logger?: AppLogger;
};

export async function createApiApp(dependencies: ApiDependencies): Promise<FastifyInstance> {
  const app = Fastify({
    logger: false,
    ajv: {
      customOptions: {
        removeAdditional: false,
        coerceTypes: false,
        useDefaults: false
      }
    }
  });
  const logger = dependencies.logger ?? new AppLogger();

  await registerCheckRoute(app);
  await registerProcessRoute(app, { service: dependencies.processMessageService, logger });

  return app;
}
