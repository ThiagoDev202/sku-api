import type { FastifyInstance } from "fastify";
import type { ProcessMessageService } from "../../application/processMessageService.js";
import type { AppLogger } from "../../shared/logging/appLogger.js";
import { ProcessMessageDto, processMessageSchema } from "../../shared/dto/processMessageDto.js";
import { ProcessResponseDto, processResponseSchema } from "../../shared/dto/processResponseDto.js";

export async function registerProcessRoute(
  app: FastifyInstance,
  dependencies: { service: ProcessMessageService; logger: AppLogger }
): Promise<void> {
  app.post<{ Body: { run_id: string; seq: number; sku: string } }>("/process", {
    schema: {
      body: processMessageSchema,
      response: { 200: processResponseSchema }
    }
  }, async (request, reply) => {
    const message = ProcessMessageDto.from(request.body);
    try {
      await dependencies.service.accept(message);
      return new ProcessResponseDto();
    } catch {
      dependencies.logger.error("process_message_enqueue_failed", {
        runId: message.run_id,
        seq: message.seq,
        sku: message.sku
      });
      return reply.code(503).send({ error: "service_unavailable" });
    }
  });
}
