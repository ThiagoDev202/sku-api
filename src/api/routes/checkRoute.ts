import type { FastifyInstance } from "fastify";
import { CheckRequestDto } from "../../shared/dto/checkRequestDto.js";
import { CheckResponseDto, checkResponseSchema } from "../../shared/dto/checkResponseDto.js";
import { checkRequestSchema } from "../../shared/dto/checkRequestDto.js";

export async function registerCheckRoute(app: FastifyInstance): Promise<void> {
  app.post<{ Body: { token: string } }>("/check", {
    schema: {
      body: checkRequestSchema,
      response: { 200: checkResponseSchema }
    }
  }, async (request) => {
    const body = new CheckRequestDto(request.body.token);
    return new CheckResponseDto(body.token);
  });
}
