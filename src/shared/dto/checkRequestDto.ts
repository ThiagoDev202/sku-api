import type { FastifySchema } from "fastify";

export class CheckRequestDto {
  constructor(readonly token: string) {}
}

export const checkRequestSchema: FastifySchema["body"] = {
  type: "object",
  additionalProperties: false,
  required: ["token"],
  properties: {
    token: { type: "string", minLength: 1 }
  }
};
