import type { FastifySchema } from "fastify";

export class ProcessMessageDto {
  constructor(
    readonly run_id: string,
    readonly seq: number,
    readonly sku: string
  ) {}

  static from(value: { run_id: string; seq: number; sku: string }): ProcessMessageDto {
    return new ProcessMessageDto(value.run_id, value.seq, value.sku);
  }
}

export const processMessageSchema: FastifySchema["body"] = {
  type: "object",
  additionalProperties: false,
  required: ["run_id", "seq", "sku"],
  properties: {
    run_id: { type: "string", minLength: 1 },
    seq: { type: "number" },
    sku: { type: "string", minLength: 1 }
  }
};
