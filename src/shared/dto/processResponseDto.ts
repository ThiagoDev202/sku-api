export class ProcessResponseDto {
  constructor(readonly ok: true = true) {}
}

export const processResponseSchema = {
  type: "object",
  additionalProperties: false,
  required: ["ok"],
  properties: {
    ok: { type: "boolean", const: true }
  }
} as const;
