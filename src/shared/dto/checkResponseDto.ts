export class CheckResponseDto {
  constructor(readonly token: string) {}
}

export const checkResponseSchema = {
  type: "object",
  additionalProperties: false,
  required: ["token"],
  properties: {
    token: { type: "string" }
  }
} as const;
