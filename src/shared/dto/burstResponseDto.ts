export class BurstResponseDto {
  constructor(
    readonly run_id: string,
    readonly total: number,
    readonly started_at: string
  ) {}

  static from(value: unknown): BurstResponseDto {
    if (!isObject(value) || typeof value.run_id !== "string" || typeof value.total !== "number" ||
      !Number.isInteger(value.total) || value.total < 0 || typeof value.started_at !== "string") {
      throw new Error("Burst response is invalid");
    }
    return new BurstResponseDto(value.run_id, value.total, value.started_at);
  }
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
