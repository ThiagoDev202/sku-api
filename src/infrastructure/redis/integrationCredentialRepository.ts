import type { Redis } from "ioredis";
import { RegisterResponseDto } from "../../shared/dto/registerResponseDto.js";

export class IntegrationCredentialRepository {
  constructor(private readonly redis: Redis) {}

  async save(credentials: RegisterResponseDto): Promise<void> {
    await this.redis.set("integration:credentials", JSON.stringify(credentials));
  }

  async get(): Promise<RegisterResponseDto> {
    const value = await this.redis.get("integration:credentials");
    if (!value) {
      throw new Error("Integration credentials are not registered");
    }
    const parsed: unknown = JSON.parse(value);
    if (!isCredentials(parsed)) {
      throw new Error("Integration credentials are invalid");
    }
    return new RegisterResponseDto(parsed.cid, parsed.token);
  }
}

function isCredentials(value: unknown): value is { cid: string; token: string } {
  return typeof value === "object" && value !== null &&
    "cid" in value && typeof value.cid === "string" &&
    "token" in value && typeof value.token === "string";
}
