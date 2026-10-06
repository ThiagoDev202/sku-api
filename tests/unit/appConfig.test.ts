import { describe, expect, it } from "vitest";
import { AppConfig } from "../../src/shared/config/appConfig.js";

describe("AppConfig environment configuration", () => {
  const requiredEnvironment = {
    PUBLIC_WEBHOOK_URL: "https://webhook.example.test",
    REGISTRATION_NAME: "sku-integration"
  };

  it("requires PLATFORM_BASE_URL instead of using a built-in default", () => {
    expect(() =>
      AppConfig.fromEnvironment({
        ...requiredEnvironment,
        REDIS_URL: "redis://redis.example.test:6379"
      })
    ).toThrow("PLATFORM_BASE_URL is required");
  });

  it("requires REDIS_URL instead of using a built-in default", () => {
    expect(() =>
      AppConfig.fromEnvironment({
        ...requiredEnvironment,
        PLATFORM_BASE_URL: "https://platform.example.test"
      })
    ).toThrow("REDIS_URL is required");
  });

  it("uses the explicitly configured platform and Redis URLs", () => {
    const config = AppConfig.fromEnvironment({
      ...requiredEnvironment,
      PLATFORM_BASE_URL: "https://platform.example.test/api/",
      REDIS_URL: "rediss://redis.example.test:6380/2"
    });

    expect(config.platformBaseUrl).toBe("https://platform.example.test/api");
    expect(config.redisUrl).toBe("rediss://redis.example.test:6380/2");
  });
});
