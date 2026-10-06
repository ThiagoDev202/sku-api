export class AppConfig {
  readonly port: number;
  readonly platformBaseUrl: string;
  readonly publicWebhookUrl: string;
  readonly registrationName: string;
  readonly redisUrl: string;

  constructor(values: {
    port: number;
    platformBaseUrl: string;
    publicWebhookUrl: string;
    registrationName: string;
    redisUrl: string;
  }) {
    this.port = values.port;
    this.platformBaseUrl = values.platformBaseUrl;
    this.publicWebhookUrl = values.publicWebhookUrl;
    this.registrationName = values.registrationName;
    this.redisUrl = values.redisUrl;
  }

  static fromEnvironment(environment: NodeJS.ProcessEnv = process.env): AppConfig {
    return new AppConfig({
      port: parsePort(environment.PORT),
      platformBaseUrl: parseHttpsUrl(
        requiredValue(environment.PLATFORM_BASE_URL, "PLATFORM_BASE_URL"),
        "PLATFORM_BASE_URL"
      ),
      publicWebhookUrl: parseHttpsUrl(requiredValue(environment.PUBLIC_WEBHOOK_URL, "PUBLIC_WEBHOOK_URL"), "PUBLIC_WEBHOOK_URL"),
      registrationName: requiredValue(environment.REGISTRATION_NAME, "REGISTRATION_NAME"),
      redisUrl: parseRedisUrl(requiredValue(environment.REDIS_URL, "REDIS_URL"))
    });
  }
}

function requiredValue(value: string | undefined, name: string): string {
  const normalizedValue = value?.trim();
  if (!normalizedValue) {
    throw new Error(`${name} is required`);
  }
  return normalizedValue;
}

function parsePort(value: string | undefined): number {
  if (value === undefined || value.trim() === "") {
    return 4000;
  }
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PORT must be an integer between 1 and 65535");
  }
  return port;
}

function parseHttpsUrl(value: string, name: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${name} must be a valid HTTPS URL`);
  }
  if (url.protocol !== "https:" || url.username || url.password) {
    throw new Error(`${name} must be a valid HTTPS URL`);
  }
  return url.toString().replace(/\/$/, "");
}

function parseRedisUrl(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("REDIS_URL must be a valid redis URL");
  }
  if ((url.protocol !== "redis:" && url.protocol !== "rediss:") || url.username || url.password) {
    throw new Error("REDIS_URL must be a valid redis URL without embedded credentials");
  }
  return url.toString();
}
