import { Redis, type RedisOptions } from "ioredis";

export class RedisConnection {
  constructor(private readonly redisUrl: string) {}

  createClient(options: Partial<RedisOptions> = {}): Redis {
    return new Redis(this.redisUrl, {
      connectTimeout: 500,
      maxRetriesPerRequest: 1,
      retryStrategy: (attempt) => Math.min(attempt * 100, 1000),
      ...options
    });
  }
}
