import { afterEach, describe, expect, it, vi } from "vitest";
import type { FastifyInstance } from "fastify";
import { ProcessMessageService } from "../../src/application/processMessageService.js";
import { createApiApp } from "../../src/api/app.js";

describe("Fastify inbound API", () => {
  let app: FastifyInstance | undefined;

  afterEach(async () => {
    await app?.close();
    app = undefined;
  });

  it("echoes the handshake token and accepts a process message after enqueue", async () => {
    const enqueue = vi.fn(async () => undefined);
    app = await createApiApp({ processMessageService: new ProcessMessageService({ publisher: { enqueue } }) });

    const check = await app.inject({ method: "POST", url: "/check", payload: { token: "handshake-token" } });
    const process = await app.inject({
      method: "POST",
      url: "/process",
      payload: { run_id: "run-api", seq: 3, sku: "sku-003" }
    });

    expect(check.statusCode).toBe(200);
    expect(check.json()).toEqual({ token: "handshake-token" });
    expect(process.statusCode).toBe(200);
    expect(process.json()).toEqual({ ok: true });
    expect(enqueue).toHaveBeenCalledWith({ run_id: "run-api", seq: 3, sku: "sku-003" });
  });

  it("rejects unknown fields and invalid process DTOs without enqueueing", async () => {
    const enqueue = vi.fn(async () => undefined);
    app = await createApiApp({ processMessageService: new ProcessMessageService({ publisher: { enqueue } }) });

    const invalidHandshake = await app.inject({
      method: "POST",
      url: "/check",
      payload: { token: "handshake-token", extra: true }
    });
    const invalidProcess = await app.inject({
      method: "POST",
      url: "/process",
      payload: { run_id: "run-api", seq: "3", sku: "sku-003" }
    });
    const extraProcessField = await app.inject({
      method: "POST",
      url: "/process",
      payload: { run_id: "run-api", seq: 3, sku: "sku-003", token: "private" }
    });

    expect(invalidHandshake.statusCode).toBe(400);
    expect(invalidProcess.statusCode).toBe(400);
    expect(extraProcessField.statusCode).toBe(400);
    expect(enqueue).not.toHaveBeenCalled();
  });

  it("returns a non-2xx response when the queue rejects the message", async () => {
    const enqueue = vi.fn().mockRejectedValue(new Error("queue unavailable"));
    app = await createApiApp({ processMessageService: new ProcessMessageService({ publisher: { enqueue } }) });

    const response = await app.inject({
      method: "POST",
      url: "/process",
      payload: { run_id: "run-api", seq: 3, sku: "sku-003" }
    });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({ error: "service_unavailable" });
  });
});
