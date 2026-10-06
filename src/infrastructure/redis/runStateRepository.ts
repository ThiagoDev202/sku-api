import { createHash } from "node:crypto";
import type { Redis } from "ioredis";
import type { ProcessMessageDto } from "../../shared/dto/processMessageDto.js";
import type { EnrichmentResult } from "../../domain/enrichmentResult.js";
import type { TerminalFailure } from "../../domain/terminalFailure.js";
import type { BurstResponseDto } from "../../shared/dto/burstResponseDto.js";
import type { CallbackResponseDto } from "../../shared/dto/callbackResponseDto.js";

export type RunFinalizationState =
  | "COLLECTING"
  | "READY"
  | "CALLBACK_ATTEMPTED"
  | "SENT"
  | "UNKNOWN"
  | "BLOCKED_FOR_POLICY";

export type RunSnapshot = {
  run_id: string;
  cid: string | null;
  total: number | null;
  results: Array<{ seq: number; sku: string; price: number; stock: number }>;
  terminalFailures: Array<{ seq: number; reason: string }>;
  finalizationState?: RunFinalizationState;
};

export type StoredCallbackResponse = {
  body: string;
  truncated: boolean;
  capturedAt: string;
};

export type StoredMessageState =
  | { state: "SUCCEEDED" }
  | { state: "FAILED_TERMINAL"; failure: { statusCode: number | null; errorCode: string; attempts: number } }
  | null;

const recordTerminalScript = `
local current = redis.call('HGET', KEYS[2], ARGV[1])
if current then return 0 end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[1], ARGV[3])
redis.call('HSETNX', KEYS[1], 'finalization_state', 'COLLECTING')
redis.call('HINCRBY', KEYS[1], 'terminal_count', 1)
redis.call('HINCRBY', KEYS[1], ARGV[4], 1)
return 1
`;

const reserveFailedSkuReprocessScript = `
local finalizationState = redis.call('HGET', KEYS[1], 'finalization_state')
if finalizationState ~= 'BLOCKED_FOR_POLICY' then return false end
if redis.call('HGET', KEYS[2], ARGV[1]) ~= 'FAILED_TERMINAL' then return false end
if not redis.call('HGET', KEYS[3], ARGV[1]) then return false end
local activeField = 'reprocess_active:' .. ARGV[1]
local activeTicket = redis.call('HGET', KEYS[1], activeField)
if activeTicket then return activeTicket end
local generationField = 'reprocess_generation:' .. ARGV[1]
local ticket = tostring(redis.call('HINCRBY', KEYS[1], generationField, 1))
redis.call('HSET', KEYS[1], activeField, ticket)
return ticket
`;

const recordExplicitReprocessSuccessScript = `
local sequence = ARGV[1]
local activeField = 'reprocess_active:' .. sequence
if redis.call('HGET', KEYS[2], sequence) ~= 'FAILED_TERMINAL' then return 0 end
if not redis.call('HGET', KEYS[1], activeField) then return 0 end
local previousFailure = redis.call('HGET', KEYS[4], sequence)
if previousFailure then redis.call('RPUSH', KEYS[5], previousFailure) end
redis.call('HSET', KEYS[2], sequence, 'SUCCEEDED')
redis.call('HSET', KEYS[3], sequence, ARGV[2])
redis.call('HDEL', KEYS[4], sequence)
redis.call('HDEL', KEYS[1], activeField)
redis.call('HINCRBY', KEYS[1], 'failure_count', -1)
redis.call('HINCRBY', KEYS[1], 'success_count', 1)
if redis.call('HGET', KEYS[1], 'finalization_state') == 'BLOCKED_FOR_POLICY' then
  redis.call('HSET', KEYS[1], 'finalization_state', 'COLLECTING')
end
return 1
`;

const recordExplicitReprocessFailureScript = `
local sequence = ARGV[1]
local activeField = 'reprocess_active:' .. sequence
if redis.call('HGET', KEYS[2], sequence) ~= 'FAILED_TERMINAL' then return 0 end
if not redis.call('HGET', KEYS[1], activeField) then return 0 end
local previousFailure = redis.call('HGET', KEYS[3], sequence)
if previousFailure then redis.call('RPUSH', KEYS[4], previousFailure) end
redis.call('HSET', KEYS[3], sequence, ARGV[2])
redis.call('HDEL', KEYS[1], activeField)
return 1
`;

const saveMetadataScript = `
local existingCid = redis.call('HGET', KEYS[1], 'cid')
local existingTotal = redis.call('HGET', KEYS[1], 'total')
if existingCid and existingCid ~= ARGV[2] then return -1 end
if existingTotal and existingTotal ~= ARGV[3] then return -1 end
redis.call('HSET', KEYS[1], 'run_id', ARGV[1], 'cid', ARGV[2], 'total', ARGV[3], 'started_at', ARGV[4])
redis.call('HSETNX', KEYS[1], 'finalization_state', 'COLLECTING')
return 1
`;

const markFinalizationScript = `
local current = redis.call('HGET', KEYS[1], 'finalization_state')
if current == 'SENT' or current == 'UNKNOWN' or current == 'BLOCKED_FOR_POLICY' then return 0 end
if current == 'CALLBACK_ATTEMPTED' and ARGV[1] ~= 'SENT' and ARGV[1] ~= 'UNKNOWN' then return 0 end
redis.call('HSET', KEYS[1], 'finalization_state', ARGV[1])
return 1
`;

const reserveCallbackScript = `
local current = redis.call('HGET', KEYS[1], 'finalization_state')
if current == 'CALLBACK_ATTEMPTED' or current == 'SENT' or current == 'UNKNOWN' or current == 'BLOCKED_FOR_POLICY' then return 0 end
local total = tonumber(redis.call('HGET', KEYS[1], 'total') or '')
local terminal = tonumber(redis.call('HGET', KEYS[1], 'terminal_count') or '0')
local failures = tonumber(redis.call('HGET', KEYS[1], 'failure_count') or '0')
if not total or terminal ~= total or failures > 0 then return 0 end
redis.call('HSET', KEYS[1], 'finalization_state', 'CALLBACK_ATTEMPTED')
return 1
`;

export class RunStateRepository {
  constructor(private readonly redis: Redis) {}

  async recordMetadata(burst: BurstResponseDto, cid: string): Promise<void> {
    const result = await this.redis.eval(
      saveMetadataScript,
      1,
      this.stateKey(burst.run_id),
      burst.run_id,
      cid,
      String(burst.total),
      burst.started_at
    );
    if (Number(result) < 0) {
      throw new Error("Run metadata conflicts with previously stored metadata");
    }
  }

  async recordSuccess(message: ProcessMessageDto, result: EnrichmentResult): Promise<boolean> {
    return Number(await this.redis.eval(
      recordTerminalScript,
      3,
      this.stateKey(message.run_id),
      this.statusKey(message.run_id),
      this.resultsKey(message.run_id),
      String(message.seq),
      "SUCCEEDED",
      JSON.stringify(result),
      "success_count"
    )) === 1;
  }

  async recordTerminalFailure(message: ProcessMessageDto, failure: TerminalFailure): Promise<boolean> {
    const storedFailure = {
      seq: failure.seq,
      sku: failure.sku,
      reason: failure.errorCode,
      errorCode: failure.errorCode,
      statusCode: failure.statusCode,
      attempts: failure.attempts
    };
    return Number(await this.redis.eval(
      recordTerminalScript,
      3,
      this.stateKey(message.run_id),
      this.statusKey(message.run_id),
      this.failuresKey(message.run_id),
      String(message.seq),
      "FAILED_TERMINAL",
      JSON.stringify(storedFailure),
      "failure_count"
    )) === 1;
  }

  async getOriginalFailedMessage(runId: string, seq: number): Promise<ProcessMessageDto | null> {
    const serializedFailure = await this.redis.hget(this.failuresKey(runId), String(seq));
    if (!serializedFailure) {
      return null;
    }
    const parsed: unknown = JSON.parse(serializedFailure);
    if (!isObject(parsed) || parsed.seq !== seq || typeof parsed.sku !== "string") {
      return null;
    }
    return { run_id: runId, seq, sku: parsed.sku };
  }

  async reserveFailedSkuReprocess(runId: string, seq: number): Promise<string | null> {
    const result = await this.redis.eval(
      reserveFailedSkuReprocessScript,
      3,
      this.stateKey(runId),
      this.statusKey(runId),
      this.failuresKey(runId),
      String(seq)
    );
    return typeof result === "string" || typeof result === "number" ? String(result) : null;
  }

  async recordExplicitReprocessSuccess(message: ProcessMessageDto, result: EnrichmentResult): Promise<boolean> {
    return Number(await this.redis.eval(
      recordExplicitReprocessSuccessScript,
      5,
      this.stateKey(message.run_id),
      this.statusKey(message.run_id),
      this.resultsKey(message.run_id),
      this.failuresKey(message.run_id),
      this.failureHistoryKey(message.run_id),
      String(message.seq),
      JSON.stringify(result)
    )) === 1;
  }

  async recordExplicitReprocessFailure(message: ProcessMessageDto, failure: TerminalFailure): Promise<boolean> {
    const storedFailure = {
      seq: failure.seq,
      sku: failure.sku,
      reason: failure.errorCode,
      errorCode: failure.errorCode,
      statusCode: failure.statusCode,
      attempts: failure.attempts
    };
    return Number(await this.redis.eval(
      recordExplicitReprocessFailureScript,
      4,
      this.stateKey(message.run_id),
      this.statusKey(message.run_id),
      this.failuresKey(message.run_id),
      this.failureHistoryKey(message.run_id),
      String(message.seq),
      JSON.stringify(storedFailure)
    )) === 1;
  }

  async getFailureHistory(runId: string): Promise<Array<{ seq: number; sku: string; reason: string }>> {
    const values = await this.redis.lrange(this.failureHistoryKey(runId), 0, -1);
    return values.map((value) => parseFailureHistory(value));
  }

  async getMessageState(message: ProcessMessageDto): Promise<StoredMessageState> {
    const field = String(message.seq);
    const state = await this.redis.hget(this.statusKey(message.run_id), field);
    if (state === "SUCCEEDED") {
      return { state: "SUCCEEDED" };
    }
    if (state !== "FAILED_TERMINAL") {
      return null;
    }
    const serializedFailure = await this.redis.hget(this.failuresKey(message.run_id), field);
    if (!serializedFailure) {
      throw new Error("Stored terminal failure is missing");
    }
    const parsed: unknown = JSON.parse(serializedFailure);
    if (!isObject(parsed) || typeof parsed.errorCode !== "string" || typeof parsed.attempts !== "number") {
      throw new Error("Stored terminal failure is invalid");
    }
    return {
      state: "FAILED_TERMINAL",
      failure: {
        statusCode: typeof parsed.statusCode === "number" ? parsed.statusCode : null,
        errorCode: parsed.errorCode,
        attempts: parsed.attempts
      }
    };
  }

  async getSnapshot(runId: string): Promise<RunSnapshot> {
    const stateKey = this.stateKey(runId);
    const [state, rawResults, rawFailures] = await Promise.all([
      this.redis.hgetall(stateKey),
      this.redis.hgetall(this.resultsKey(runId)),
      this.redis.hgetall(this.failuresKey(runId))
    ]);
    return {
      run_id: runId,
      cid: state.cid ?? null,
      total: state.total === undefined ? null : Number(state.total),
      results: Object.values(rawResults).map((value) => parseSuccess(value)),
      terminalFailures: Object.values(rawFailures).map((value) => parseFailure(value)),
      finalizationState: parseFinalizationState(state.finalization_state)
    };
  }

  async reserveCallback(runId: string): Promise<boolean> {
    return Number(await this.redis.eval(reserveCallbackScript, 1, this.stateKey(runId))) === 1;
  }

  async markFinalizationState(runId: string, state: RunFinalizationState): Promise<void> {
    await this.redis.eval(markFinalizationScript, 1, this.stateKey(runId), state);
  }

  async recordCallbackResponse(runId: string, response: CallbackResponseDto): Promise<void> {
    const capturedAt = new Date().toISOString();
    await this.redis.hset(
      this.stateKey(runId),
      "callback_response_body", response.body,
      "callback_response_truncated", String(response.truncated),
      "callback_response_captured_at", capturedAt
    );
  }

  async getCallbackResponse(runId: string): Promise<StoredCallbackResponse | null> {
    const [body, truncated, capturedAt] = await Promise.all([
      this.redis.hget(this.stateKey(runId), "callback_response_body"),
      this.redis.hget(this.stateKey(runId), "callback_response_truncated"),
      this.redis.hget(this.stateKey(runId), "callback_response_captured_at")
    ]);
    if (body === null || truncated === null || capturedAt === null) {
      return null;
    }
    return { body, truncated: truncated === "true", capturedAt };
  }

  private stateKey(runId: string): string {
    return `run:${this.idFor(runId)}:state`;
  }

  private statusKey(runId: string): string {
    return `run:${this.idFor(runId)}:status`;
  }

  private resultsKey(runId: string): string {
    return `run:${this.idFor(runId)}:results`;
  }

  private failuresKey(runId: string): string {
    return `run:${this.idFor(runId)}:failures`;
  }

  private failureHistoryKey(runId: string): string {
    return `run:${this.idFor(runId)}:failure-history`;
  }

  private idFor(runId: string): string {
    return createHash("sha256").update(runId).digest("hex");
  }
}

function parseSuccess(value: string): { seq: number; sku: string; price: number; stock: number } {
  const parsed: unknown = JSON.parse(value);
  if (!isObject(parsed) || typeof parsed.seq !== "number" || typeof parsed.sku !== "string" ||
    typeof parsed.price !== "number" || typeof parsed.stock !== "number") {
    throw new Error("Stored enrichment result is invalid");
  }
  return { seq: parsed.seq, sku: parsed.sku, price: parsed.price, stock: parsed.stock };
}

function parseFailure(value: string): { seq: number; reason: string } {
  const parsed: unknown = JSON.parse(value);
  if (!isObject(parsed) || typeof parsed.seq !== "number" || typeof parsed.reason !== "string") {
    throw new Error("Stored terminal failure is invalid");
  }
  return { seq: parsed.seq, reason: parsed.reason };
}

function parseFailureHistory(value: string): { seq: number; sku: string; reason: string } {
  const parsed: unknown = JSON.parse(value);
  if (!isObject(parsed) || typeof parsed.seq !== "number" || typeof parsed.sku !== "string" || typeof parsed.reason !== "string") {
    throw new Error("Stored terminal failure history is invalid");
  }
  return { seq: parsed.seq, sku: parsed.sku, reason: parsed.reason };
}

function parseFinalizationState(value: string | undefined): RunFinalizationState {
  if (value === "READY" || value === "CALLBACK_ATTEMPTED" || value === "SENT" || value === "UNKNOWN" || value === "BLOCKED_FOR_POLICY") {
    return value;
  }
  return "COLLECTING";
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
