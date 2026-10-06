import type { ProcessMessageDto } from "../shared/dto/processMessageDto.js";

export type ProcessRecordState = "QUEUED" | "PROCESSING" | "SUCCEEDED" | "FAILED_TERMINAL";

export class ProcessRecord {
  constructor(
    readonly message: ProcessMessageDto,
    readonly state: ProcessRecordState
  ) {}
}
