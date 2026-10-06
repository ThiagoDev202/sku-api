const MAX_CALLBACK_RESPONSE_BYTES = 65536;

export class CallbackResponseDto {
  constructor(readonly body: string, readonly truncated: boolean) {}

  static fromText(body: string): CallbackResponseDto {
    const buffer = Buffer.from(body, "utf8");
    if (buffer.byteLength <= MAX_CALLBACK_RESPONSE_BYTES) {
      return new CallbackResponseDto(body, false);
    }
    return new CallbackResponseDto(buffer.subarray(0, MAX_CALLBACK_RESPONSE_BYTES).toString("utf8"), true);
  }
}
