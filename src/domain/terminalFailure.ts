export class TerminalFailure {
  constructor(
    readonly seq: number,
    readonly sku: string,
    readonly statusCode: number | null,
    readonly errorCode: string,
    readonly attempts: number
  ) {}
}
