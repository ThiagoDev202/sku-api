import type { EnrichmentResponseDto } from "../shared/dto/enrichmentResponseDto.js";

export class EnrichmentResult {
  constructor(
    readonly seq: number,
    readonly sku: string,
    readonly price: number,
    readonly stock: number
  ) {}

  static from(seq: number, value: EnrichmentResponseDto): EnrichmentResult {
    return new EnrichmentResult(seq, value.sku, value.price, value.stock);
  }
}
