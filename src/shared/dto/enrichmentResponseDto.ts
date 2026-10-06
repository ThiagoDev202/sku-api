export class EnrichmentResponseDto {
  constructor(
    readonly sku: string,
    readonly price: number,
    readonly stock: number
  ) {}

  static from(value: unknown, expectedSku: string): EnrichmentResponseDto {
    if (!isObject(value) || typeof value.sku !== "string" || value.sku !== expectedSku) {
      throw new Error("Enrichment response SKU is invalid");
    }
    if (typeof value.price !== "number" || !Number.isFinite(value.price)) {
      throw new Error("Enrichment response price is invalid");
    }
    if (typeof value.stock !== "number" || !Number.isFinite(value.stock)) {
      throw new Error("Enrichment response stock is invalid");
    }
    return new EnrichmentResponseDto(value.sku, value.price, value.stock);
  }
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
