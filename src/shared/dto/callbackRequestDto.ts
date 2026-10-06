export type CallbackResultItem = {
  seq: number;
  sku: string;
  price: number;
  stock: number;
};

export class CallbackRequestDto {
  constructor(
    readonly cid: string,
    readonly run_id: string,
    readonly result: CallbackResultItem[]
  ) {}
}
