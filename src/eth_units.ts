export type Wei = number;
export type WeiBI = bigint;

export type Eth = number;

export const ethFromWei = (wei: number): number => wei / 1e18;
