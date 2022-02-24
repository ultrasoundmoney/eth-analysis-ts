export type Wei = number;
export type WeiBI = bigint;

export type Eth = number;
export type EthBI = bigint;

export const ethFromWeiBI = (wei: bigint): bigint => wei / 10n ** 18n;
export const ethFromWei = (wei: number): number => wei / 1e18;
