export type Wei = number;
export type WeiBI = bigint;

export type Eth = number;

export const ethFromWei = (wei: number): number => wei / 1e18;

export const weiFromEth = (eth: number): number => eth * 1e18;

export const ethFromGwei = (gwei: number): number => gwei / 1e9;

export const gweiFromEth = (eth: number): number => eth * 1e9;

export const gweiFromWei = (wei: number): number => wei / 1e9;

export const weiFromGwei = (gwei: number): number => gwei * 1e9;
