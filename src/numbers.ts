export const hexToNumber = (hex: string) => Number.parseInt(hex, 16);

export const numberToHex = (num: number) => `0x${num.toString(16)}`;

export const weiToEth = (wei: number): number => wei / 10 ** 18;

export const weiToGwei = (wei: number): number => wei / 10 ** 9;

export const weiToEthBI = (wei: bigint): bigint => wei / BigInt(10 ** 18);

export const weiToGweiBI = (wei: bigint): bigint => wei / BigInt(10 ** 9);

export const sum = (nums: readonly number[]) =>
  nums.reduce((sum, num) => sum + num, 0);
