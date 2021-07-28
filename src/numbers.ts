export const hexToNumber = (hex: string) => Number.parseInt(hex, 16);

export const numberToHex = (num: number) => `0x${num.toString(16)}`;

export const weiToEth = (wei: number): number => wei / 10 ** 18;

export const sum = (nums: number[]) => nums.reduce((sum, num) => sum + num, 0);
