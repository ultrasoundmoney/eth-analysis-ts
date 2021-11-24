export const hexToNumber = (hex: string): number => Number.parseInt(hex, 16);

export const numberToHex = (num: number): string => `0x${num.toString(16)}`;
