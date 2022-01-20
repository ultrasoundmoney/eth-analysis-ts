export const numberFromHex = (hex: string): number => Number.parseInt(hex, 16);

export const hexFromNumber = (num: number): string => `0x${num.toString(16)}`;
