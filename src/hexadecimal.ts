export const hexToNumber = (hex: string) => Number.parseInt(hex, 16);

export const numberToHex = (num: number) => `0x${num.toString(16)}`;
