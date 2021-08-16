export const weiToEth = (wei: number): number => wei / 10 ** 18;

export const weiToGwei = (wei: number): number => wei / 10 ** 9;

export const weiToEthBI = (wei: bigint): bigint => wei / BigInt(10 ** 18);

export const weiToGweiBI = (wei: bigint): bigint => wei / BigInt(10 ** 9);
