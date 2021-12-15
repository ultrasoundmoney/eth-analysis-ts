import { pipe } from "./fp.js";

export type Usd = number;
export type UsdScaled = bigint;

// We use a factor two scaling for USD amounts.
const scalingFactor = 2;

export const usdToScaled = (amount: number): bigint =>
  pipe(amount * 10 ** scalingFactor, Math.round, BigInt);

export const scaledToUsd = (amount: bigint): number =>
  pipe(amount / 10n ** 2n, Number);
