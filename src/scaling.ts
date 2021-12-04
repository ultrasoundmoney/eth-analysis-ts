import { pipe } from "./fp.js";

// TODO: scale as early as possible.
// We use a factor two scaling for USD amounts.
export const usdToScaled = (amount: number): bigint =>
  pipe(amount * 10 ** 2, Math.round, BigInt);

export const scaledToUsd = (amount: bigint): number =>
  pipe(amount / 10n ** 2n, Number);
