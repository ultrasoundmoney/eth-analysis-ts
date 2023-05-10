import { pipe } from "./fp.js";

export const ethFromWei = (wei: bigint | number): string =>
  pipe(
    Number(wei),
    (num) => num / 1e18,
    (num) => num.toFixed(2),
  );

export const ethFromGwei = (gwei: bigint | number): string =>
  pipe(
    Number(gwei),
    (num) => num / 1e9,
    (num) => num.toFixed(2),
  );
