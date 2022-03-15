import { pipe } from "./fp.js";

export const ethFromWei = (wei: bigint | number): string =>
  pipe(Number(wei), (num) => num / 1e18, String);
