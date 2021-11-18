import * as DateFns from "date-fns";
import { flow } from "./fp.js";

export type JsTimestamp = number;

// For a timeline |----B ------- A---->
// This function takes two arguments, A, and B, and measures the number of seconds from B, to A. Answer is negative when A comes before B in time.
// Example:
// 2 July 2014 12:30:07.999 and 2 July 2014 12:30:20.000?
// const result = differenceInSeconds(
//   new Date(2014, 6, 2, 12, 30, 20, 0),
//   new Date(2014, 6, 2, 12, 30, 7, 999)
// )
// => 12
export const secondsBetweenInverse = DateFns.differenceInSeconds;

export const secondsBetween = (
  from: Date | number,
  to: Date | number,
): number => secondsBetweenInverse(to, from);

export const secondsBetweenAbs = flow(secondsBetween, Math.abs);

export const millisecondsBetween = (
  from: Date | number,
  to: Date | number,
): number => DateFns.differenceInMilliseconds(to, from);

export const millisecondsBetweenAbs = flow(millisecondsBetween, Math.abs);
