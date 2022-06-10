import { camelCase } from "change-case";
import { A, pipe } from "./fp.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const camelCaseKeys = <A = any>(obj: { [k: string]: A }) =>
  pipe(
    Object.entries(obj),
    A.map(([key, value]) => [camelCase(key), value] as [PropertyKey, A]),
    (entries) => Object.fromEntries(entries),
  );
