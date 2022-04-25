import { D, pipe } from "./fp.js";

export const decodeEmptyString = pipe(
  D.string,
  D.parse((s) => (s === "" ? D.success(null) : D.success(s))),
);
