import { D, E, pipe } from "./fp.js";

export const decodeEmptyString = pipe(
  D.string,
  D.parse((s) => (s === "" ? D.success(null) : D.success(s))),
);

export class DecodeError extends Error {}

export const decodeWithError =
  <A>(decoder: D.Decoder<unknown, A>) =>
  (u: unknown) =>
    pipe(
      decoder.decode(u),
      E.mapLeft((e) => new DecodeError(`failed to decode\n${D.draw(e)}`)),
    );
