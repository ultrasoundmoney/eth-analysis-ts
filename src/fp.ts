import * as Apply from "fp-ts/lib/Apply.js";
import { pipe } from "fp-ts/lib/function.js";
import * as Mo from "fp-ts/lib/Monoid.js";
import * as O from "fp-ts/lib/Option.js";
import * as T from "fp-ts/lib/Task.js";
import * as TE from "fp-ts/lib/TaskEither.js";
import * as Void from "fp-ts/lib/void.js";
import * as Log from "./log.js";

export * as Ap from "fp-ts/lib/Apply.js";
export * as A from "fp-ts/lib/Array.js";
export * as B from "fp-ts/lib/boolean.js";
export * as E from "fp-ts/lib/Either.js";
export { flow, pipe } from "fp-ts/lib/function.js";
export * as IO from "fp-ts/lib/IO.js";
export * as Mo from "fp-ts/lib/Monoid.js";
export * as NEA from "fp-ts/lib/NonEmptyArray.js";
export * as Num from "fp-ts/lib/number.js";
export * as O from "fp-ts/lib/Option.js";
export * as Ord from "fp-ts/lib/Ord.js";
export * as RTE from "fp-ts/lib/ReaderTaskEither.js";
export * as RA from "fp-ts/lib/ReadonlyArray.js";
export * as T from "fp-ts/lib/Task.js";
export * as TE from "fp-ts/lib/TaskEither.js";
export * as TO from "fp-ts/lib/TaskOption.js";
export * as Void from "fp-ts/lib/void.js";
export * as D from "io-ts/lib/Decoder.js";

type ErrorLike = { error: Error };

const getOrThrow = <A>(
  te: TE.TaskEither<string | Error | ErrorLike, A>,
): T.Task<A> =>
  pipe(
    te,
    TE.getOrElse((e): never => {
      if (e instanceof Error) {
        throw e;
      }

      if (typeof e === "string") {
        throw new Error(e);
      }

      if ("error" in e && e.error instanceof Error) {
        throw e.error;
      }

      throw new Error("getOrThrow, failed to throw error");
    }),
  );

export const TAlt = {
  concatAllVoid: T.map(Mo.concatAll(Void.Monoid)),
  constVoid: () => T.of(undefined),
  seqSParT: Apply.sequenceS(T.ApplyPar),
  seqSSeqT: Apply.sequenceS(T.ApplySeq),
  seqTParT: Apply.sequenceT(T.ApplyPar),
  seqTSeqT: Apply.sequenceT(T.ApplySeq),
  logDebugStr: <A>(msg: string) =>
    T.chainFirstIOK<A, void>((value) => () => {
      Log.debug(msg + String(value));
    }),
  logDebug: <A>(msg: string) =>
    T.chainFirstIOK<A, void>((value) => () => {
      Log.debug(msg, value);
    }),
};

export const TEAlt = {
  concatAllVoid: TE.map(Mo.concatAll(Void.Monoid)),
  errorFromUnknown: (e: unknown): Error =>
    e instanceof Error ? e : new Error(String(e)),
  getOrThrow,
  seqSParTE: Apply.sequenceS(TE.ApplyPar),
  seqSSeqTE: Apply.sequenceS(TE.ApplySeq),
  seqTParTE: Apply.sequenceT(TE.ApplyPar),
  seqTSeqTE: Apply.sequenceT(TE.ApplySeq),
};

export const OAlt = {
  getOrThrow: (message: string) =>
    O.getOrElseW(() => {
      throw new Error(message);
    }),
};
