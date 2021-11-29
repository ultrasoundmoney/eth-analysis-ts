import { sequenceS, sequenceT } from "fp-ts/lib/Apply.js";
import { pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";
import * as TE from "fp-ts/lib/TaskEither.js";

export * as A from "fp-ts/lib/Array.js";
export * as B from "fp-ts/lib/boolean.js";
export * as D from "io-ts/lib/Decoder.js";
export * as E from "fp-ts/lib/Either.js";
export * as NEA from "fp-ts/lib/NonEmptyArray.js";
export * as Num from "fp-ts/lib/number.js";
export * as O from "fp-ts/lib/Option.js";
export * as RA from "fp-ts/lib/ReadonlyArray.js";
export * as ROA from "fp-ts/lib/ReadonlyArray.js";
export * as RTE from "fp-ts/lib/ReaderTaskEither.js";
export * as T from "fp-ts/lib/Task.js";
export * as TE from "fp-ts/lib/TaskEither.js";
export { Ord } from "fp-ts/lib/Ord.js";
export { flow, pipe } from "fp-ts/lib/function.js";

const getOrThrow = <A>(te: TE.TaskEither<string | undefined, A>): T.Task<A> =>
  pipe(
    te,
    TE.getOrElse((e) => {
      throw new Error(e);
    }),
  );

export const TAlt = {
  seqTParT: sequenceT(T.ApplyPar),
  seqTSeqT: sequenceT(T.ApplySeq),
  seqSParT: sequenceS(T.ApplyPar),
  seqSSeqT: sequenceS(T.ApplySeq),
};

export const TEAlt = {
  seqTParTE: sequenceT(TE.ApplyPar),
  seqTSeqTE: sequenceT(TE.ApplySeq),
  seqSParTE: sequenceS(TE.ApplyPar),
  seqSSeqTE: sequenceS(TE.ApplySeq),
  getOrThrow,
};
