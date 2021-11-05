import * as T from "fp-ts/lib/Task.js";
import * as TE from "fp-ts/lib/TaskEither.js";
import { sequenceS, sequenceT } from "fp-ts/lib/Apply.js";

export * as A from "fp-ts/lib/Array.js";
export * as B from "fp-ts/lib/boolean.js";
export * as E from "fp-ts/lib/Either.js";
export * as D from "io-ts/lib/Decoder.js";
export * as NEA from "fp-ts/lib/NonEmptyArray.js";
export * as Num from "fp-ts/lib/number.js";
export * as O from "fp-ts/lib/Option.js";
export * as RA from "fp-ts/lib/ReadonlyArray.js";
export * as RTE from "fp-ts/lib/ReaderTaskEither.js";
export * as T from "fp-ts/lib/Task.js";
export * as TE from "fp-ts/lib/TaskEither.js";
export { Ord } from "fp-ts/lib/Ord.js";
export { flow, pipe } from "fp-ts/lib/function.js";

export const seqTParT = sequenceT(T.ApplyPar);
export const seqTSeqT = sequenceT(T.ApplySeq);
export const seqSParT = sequenceS(T.ApplyPar);
export const seqSSeqT = sequenceS(T.ApplySeq);

export const seqTParTE = sequenceT(TE.ApplyPar);
export const seqTSeqTE = sequenceT(TE.ApplySeq);
export const seqSParTE = sequenceS(TE.ApplyPar);
export const seqSSeqTE = sequenceS(TE.ApplySeq);
