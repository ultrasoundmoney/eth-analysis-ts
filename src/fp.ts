import * as Apply from "fp-ts/lib/Apply.js";
import * as E from "fp-ts/lib/Either.js";
import { pipe } from "fp-ts/lib/function.js";
import * as IO from "fp-ts/lib/IO.js";
import * as MapF from "fp-ts/lib/Map.js";
import * as Mo from "fp-ts/lib/Monoid.js";
import * as Num from "fp-ts/lib/number.js";
import * as O from "fp-ts/lib/Option.js";
import * as S from "fp-ts/lib/string.js";
import * as T from "fp-ts/lib/Task.js";
import * as TE from "fp-ts/lib/TaskEither.js";
import * as TO from "fp-ts/lib/TaskOption.js";
import * as Void from "fp-ts/lib/void.js";
import * as Log from "./log.js";

export * as Ap from "fp-ts/lib/Apply.js";
export * as A from "fp-ts/lib/Array.js";
export * as B from "fp-ts/lib/boolean.js";
export * as E from "fp-ts/lib/Either.js";
export { flow, identity, pipe } from "fp-ts/lib/function.js";
export * as IO from "fp-ts/lib/IO.js";
export * as MapF from "fp-ts/lib/Map.js";
export * as Mo from "fp-ts/lib/Monoid.js";
export * as NEA from "fp-ts/lib/NonEmptyArray.js";
export * as Num from "fp-ts/lib/number.js";
export * as O from "fp-ts/lib/Option.js";
export * as Ord from "fp-ts/lib/Ord.js";
export * as Ordering from "fp-ts/lib/Ordering.js";
export * as RTE from "fp-ts/lib/ReaderTaskEither.js";
export * as RA from "fp-ts/lib/ReadonlyArray.js";
export * as RNEA from "fp-ts/lib/ReadonlyNonEmptyArray.js";
export * as Rec from "fp-ts/lib/Record.js";
export * as S from "fp-ts/lib/string.js";
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
  seqSPar: Apply.sequenceS(T.ApplyPar),
  seqSSeq: Apply.sequenceS(T.ApplySeq),
  seqTPar: Apply.sequenceT(T.ApplyPar),
  seqTSeq: Apply.sequenceT(T.ApplySeq),
  when: (shouldExecute: boolean, task: T.Task<void>) =>
    shouldExecute ? task : T.of(undefined as void),
  whenT: (conditionalTask: T.Task<void>) => (ta: T.Task<boolean>) =>
    pipe(
      ta,
      T.chain((shouldRetry) => TAlt.when(shouldRetry, conditionalTask)),
    ),
  debugTap: <A>(message: string) =>
    T.chainFirstIOK<A, void>((value) => () => {
      Log.debug(message, value);
    }),
  debugTapStr: <A>(message: string) =>
    T.chainFirstIOK<A, void>((value) => () => {
      Log.debug(message + String(value));
    }),
  chainFirstLog: <A>(level: Log.Level, format: (a: A) => string) =>
    T.chainFirstIOK<A, void>((value) => Log.logIO(level, format(value))),
  chainFirstLogDebug: <A>(format: (a: A) => string) =>
    TAlt.chainFirstLog("DEBUG", format),
};

export const TEAlt = {
  chainFirstLog: <A>(level: Log.Level, format: (a: A) => string) =>
    TE.chainFirstIOK<A, void>((value) => Log.logIO(level, format(value))),
  chainFirstLogDebug: <A>(format: (a: A) => string) =>
    TEAlt.chainFirstLog("DEBUG", format),
  concatAllVoid: TE.map(Mo.concatAll(Void.Monoid)),
  decodeUnknownError: (e: unknown): Error =>
    e instanceof Error ? e : new Error(String(e)),
  debugTap: <A>(message: string) =>
    TE.chainFirstIOK<A, void>((value) => () => {
      Log.debug(message, value);
    }),
  debugTapStr: <A>(message: string) =>
    TE.chainFirstIOK<A, void>((value) => () => {
      Log.debug(message + String(value));
    }),
  getOrThrow,
  seqSPar: Apply.sequenceS(TE.ApplyPar),
  seqSSeq: Apply.sequenceS(TE.ApplySeq),
  seqTPar: Apply.sequenceT(TE.ApplyPar),
  seqTSeq: Apply.sequenceT(TE.ApplySeq),
  tap: <E, A>(task: TE.TaskEither<E, A>) =>
    pipe(
      task,
      TE.chainFirstIOK((u) => Log.debugIO("tap", u)),
    ),
  tapWithMessage:
    <E, A>(message: string) =>
    (task: TE.TaskEither<E, A>) =>
      pipe(
        task,
        TE.chainFirstIOK((u) => Log.debugIO(message, u)),
      ),
  when: <E>(shouldExecute: boolean, task: TE.TaskEither<E, void>) =>
    shouldExecute ? task : TE.of(undefined as void),
};

export const OAlt = {
  getOrThrow: (message: string) =>
    O.getOrElseW(() => {
      throw new Error(message);
    }),
  seqS: Apply.sequenceS(O.Apply),
  seqT: Apply.sequenceT(O.Apply),
};

export const TOAlt = {
  concatAllVoid: TO.map(Mo.concatAll(Void.Monoid)),
  expect:
    (message: string) =>
    <A>(taskOption: TO.TaskOption<A>) =>
      pipe(
        taskOption,
        TO.getOrElseW<A>(() => {
          throw new Error(message);
        }),
      ),
  seqSPar: Apply.sequenceS(TO.ApplyPar),
  seqSSeq: Apply.sequenceS(TO.ApplySeq),
  seqTPar: Apply.sequenceT(TO.ApplyPar),
  seqTSeq: Apply.sequenceT(TO.ApplySeq),
  doOrSkipVoid: (ta: TO.TaskOption<void>) =>
    pipe(
      ta,
      TO.match(
        (): void => undefined,
        (): void => undefined,
      ),
    ),
};

export const IOAlt = {
  concatAllVoid: IO.map(Mo.concatAll(Void.Monoid)),
};

export const EAlt = {
  getOrThrow: <E, A>(either: E.Either<E, A>) =>
    pipe(
      either,
      E.getOrElse<E, A>((message): never => {
        if (typeof message === "string") {
          throw new Error(message);
        }

        throw new Error(String(message));
      }),
    ),
};

export const MapS = {
  lookup: MapF.lookup(S.Eq),
  upsertAt: MapF.upsertAt(S.Eq),
};

export const MapN = {
  lookup: MapF.lookup(Num.Eq),
};

export const ErrAlt = {
  unknownToError: (e: unknown): Error => {
    if (e instanceof Error) {
      return e;
    }

    return new Error(String(e));
  },
};
