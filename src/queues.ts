import PQueue from "p-queue";
import { E, pipe, T, TE } from "./fp.js";

export class TimeoutError extends Error {}

const getIsPQueueTimeoutError = (u: unknown): boolean =>
  typeof (u as { name: string })?.name === "string" &&
  (u as { name: string }).name === "TimeoutError";

export const queueOnQueueWithTimeoutUndefined =
  <E, A>(queue: PQueue) =>
  (task: TE.TaskEither<E, A>) =>
    pipe(
      () => queue.add<E.Either<E, A> | undefined>(task as never),
      // It is no longer clear here if our task timed out or if the task succeeded and returned undefined. To protect the caller we fail with an error.
      T.map(
        (result): E.Either<E | TimeoutError, A> =>
          result === undefined ? E.left(new TimeoutError()) : result,
      ),
    );

export const queueOnQueueWithTimeoutThrown =
  <E, A>(queue: PQueue) =>
  (task: TE.TaskEither<E, A>) =>
    pipe(
      TE.tryCatch(
        () => queue.add<E.Either<E, A>>(task as never),
        (error) =>
          getIsPQueueTimeoutError(error) ? new TimeoutError() : (error as E),
      ),
      // tryCatch expects a simple () => Promise<A> but we're passing a () => Promise<Either<E, A>>, tryCatch then wraps the return value in a TaskEither for us creating a TaskEither<E1, Either<E2, A>>, we want TaskEither<E1 | E2, A>, the below fn achieves just that.
      TE.chainEitherKW((either) =>
        either === undefined
          ? E.left<E | TimeoutError>(new TimeoutError())
          : either,
      ),
    );

export const queueOnQueue =
  <E, A>(queue: PQueue) =>
  (task: TE.TaskEither<E, A>) =>
  () =>
    queue.add<E.Either<E, A>>(task as never);

export const queueOnQueueT =
  <A>(queue: PQueue) =>
  (task: T.Task<A>) =>
  () =>
    queue.add<A>(task as never);
