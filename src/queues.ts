import PQueue from "p-queue";
import { E, pipe, T, TE } from "./fp.js";

export class TimeoutError extends Error {}
export class QueueError extends Error {}

const getIsPQueueTimeoutError = (u: unknown): boolean =>
  typeof (u as { name: string })?.name === "string" &&
  (u as { name: string }).name === "TimeoutError";

/** Only works with p-queue, and only if timeouts are thrown as errors. */
export const queueOnQueueWithTimeoutTE =
  <E, A>(queue: PQueue) =>
  (task: TE.TaskEither<E, A>) =>
    pipe(
      TE.tryCatch(
        () => queue.add<E.Either<E, A>>(task as never),
        (error) =>
          getIsPQueueTimeoutError(error) ? new TimeoutError() : (error as E),
      ),
      // tryCatch returns an Either, but our task also returns an Either, so we need to flatten it.
      T.map(E.flattenW),
    );

export const queueOnQueueWithTimeoutT =
  <A>(queue: PQueue) =>
  (task: T.Task<A>): TE.TaskEither<TimeoutError | QueueError, A> =>
    pipe(
      TE.tryCatch(
        () => queue.add<A>(task as never),
        (error) =>
          getIsPQueueTimeoutError(error)
            ? new TimeoutError()
            : // If this is not a timeout error, we don't know what it is.
              new QueueError("expect Task to never throw an error."),
      ),
    );

export const queueOnQueue =
  <E, A>(queue: PQueue) =>
  (task: TE.TaskEither<E, A>): TE.TaskEither<E, A> =>
  () =>
    queue.add<E.Either<E, A>>(task as never);

export const queueOnQueueT =
  <A>(queue: PQueue) =>
  (task: T.Task<A>): T.Task<A> =>
  () =>
    queue.add<A>(task as never);
