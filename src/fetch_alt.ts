import fetch, { RequestInfo, RequestInit, Response } from "node-fetch";
import * as Retry from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";
import { delay } from "./delay.js";
import { E, pipe, TE } from "./fp.js";
import * as Log from "./log.js";

type FetchFn = (
  url: RequestInfo,
  init?: RequestInit | undefined,
) => Promise<Response>;

export const withRetry = (
  limit = 3,
  delayMillis = 2000,
  useBinaryExponentialBackoff = true,
): FetchFn => {
  let attempt = 1;
  return async (url: RequestInfo, init?: RequestInit): Promise<Response> => {
    let res = await fetch(url, init);

    const delayMultiplier = useBinaryExponentialBackoff
      ? 2 ** (attempt - 1)
      : 1;
    const nextDelay = delayMillis * delayMultiplier;

    while (attempt !== limit) {
      if (res.status >= 200 && res.status < 300) {
        return res;
      }

      Log.debug(
        `fetch ${url} failed, status: ${res.status}, attempt: ${attempt}, retrying`,
      );

      Log.debug(`waiting ${nextDelay / 1000}s before retry`);
      await delay(nextDelay);

      attempt = attempt + 1;
      res = await fetch(url, init);
    }

    Log.debug(
      `fetch ${url} failed, hit retry limit, returning response as is.`,
    );

    return res;
  };
};

export class FetchError extends Error {}
export class BadResponseError extends Error {
  public status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

export type FetchWithRetryError = FetchError | BadResponseError | Error;

export const fetchWithRetry = (
  url: RequestInfo,
  init?: RequestInit,
  acceptStatuses = [200, 201, 202, 204, 206],
  retryPolicy = Retry.Monoid.concat(
    Retry.exponentialBackoff(2000),
    Retry.limitRetries(3),
  ),
): TE.TaskEither<FetchWithRetryError, Response> =>
  retrying(
    retryPolicy,
    (status) =>
      pipe(
        TE.tryCatch(
          () => fetch(url, init),
          (e) => (e instanceof Error ? e : new FetchError(String(e))),
        ),
        TE.chain((res) => {
          if (acceptStatuses.includes(res.status)) {
            return TE.right(res);
          }

          Log.debug(
            `fetch ${url} failed, status: ${res.status}, attempt: ${status.iterNumber}, wait sum: ${status.cumulativeDelay}ms, retrying`,
          );

          return TE.left(
            new BadResponseError(`fetch ${url}, got ${res.status}`, res.status),
          );
        }),
      ),
    E.isLeft,
  );
