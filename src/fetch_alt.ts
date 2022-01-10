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

type RetryOptions = {
  acceptStatuses?: number[];
  retryPolicy?: Retry.RetryPolicy;
  noRetryStatuses?: number[];
};

const defaultRetryOptions = {
  acceptStatuses: [200, 201, 202, 204, 206],
  retryPolicy: Retry.Monoid.concat(
    Retry.exponentialBackoff(2000),
    Retry.limitRetries(3),
  ),
  noRetryStatuses: [400, 403, 404],
};

export const fetchWithRetry = (
  url: RequestInfo,
  init?: RequestInit,
  options: RetryOptions = {},
): TE.TaskEither<FetchWithRetryError, Response> =>
  pipe({ ...defaultRetryOptions, ...options }, (options) =>
    retrying(
      options.retryPolicy,
      (status) =>
        pipe(
          TE.tryCatch(
            () => fetch(url, init),
            (e) => (e instanceof Error ? e : new FetchError(String(e))),
          ),
          TE.chain((res) => {
            if (options.acceptStatuses.includes(res.status)) {
              return TE.right(res);
            }

            Log.debug(
              `fetch ${url} failed, status: ${res.status}, attempt: ${status.iterNumber}, wait sum: ${status.cumulativeDelay}ms, retrying`,
            );

            return TE.left(
              new BadResponseError(
                `fetch ${url}, got ${res.status}`,
                res.status,
              ),
            );
          }),
        ),
      (eRes) =>
        pipe(
          eRes,
          E.match(
            (e) =>
              e instanceof BadResponseError &&
              options.noRetryStatuses.includes(e.status)
                ? // If the result is a bad response but the response status is in the don't-retry list, don't retry
                  false
                : // In all other cases, retry.
                  true,
            // We have a Right, don't retry.
            () => false,
          ),
        ),
    ),
  );
