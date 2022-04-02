import fetch, { RequestInfo, RequestInit } from "node-fetch";
import * as Retry from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";
import * as Errors from "./errors.js";
import { E, pipe, TE } from "./fp.js";
import * as Log from "./log.js";

export class FetchError extends Error {}
export class BadResponseError extends Error {
  public status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

type RetryOptions = {
  acceptStatuses?: number[];
  retryPolicy?: Retry.RetryPolicy;
  noRetryStatuses?: number[];
};

const defaultRetryOptions = {
  acceptStatuses: [200, 201, 202, 204, 206],
  retryPolicy: Retry.Monoid.concat(
    Retry.exponentialBackoff(2000),
    Retry.limitRetries(2),
  ),
  noRetryStatuses: [400, 403, 404],
};

export const fetchWithRetry = (
  url: RequestInfo,
  init?: RequestInit,
  options: RetryOptions = {},
) =>
  pipe({ ...defaultRetryOptions, ...options }, (options) =>
    retrying(
      options.retryPolicy,
      (status) =>
        pipe(
          TE.tryCatch(
            () => fetch(url, init),
            (e): Error | FetchError =>
              e instanceof Error ? e : new FetchError(String(e)),
          ),
          TE.chainW((res) => {
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
  );

export const fetchWithRetryJson = <A>(
  url: RequestInfo,
  init?: RequestInit,
  options: RetryOptions = {},
) =>
  pipe(
    fetchWithRetry(url, init, options),
    TE.chainW(
      TE.tryCatchK(
        (res) => res.json() as Promise<A>,
        Errors.decodeErrorFromUnknown,
      ),
    ),
  );

export const fetchTE = (
  url: RequestInfo,
  init?: RequestInit,
  options: RetryOptions = {},
) =>
  pipe({ ...defaultRetryOptions, ...options }, (options) =>
    pipe(
      TE.tryCatch(
        () => fetch(url, init),
        (e): Error | FetchError =>
          e instanceof Error ? e : new FetchError(String(e)),
      ),
      TE.chainW((res) => {
        if (options.acceptStatuses.includes(res.status)) {
          return TE.right(res);
        }

        Log.debug(`fetch ${url} failed, status: ${res.status}`);

        return TE.left(
          new BadResponseError(`fetch ${url}, got ${res.status}`, res.status),
        );
      }),
    ),
  );
