import nodeFetch, * as NodeFetch from "node-fetch";
import * as Retry from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";
import { E, IO, O, pipe, T, TE } from "./fp.js";
import * as Log from "./log.js";

export class DecodeJsonError extends Error {}
export class FetchError extends Error {}
export class BadResponseError extends Error {
  public status: number;
  public body: unknown | undefined;
  constructor(message: string, status: number, body?: unknown) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

const defaultAcceptStatuses = [200, 201, 202, 204, 206];

const parseMessageFromUnknown = (u: unknown) =>
  pipe(
    typeof u === "string" ? O.some(u) : O.none,
    O.alt(() =>
      typeof (u as { message?: string })?.message === "string"
        ? O.some((u as { message: string }).message)
        : O.none,
    ),
  );

export const fetch = (
  url: NodeFetch.RequestInfo,
  init?: NodeFetch.RequestInit,
  acceptStatuses: number[] = defaultAcceptStatuses,
) =>
  pipe(
    TE.tryCatch(
      () => nodeFetch(url, init),
      (u) =>
        u instanceof FetchError
          ? u
          : u instanceof Error
          ? u
          : new Error(String(u)),
    ),
    TE.chainW((res) =>
      acceptStatuses.includes(res.status)
        ? TE.right(res)
        : pipe(
            // We try to decode a json error body to put on the error, but do not expect it to work.
            TE.tryCatch(
              () => res.json(),
              // Failed to decode json error body. This is within expectations
              () =>
                new BadResponseError(
                  `failed to fetch ${url}, got status ${res.status}`,
                  res.status,
                ),
            ),
            // We managed to decode a json body despite the bad response.
            TE.chain((body) =>
              pipe(
                parseMessageFromUnknown(body),
                O.match(
                  () => `failed to fetch ${url}, got status ${res.status}`,
                  (message) =>
                    `failed to fetch ${url}, status: ${res.status}, message: ${message}`,
                ),
                (message) =>
                  TE.left(new BadResponseError(message, res.status, body)),
              ),
            ),
          ),
    ),
  );

export const decodeJsonResponse = TE.tryCatchK(
  (res: NodeFetch.Response) => res.json(),
  (e) =>
    e instanceof Error
      ? new DecodeJsonError(e.message)
      : new DecodeJsonError(String(e)),
);

export const fetchJson = (
  url: NodeFetch.RequestInfo,
  init?: NodeFetch.RequestInit,
  acceptStatuses: number[] = defaultAcceptStatuses,
) => pipe(fetch(url, init, acceptStatuses), TE.chainW(decodeJsonResponse));

const defaultRetryPolicy = Retry.Monoid.concat(
  Retry.exponentialBackoff(2000),
  Retry.limitRetries(2),
);
const defaultNoRetryStatuses = [400, 401, 403, 404, 405];

type FetchWithRetryOptions = {
  retryPolicy?: Retry.RetryPolicy;
  noRetryStatuses?: number[];
  acceptStatuses?: number[];
};

export const fetchWithRetry = (
  url: NodeFetch.RequestInfo,
  init?: NodeFetch.RequestInit,
  {
    acceptStatuses = defaultAcceptStatuses,
    noRetryStatuses = defaultNoRetryStatuses,
    retryPolicy = defaultRetryPolicy,
  }: FetchWithRetryOptions = {},
) =>
  retrying(
    retryPolicy,
    (status) =>
      pipe(
        fetch(url, init, acceptStatuses),
        T.chainFirstIOK(
          E.match(
            (e) =>
              e instanceof BadResponseError &&
              noRetryStatuses.includes(e.status)
                ? // Status is a no retry status, i.e. expected, nothing to log.
                  IO.of(undefined)
                : e instanceof BadResponseError
                ? Log.debugIO(
                    `retrying request ${url}, last status was: ${e.status}, attempt: ${status.iterNumber}, wait sum: ${status.cumulativeDelay}ms`,
                  )
                : Log.debugIO(
                    `retrying request ${url}, attempt: ${status.iterNumber}, wait sum: ${status.cumulativeDelay}ms`,
                  ),
            () => IO.of(undefined),
          ),
        ),
      ),
    E.match(
      (e) =>
        e instanceof BadResponseError && noRetryStatuses.includes(e.status)
          ? // If the result is a bad response but the response status is in the don't-retry list, don't retry
            false
          : // In all other cases, retry.
            true,
      // We have a Right, don't retry.
      () => false,
    ),
  );

export const fetchWithRetryJson = (
  url: NodeFetch.RequestInfo,
  init?: NodeFetch.RequestInit,
  options: FetchWithRetryOptions = {},
) => pipe(fetchWithRetry(url, init, options), TE.chainW(decodeJsonResponse));
