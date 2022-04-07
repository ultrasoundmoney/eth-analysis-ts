import nodeFetch, * as NodeFetch from "node-fetch";
import * as Retry from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";
import { match, P } from "ts-pattern";
import { E, flow, IO, pipe, TE } from "./fp.js";
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

export const fetch = (
  url: NodeFetch.RequestInfo,
  init?: NodeFetch.RequestInit,
  acceptStatuses: number[] = defaultAcceptStatuses,
) =>
  pipe(
    TE.tryCatch(
      () => nodeFetch(url, init),
      (e): FetchError | Error =>
        match(e)
          .with(P.instanceOf(FetchError), (e) => e)
          .with(P.instanceOf(Error), (e) => e)
          .otherwise((u) => new Error(String(u))),
    ),
    TE.chainW((res) =>
      match(res.status)
        .when(
          (status) => acceptStatuses.includes(status),
          () => TE.right(res),
        )
        .otherwise(
          flow(
            // We try to decode a json error body to put on the error, but do not expect it to work.
            TE.tryCatchK(
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
              TE.left(
                new BadResponseError(
                  `failed to fetch ${url}, got status ${res.status}`,
                  res.status,
                  body,
                ),
              ),
            ),
          ),
        ),
    ),
  );

export const decodeJsonResponse = TE.tryCatchK(
  (res: NodeFetch.Response) => res.json(),
  (e) =>
    match(e)
      .with(P.instanceOf(Error), (e) => new DecodeJsonError(e.message))
      .otherwise((u) => new DecodeJsonError(String(u))),
);
export const fetchJson = (
  url: NodeFetch.RequestInfo,
  init?: NodeFetch.RequestInit,
  acceptStatuses: number[] = defaultAcceptStatuses,
) => pipe(fetch(url, init, acceptStatuses), TE.chain(decodeJsonResponse));

const defaultRetryPolicy = Retry.Monoid.concat(
  Retry.exponentialBackoff(2000),
  Retry.limitRetries(2),
);
const defaultNoRetryStatuses = [400, 401, 403, 404];

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
        TE.chainFirstIOK((res) =>
          status.iterNumber === 0
            ? IO.of(undefined)
            : Log.debugIO(
                `retrying request ${url}, last status was: ${res.status}, attempt: ${status.iterNumber}, wait sum: ${status.cumulativeDelay}ms`,
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
