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

export const fetchWithRetry = (
  url: RequestInfo,
  init?: RequestInit,
): TE.TaskEither<Error, Response> =>
  retrying(
    Retry.Monoid.concat(Retry.exponentialBackoff(2000), Retry.limitRetries(3)),
    (status) =>
      pipe(
        TE.tryCatch(
          () => fetch(url, init),
          (e) => {
            if (e instanceof Error) {
              return e;
            }

            return new FetchError(String(e));
          },
        ),
        TE.chain((res) => {
          if (res.status >= 200 && res.status < 300) {
            return TE.right(res);
          }

          Log.debug(
            `fetch ${url} failed, status: ${res.status}, attempt: ${status.iterNumber}, wait sum: ${status.cumulativeDelay}ms, retrying`,
          );

          return TE.left(new Error("bad response"));
        }),
      ),
    E.isLeft,
  );
