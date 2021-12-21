import fetch, { RequestInfo, RequestInit, Response } from "node-fetch";
import { delay } from "./delay.js";
import * as Log from "./log.js";

export const withRetry = (
  limit = 3,
  delayMillis = 2000,
  useBinaryExponentialBackoff = true,
) => {
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
