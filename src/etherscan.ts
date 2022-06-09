import { parseHTML } from "linkedom";
import fetch, { Response } from "node-fetch";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import * as Retry from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";
import { formatUrl } from "url-sub";
import type { AbiItem } from "web3-utils";
import * as Config from "./config.js";
import { getEtherscanApiKey } from "./config.js";
import * as Duration from "./duration.js";
import * as Fetch from "./fetch.js";
import { BadResponseError, FetchError } from "./fetch.js";
import { B, E, flow, O, pipe, TE, TEAlt } from "./fp.js";
import * as Log from "./log.js";
import { queueOnQueueWithTimeoutThrown } from "./queues.js";

export const apiQueue = new PQueue({
  carryoverConcurrencyCount: true,
  concurrency: 2,
  interval: Duration.millisFromSeconds(4),
  intervalCap: 4,
  throwOnTimeout: true,
});

type AbiResponse = { status: "0" | "1"; result: string; message: string };

export class AbiApiError extends Error {}
export class AbiNotVerifiedError extends Error {}

export type FetchAbiError = AbiApiError | AbiNotVerifiedError | Error;

const makeAbiUrl = (address: string) =>
  formatUrl("https://api.etherscan.io", "/api", {
    module: "contract",
    action: "getabi",
    address,
    apiKey: getEtherscanApiKey(),
  });

export const fetchAbi = (address: string) =>
  pipe(
    Fetch.fetchWithRetryJson(makeAbiUrl(address)),
    queueOnQueueWithTimeoutThrown(apiQueue),
    TE.map((u) => u as AbiResponse),
    TE.chainW((abiRaw) => {
      if (abiRaw.status === "1") {
        return TE.right(JSON.parse(abiRaw.result) as AbiItem[]);
      }

      if (abiRaw.status === "0") {
        if (abiRaw.result === "Contract source code not verified") {
          return TE.left(
            new AbiNotVerifiedError("Contract source code not verified"),
          );
        }

        return TE.left(new AbiApiError(abiRaw.result));
      }

      Log.error("unexpected etherscan API response", abiRaw);
      return TE.left(new Error(abiRaw.result));
    }),
  );

// We want to not be pulling ABIs every time, at the same time they may get updated sometimes.
const abiCache = new QuickLRU<string, AbiItem[]>({
  maxSize: 1000,
  maxAge: Duration.millisFromHours(12),
});

const getCachedAbi = (address: string) =>
  pipe(abiCache.get(address), O.fromNullable);

const fetchAndCacheAbi = (address: string) =>
  pipe(
    fetchAbi(address),
    TE.chainFirstIOK((abi) => () => {
      abiCache.set(address, abi);
    }),
  );

export const getAbi = (address: string) =>
  pipe(
    getCachedAbi(address),
    O.match(() => fetchAndCacheAbi(address), TE.right),
  );

const blockscanRetryPolicy = Retry.Monoid.concat(
  Retry.exponentialBackoff(2000),
  Retry.limitRetries(5),
);

export class NoNameTagInHtmlError extends Error {}

export const getNameTag = (address: string) =>
  pipe(
    Fetch.fetchWithRetry(
      `https://blockscan.com/address/${address}`,
      undefined,
      { retryPolicy: blockscanRetryPolicy },
    ),
    TE.chainW(TE.tryCatchK((res) => res.text(), TEAlt.decodeUnknownError)),
    TE.chainEitherKW((text) => {
      const { document } = parseHTML(text);
      const etherscanPublicName = document.querySelector(
        ".badge-secondary",
      ) as {
        innerText: string;
      } | null;

      return pipe(
        etherscanPublicName?.innerText,
        E.fromNullable(new NoNameTagInHtmlError()),
      );
    }),
  );

// Etherscan is behind cloudflare. Locally cloudflare seems fine with our scraping requests, but from the digital ocean IPs it appears we get refused with a 403, perhaps failing some challenge.

const etherscanScrapeRetryPolicy = Retry.Monoid.concat(
  Retry.exponentialBackoff(2000),
  Retry.limitRetries(5),
);

const decodeResWithHiddenRateLimit = (
  res: Response,
  status: Retry.RetryStatus,
) =>
  pipe(
    res.status === 200,
    B.match(
      () =>
        pipe(
          Log.debugT(
            `fetch etherscan meta title failed, status: ${res.status}, attempt: ${status.iterNumber}, wait sum: ${status.cumulativeDelay}ms, retrying`,
          ),
          TE.fromTask,
          TE.chain(() =>
            TE.left(
              new BadResponseError(
                `fetch etherscan meta title, got ${res.status}`,
                res.status,
              ),
            ),
          ),
        ),
      // On a 200 response its still possible we hit an etherscan rate-limit. We parse the html to find out.
      flow(
        TE.tryCatchK(() => res.text(), TEAlt.decodeUnknownError),
        TE.chain((html) =>
          html.includes(
            "amounts of traffic coming from your network, please try again later",
          )
            ? TE.left(
                new Error("fetch etherscan meta title, hit hidden rate-limit"),
              )
            : TE.right(html),
        ),
      ),
    ),
  );

// This fetch is a special version of our normal retry fetch, it also parses the response html to check if etherscan is replying 200, but telling us to slow down.
const fetchMetaTitleWithSpecialRetry = (address: string) =>
  retrying(
    etherscanScrapeRetryPolicy,
    (status) =>
      pipe(
        TE.tryCatch(
          () => fetch(`https://etherscan.io/address/${address}`),
          (e) => (e instanceof Error ? e : new FetchError(String(e))),
        ),
        TE.chain((res) => decodeResWithHiddenRateLimit(res, status)),
      ),
    E.isLeft,
  );

export class NoMeaningfulTitleError extends Error {}
type GetMetaTitleError = NoMeaningfulTitleError | Error;

export const getMetaTitle = (
  address: string,
): TE.TaskEither<GetMetaTitleError, string> =>
  pipe(
    fetchMetaTitleWithSpecialRetry(address),
    TE.chainEitherK((html) => {
      const { document } = parseHTML(html);
      const etherscanTokenName = document.querySelector(
        "meta[property='og:title']",
      );

      if (
        etherscanTokenName === null ||
        etherscanTokenName.getAttribute === undefined
      ) {
        return E.left(new Error('no meta element with property="og-title"'));
      }

      const rawTokenName = etherscanTokenName.getAttribute("content");
      if (rawTokenName === null) {
        return E.left(new Error("no attribute 'content' in meta element"));
      }

      // Examples:
      // SHIBA INU (SHIB) Token Tracker | Etherscan
      // Tether USD (USDT) Token Tracker | Etherscan
      // USD Coin | 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
      // Contract address 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
      const nameRegex = /^(\w+\s?){1,2}(:\s(\w+\s?){1,2})?/;

      return pipe(
        rawTokenName.match(nameRegex),
        O.fromNullable,
        O.map((matches) => matches[0]),
        O.map((rawName) => rawName.trimEnd()),
        O.match(
          () =>
            E.left(
              new Error(
                `found etherscan token page, but failed to parse meta for ${address}`,
              ),
            ),
          (name) =>
            name === "Contract Address"
              ? E.left(
                  new NoMeaningfulTitleError(
                    "meta title is not contract specific but generic",
                  ),
                )
              : E.right(name),
        ),
      );
    }),
  );

type EthSupplyResponse =
  | { status: "0"; message: string }
  | {
      status: "1";
      message: string;
      result: string;
    };

const makeEthSupplyUrl = () =>
  formatUrl("https://api.etherscan.io", "/api", {
    module: "stats",
    action: "ethsupply",
    apiKey: Config.getEtherscanApiKey(),
  });

/**
 * Returns the current eth supply in Wei as a bigint.
 */
export const getEthSupply = () =>
  pipe(
    Fetch.fetchWithRetry(makeEthSupplyUrl()),
    queueOnQueueWithTimeoutThrown(apiQueue),
    TE.chain((res) =>
      TE.tryCatch(
        () => res.json() as Promise<EthSupplyResponse>,
        TEAlt.decodeUnknownError,
      ),
    ),
    TE.chainEitherK((body) => {
      if (body.status === "1") {
        return E.right(BigInt(body.result));
      }

      if (body.status === "0") {
        Log.error("get etherescan eth supply error", body);
        return E.left(new Error(body.message));
      }

      Log.error("get etherscan eth supply unexpected response", body);
      return E.left(new Error("get etherscan eth supply, unexpected response"));
    }),
  );
