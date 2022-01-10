import * as DateFns from "date-fns";
import { parseHTML } from "linkedom";
import fetch from "node-fetch";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import * as Retry from "retry-ts";
import { constantDelay, limitRetries, Monoid } from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";
import urlcatM from "urlcat";
import type { AbiItem } from "web3-utils";
import * as Config from "./config.js";
import { getEtherscanToken } from "./config.js";
import * as Duration from "./duration.js";
import { EthPrice } from "./eth_prices.js";
import * as FetchAlt from "./fetch_alt.js";
import { BadResponseError, FetchError } from "./fetch_alt.js";
import { E, O, pipe, T, TE, TEAlt } from "./fp.js";
import * as Log from "./log.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

export const apiQueue = new PQueue({
  concurrency: 2,
  interval: Duration.millisFromSeconds(4),
  intervalCap: 5,
});

const queueApiCall =
  <A>(task: T.Task<A>): T.Task<A> =>
  () =>
    apiQueue.add(task);

type AbiResponse = { status: "0" | "1"; result: string; message: string };

export class AbiApiError extends Error {}
export class AbiNotVerifiedError extends Error {}

export type FetchAbiError = AbiApiError | AbiNotVerifiedError | Error;

const makeAbiUrl = (address: string) =>
  urlcat("https://api.etherscan.io/api", {
    module: "contract",
    action: "getabi",
    address,
    apiKey: getEtherscanToken(),
  });

export const fetchAbi = (
  address: string,
): TE.TaskEither<FetchAbiError, AbiItem[]> =>
  pipe(
    FetchAlt.fetchWithRetry(makeAbiUrl(address)),
    queueApiCall,
    TE.chain((res) =>
      TE.tryCatch(
        () => res.json() as Promise<AbiResponse>,
        TEAlt.errorFromUnknown,
      ),
    ),
    TE.chain((abiRaw): TE.TaskEither<Error, AbiItem[]> => {
      if (abiRaw.status === "1") {
        return TE.right(JSON.parse(abiRaw.result));
      }

      if (abiRaw.status === "0") {
        if (abiRaw.result === "Contract source code not verified") {
          return TE.left(
            new AbiNotVerifiedError("Contract source code not verified"),
          );
        }

        return TE.left(new AbiApiError(abiRaw.result));
      }

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

export const getAbi = (address: string): TE.TaskEither<Error, AbiItem[]> =>
  pipe(
    getCachedAbi(address),
    O.match(() => fetchAndCacheAbi(address), TE.right),
  );

export const getNameTag = async (
  address: string,
): Promise<string | undefined> =>
  pipe(
    FetchAlt.fetchWithRetry(`https://blockscan.com/address/${address}`),
    queueApiCall,
    TE.chain((res) => TE.tryCatch(() => res.text(), TEAlt.errorFromUnknown)),
    TE.map((text) => {
      const { document } = parseHTML(text);
      const etherscanPublicName = document.querySelector(
        ".badge-secondary",
      ) as {
        innerText: string;
      } | null;

      return etherscanPublicName?.innerText;
    }),
    TE.match(
      (e) => {
        Log.error("error fetching etherscan name tag through blockscan", e);
        return undefined;
      },
      (v) => v,
    ),
  )();

// Etherscan is behind cloudflare. Locally cloudflare seems fine with our scraping requests, but from the digital ocean IPs it appears we get refused with a 403, perhaps failing some challenge.

const etherscanScrapeRetryPolicy = Retry.Monoid.concat(
  Retry.exponentialBackoff(2000),
  Retry.limitRetries(5),
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
        TE.chain((res) => {
          // On a 200 response its still possible we hit an etherscan rate-limit. We parse the html to find out.
          if (res.status === 200) {
            return pipe(
              TE.tryCatch(() => res.text(), TEAlt.errorFromUnknown),
              TE.chain((html) =>
                html.includes(
                  "amounts of traffic coming from your network, please try again later",
                )
                  ? TE.left(
                      new Error("fetch teherscan meta title, hit rate-limit"),
                    )
                  : TE.right(html),
              ),
            );
          }

          Log.debug(
            `fetch etherscan meta title failed, status: ${res.status}, attempt: ${status.iterNumber}, wait sum: ${status.cumulativeDelay}ms, retrying`,
          );

          return TE.left(
            new BadResponseError(
              `fetch etherscan meta title, got ${res.status}`,
              res.status,
            ),
          );
        }),
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
        console.log(html);

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

type UnixTimestampStr = string;

type EthPriceResponse =
  | { status: "0"; message: string }
  | {
      status: "1";
      message: string;
      result: {
        ethbtc: string;
        ethbtc_timestamp: UnixTimestampStr;
        ethusd: string;
        ethusd_timestamp: UnixTimestampStr;
      };
    };

const fetchEthPrice = (): TE.TaskEither<string, EthPrice> => {
  const url = urlcat("https://api.etherscan.io/api", {
    module: "stats",
    action: "ethprice",
    apiKey: Config.getEtherscanToken(),
  });

  return pipe(
    TE.tryCatch(() => apiQueue.add(() => fetch(url)), String),
    TE.chain((res) => {
      if (res.status !== 200) {
        return TE.left(`fetch etherscan eth price status: ${res.status}`);
      }

      return TE.fromTask(() => res.json() as Promise<EthPriceResponse>);
    }),
    TE.chain((ethPriceResponse: EthPriceResponse) => {
      if (ethPriceResponse.status !== "1") {
        return TE.left(
          `fetch etherscan eth price, api error, status: ${ethPriceResponse.status}, message: ${ethPriceResponse.message}`,
        );
      }

      return TE.right({
        timestamp: DateFns.fromUnixTime(
          Number(ethPriceResponse.result.ethusd_timestamp),
        ),
        ethusd: Number(ethPriceResponse.result.ethusd),
      });
    }),
  );
};

export const getEthPrice = () =>
  retrying(
    Monoid.concat(constantDelay(2000), limitRetries(2)),
    () => fetchEthPrice(),
    E.isLeft,
  );

type EthSupplyResponse = {
  status: "0" | "1";
  message: string;
  result: string;
};

const makeEthSupplyUrl = () =>
  urlcat("https://api.etherscan.io/api", {
    module: "stats",
    action: "ethsupply",
    apiKey: Config.getEtherscanToken(),
  });

export const getEthSupply = () =>
  pipe(
    FetchAlt.fetchWithRetry(makeEthSupplyUrl()),
    queueApiCall,
    TE.chain((res) => {
      return TE.tryCatch(
        () => res.json() as Promise<EthSupplyResponse>,
        TEAlt.errorFromUnknown,
      );
    }),
    TE.map((body) => BigInt(body.result)),
  );
