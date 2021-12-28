import * as DateFns from "date-fns";
import { parseHTML } from "linkedom";
import fetch from "node-fetch";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import { constantDelay, limitRetries, Monoid } from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";
import urlcatM from "urlcat";
import type { AbiItem } from "web3-utils";
import * as Config from "./config.js";
import { getEtherscanToken } from "./config.js";
import * as Duration from "./duration.js";
import { EthPrice } from "./eth_prices.js";
import * as FetchAlt from "./fetch_alt.js";
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

export const fetchTokenTitleQueue = new PQueue({
  interval: Duration.millisFromSeconds(8),
  intervalCap: 2,
});

const browserUA =
  "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Mobile Safari/537.36";

// Etherscan is behind cloudflare. Locally cloudflare seems fine with our scraping requests, but from the digital ocean IPs it appears we get refused with a 403, perhaps failing some challenge.
export const getTokenTitle = async (
  address: string,
): Promise<string | undefined> => {
  const html = await fetchTokenTitleQueue
    .add(() =>
      fetch(`https://etherscan.io/token/${address}`, {
        compress: true,
        highWaterMark: 1024 * 1024,
        headers: {
          Accept: "*/*",
          UserAgent: browserUA,
        },
      }),
    )
    .then((res) => {
      if (res === undefined) {
        Log.debug(`fetch token page for ${address} timed out`);
        // Queue works with a timeout that returns undefined when hit.
        return undefined;
      }

      // Etherscan seems to 403 when we request too much.
      if (res.status === 403) {
        Log.warn(`fetch etherscan token page for ${address}, 403 - forbidden`, {
          address,
        });
        return undefined;
      }

      if (res.status !== 200) {
        throw new Error(
          `fetch etherscan token page, bad response ${res.status}`,
        );
      }
      return res.text();
    });

  if (html === undefined) {
    Log.debug(
      "hit timeout on etherscan token title page fetch, returning undefined",
    );
    return undefined;
  }

  const { document } = parseHTML(html);
  const etherscanTokenName = document.querySelector(
    "meta[property='og:title']",
  );

  if (
    etherscanTokenName === null ||
    etherscanTokenName.getAttribute === undefined
  ) {
    return undefined;
  }

  const rawTokenName = etherscanTokenName.getAttribute("content");
  if (rawTokenName === null) {
    return undefined;
  }

  // Examples:
  // SHIBA INU (SHIB) Token Tracker | Etherscan
  // Tether USD (USDT) Token Tracker | Etherscan
  // USD Coin | 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
  const tokenRegex = new RegExp(/^(.+)\s\((.+)\)/);
  const matches = tokenRegex.exec(rawTokenName);

  if (matches === null) {
    return undefined;
  }

  const tokenName = matches[1];
  const tokenTicker = matches[2];

  return tokenTicker === undefined ? tokenName : `${tokenName}: ${tokenTicker}`;
};

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

export const getEthSupply = (): Promise<bigint> =>
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
    TEAlt.getOrThrow,
  )();
