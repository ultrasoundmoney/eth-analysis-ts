import * as Duration from "./duration.js";
import * as Log from "./log.js";
import PQueue from "p-queue";
import fetch from "node-fetch";
import type { AbiItem } from "web3-utils";
import { E, O, pipe, TE } from "./fp.js";
import { constantDelay, limitRetries, Monoid } from "retry-ts";
import { delay } from "./delay.js";
import { getEtherscanToken } from "./config.js";
import { parseHTML } from "linkedom";
import { retrying } from "retry-ts/lib/Task.js";
import QuickLRU from "quick-lru";

type AbiResponse = { status: "0" | "1"; result: string; message: string };

type BadGateway = { _tag: "bad-gateway" };
type ServiceUnavailable = { _tag: "service-unavailable" };
type UnknownError = { _tag: "unknown"; error: Error };
type EtherscanBadResponse = { _tag: "bad-response"; statusCode: number };
type EtherscanApiError = { _tag: "api-error"; message: string };
type JsonDecodeError = { _tag: "json-decode" };
type AbiNotFound = { _tag: "abi-not-found" };
type GetAbiError =
  | AbiNotFound
  | BadGateway
  | EtherscanApiError
  | EtherscanBadResponse
  | JsonDecodeError
  | ServiceUnavailable
  | UnknownError;

export const fetchAbi = (
  address: string,
): TE.TaskEither<GetAbiError, AbiItem[]> =>
  pipe(
    TE.tryCatch(
      () =>
        fetch(
          `https://api.etherscan.io/api?module=contract&action=getabi&address=${address}&apikey=${getEtherscanToken()}`,
        ),
      (e) => ({ _tag: "unknown" as const, error: e as Error }),
    ),
    TE.chain((res): TE.TaskEither<GetAbiError, AbiItem[]> => {
      if (res.status === 502) {
        return TE.left({
          _tag: "bad-gateway",
        });
      }

      if (res.status === 503) {
        return TE.left({
          _tag: "service-unavailable",
        });
      }

      if (res.status !== 200) {
        return TE.left({
          _tag: "bad-response",
          statusCode: res.status,
        });
      }

      return pipe(
        TE.tryCatch(
          () => res.json() as Promise<AbiResponse>,
          () => ({ _tag: "json-decode" as const }),
        ),
        TE.chain(
          (abiRaw): TE.TaskEither<GetAbiError, AbiItem[]> =>
            abiRaw.status === "1"
              ? TE.right(JSON.parse(abiRaw.result))
              : abiRaw.status === "0"
              ? TE.left({
                  _tag: "abi-not-found" as const,
                  message: `${abiRaw.message} - ${abiRaw.result}`,
                })
              : TE.left({ _tag: "api-error", message: abiRaw.result }),
        ),
      );
    }),
  );

const fetchAbiWithRetry = (
  address: string,
): TE.TaskEither<GetAbiError, AbiItem[]> =>
  retrying(
    Monoid.concat(constantDelay(1000), limitRetries(3)),
    () => fetchAbi(address),
    E.isLeft,
  );

// We want to not be pulling ABIs constantly, at the same time they may get updated sometimes.
const abiCache = new QuickLRU<string, AbiItem[]>({
  maxSize: 300,
  maxAge: Duration.milisFromHours(3),
});

export const getAbiWithCache = (
  address: string,
): Promise<AbiItem[] | undefined> =>
  pipe(
    abiCache.get(address),
    O.fromNullable,
    O.match(
      () => fetchAbiWithRetry(address),
      (abi) => TE.of(abi),
    ),
    TE.match(
      (e) => {
        // This is acceptable, probably want to handle this with Option instead.
        if (e._tag === "abi-not-found") {
          return undefined;
        }

        Log.error("get ABI failed", {
          address,
          message: "message" in e ? e.message : undefined,
          type: e._tag,
        });

        return undefined;
      },
      (abi) => {
        abiCache.set(address, abi);
        return abi;
      },
    ),
  )();

export const getNameTag = async (
  address: string,
  attempt = 0,
): Promise<string | undefined> => {
  const res = await fetch(`https://blockscan.com/address/${address}`);

  // CloudFlare timeout
  if (res.status === 522 && attempt < 2) {
    Log.warn(
      `fetch etherscan name for ${address}, cloudflare 522, attempt: ${attempt}, waiting 3s and retrying`,
    );
    await delay(Duration.milisFromSeconds(3));
    return getNameTag(address, attempt + 1);
  }

  if (res.status !== 200) {
    Log.error(
      `fetch etherscan name for ${address}, bad response ${res.status}`,
    );
    return undefined;
  }

  const html = await res.text();

  const { document } = parseHTML(html);
  const etherscanPublicName = document.querySelector(".badge-secondary") as {
    innerText: string;
  } | null;

  return etherscanPublicName?.innerText;
};

export const fetchTokenTitleQueue = new PQueue({
  interval: Duration.milisFromSeconds(8),
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
