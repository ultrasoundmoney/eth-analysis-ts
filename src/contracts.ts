import * as Duration from "./duration.js";
import * as Log from "./log.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as T from "fp-ts/lib/Task.js";
import * as Twitter from "./twitter.js";
import A from "fp-ts/lib/Array.js";
import PQueue from "p-queue";
import fetch from "node-fetch";
import type { AbiItem } from "web3-utils";
import { E, TE } from "./fp.js";
import { constantDelay, limitRetries, Monoid } from "retry-ts";
import { differenceInDays, differenceInHours } from "date-fns";
import { getEtherscanToken } from "./config.js";
import { parseHTML } from "linkedom";
import { pipe } from "fp-ts/lib/function.js";
import { retrying } from "retry-ts/lib/Task.js";
import { sql } from "./db.js";
import { web3 } from "./eth_node.js";

export const fetchEtherscanName = async (
  address: string,
): Promise<string | undefined> => {
  const html = await fetch(`https://blockscan.com/address/${address}`).then(
    (res) => {
      if (res.status !== 200) {
        throw new Error(
          `bad response trying to fetch etherscan name, status: ${res.status}`,
        );
      }
      return res.text();
    },
  );

  const { document } = parseHTML(html);
  const etherscanPublicName = document.querySelector(".badge-secondary") as {
    innerText: string;
  } | null;

  return etherscanPublicName?.innerText;
};

const fetchTokenTitleQueue = new PQueue({
  timeout: Duration.milisFromSeconds(16),
  interval: Duration.milisFromSeconds(4),
  intervalCap: 1,
});

export const fetchEtherscanTokenTitle = async (
  address: string,
): Promise<string | undefined> => {
  const html = await fetchTokenTitleQueue
    .add(() => fetch(`https://etherscan.io/token/${address}`))
    .then((res) => {
      if (res === undefined) {
        Log.debug(`fetch token page for ${address} timed out`);
        // Queue works with a timeout that returns undefined when hit.
        return undefined;
      }

      Log.debug(`fetched token page, status: ${res?.status}`);

      if (res.status === 403) {
        Log.warn(
          `fetch etherscan token page for ${address}, 403 - forbidden, rate limit?`,
        );
        return undefined;
      }

      if (res.status !== 200) {
        throw new Error(
          `fetch etherscan token page, bad response ${res.status}`,
        );
      }
      return res.text();
    });

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

export const getContractNameFetchedLongAgo = ({
  lastNameFetchAt,
}: {
  lastNameFetchAt: Date | null;
}): boolean =>
  lastNameFetchAt === null ||
  differenceInDays(new Date(), lastNameFetchAt) >= 3;

const updateContractMetadata = (
  address: string,
  name: string | null,
  imageUrl: string | null,
): Promise<void> =>
  sql`
    UPDATE contracts
    SET
      last_metadata_fetch_at = NOW(),
      name = ${name},
      image_url = ${imageUrl}
    WHERE address = ${address}
  `.then(() => undefined);

const getContractMetadata = (
  address: string,
): Promise<{
  name: string | undefined;
  lastMetadataFetchAt: Date | undefined;
  twitterHandle: string | undefined;
}> =>
  sql<
    {
      name: string | null;
      twitterHandle: string | null;
      lastMetadataFetchAt: Date | null;
    }[]
  >`
    SELECT name, twitter_handle, last_metadata_fetch_at FROM contracts
    WHERE address = ${address}
  `.then((rows) => ({
    name: rows[0]?.name ?? undefined,
    lastMetadataFetchAt: rows[0]?.lastMetadataFetchAt ?? undefined,
    twitterHandle: rows[0]?.twitterHandle ?? undefined,
  }));

const getOnChainName = async (address: string): Promise<string | undefined> => {
  const abi = await pipe(
    getAbi(address),
    TE.matchW(
      (e) => {
        if (e._tag === "api-error") {
          // Contract is not verified. Continue.
        } else {
          if (e._tag === "unknown") {
            Log.error("failed to fetch ABI", {
              address,
              type: e._tag,
              error: e.error,
            });
          } else {
            Log.error("failed to fetch ABI", { address, type: e._tag });
          }
        }
        return undefined;
      },
      (abi) => abi,
    ),
  )();

  if (abi !== undefined) {
    const contract = new web3!.eth.Contract(abi, address);
    const hasNameMethod = contract.methods["name"] !== undefined;

    if (hasNameMethod) {
      return contract.methods.name().call();
    }
  }

  return undefined;
};

const getContractName = async (
  address: string,
): Promise<string | undefined> => {
  const abiName = await getOnChainName(address);
  if (typeof abiName === "string") {
    return abiName;
  }

  const etherscanTag = await fetchEtherscanName(address);
  Log.debug(`tried to fetch etherscan tag got: ${etherscanTag}`);
  if (typeof etherscanTag === "string") {
    return etherscanTag;
  }

  const etherscanTokenTitle = await fetchEtherscanTokenTitle(address);
  if (typeof etherscanTokenTitle === "string") {
    return etherscanTokenTitle;
  }

  return;
};

const addContractMetadata = async (address: string): Promise<void> => {
  const {
    name: currentName,
    lastMetadataFetchAt,
    twitterHandle,
  } = await getContractMetadata(address);

  if (
    lastMetadataFetchAt !== undefined &&
    differenceInHours(new Date(), lastMetadataFetchAt) < 3
  ) {
    // Don't attempt to fetch contract names more than once every three hours.
    return;
  }

  const name =
    (await (typeof currentName === "string"
      ? // Don't overwrite existing names.
        Promise.resolve(currentName)
      : getContractName(address))) ?? null;

  PerformanceMetrics.onContractIdentified();

  const imageUrl =
    twitterHandle === undefined
      ? null
      : (await Twitter.getImageUrl(twitterHandle)) ?? null;

  await updateContractMetadata(address, name, imageUrl);
};

export const addContractsMetadata = (addresses: string[]): Promise<void[]> =>
  Promise.all(addresses.map((address) => addContractMetadata(address)));

export const storeContracts = (addresses: string[]): T.Task<void> => {
  if (addresses.length === 0) {
    return T.of(undefined);
  }

  return pipe(
    addresses,
    A.map((address) => ({ address })),
    (addresses) => () =>
      sql`
        INSERT INTO contracts
        ${sql(addresses, "address")}
        ON CONFLICT DO NOTHING
      `,
    T.map(() => undefined),
  );
};

type AbiRaw = { status: "0" | "1"; result: string; message: string };

type BadGateway = { _tag: "bad-gateway" };
type ServiceUnavailable = { _tag: "service-unavailable" };
type UnknownError = { _tag: "unknown"; error: Error };
type EtherscanBadResponse = { _tag: "bad-response"; statusCode: number };
type EtherscanApiError = { _tag: "api-error"; message: string };
type JsonDecodeError = { _tag: "json-decode" };
type GetAbiError =
  | BadGateway
  | ServiceUnavailable
  | EtherscanApiError
  | EtherscanBadResponse
  | JsonDecodeError
  | UnknownError;

export const getAbi = (
  address: string,
): TE.TaskEither<GetAbiError, AbiItem[]> =>
  retrying(
    Monoid.concat(constantDelay(1000), limitRetries(3)),
    () =>
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
              () => res.json() as Promise<AbiRaw>,
              () => ({ _tag: "json-decode" as const }),
            ),
            TE.chain(
              (abiRaw): TE.TaskEither<GetAbiError, AbiItem[]> =>
                abiRaw.status === "1"
                  ? TE.right(JSON.parse(abiRaw.result))
                  : abiRaw.status === "0"
                  ? TE.left({
                      _tag: "api-error" as const,
                      message: `${abiRaw.message} - ${abiRaw.result}`,
                    })
                  : TE.left({ _tag: "api-error", message: abiRaw.result }),
            ),
          );
        }),
      ),
    E.isLeft,
  );

export const setTwitterHandle = (
  address: string,
  handle: string,
): T.Task<void> =>
  pipe(
    () => sql`
      UPDATE contracts
      SET
        ${sql({ twitter_handle: handle, last_metadata_fetch_at: null })}
      WHERE
        address = ${address}
    `,
    T.map(() => undefined),
  );

export const setName = (address: string, name: string): T.Task<void> =>
  pipe(
    () => sql`
      UPDATE contracts
      SET
        ${sql({ name })}
      WHERE
        address = ${address}
    `,
    T.map(() => undefined),
  );

export const setCategory = (address: string, category: string): T.Task<void> =>
  pipe(
    () => sql`
      UPDATE contracts
      SET
        ${sql({ category })}
      WHERE
        address = ${address}
    `,
    T.map(() => undefined),
  );
