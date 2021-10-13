import * as DateFns from "date-fns";
import * as Duration from "./duration.js";
import * as Log from "./log.js";
import * as OpenSea from "./opensea.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as T from "fp-ts/lib/Task.js";
import * as Twitter from "./twitter.js";
import A from "fp-ts/lib/Array.js";
import PQueue from "p-queue";
import fetch from "node-fetch";
import type { AbiItem } from "web3-utils";
import { E, TE } from "./fp.js";
import { constantDelay, limitRetries, Monoid } from "retry-ts";
import { delay } from "./delay.js";
import { differenceInDays, differenceInHours } from "date-fns";
import { getEtherscanToken } from "./config.js";
import { parseHTML } from "linkedom";
import { pipe } from "fp-ts/lib/function.js";
import { retrying } from "retry-ts/lib/Task.js";
import { sql } from "./db.js";
import { web3 } from "./eth_node.js";

export const fetchEtherscanName = async (
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
    return fetchEtherscanName(address, attempt + 1);
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

const fetchTokenTitleQueue = new PQueue({
  timeout: Duration.milisFromSeconds(8),
  interval: Duration.milisFromSeconds(60),
  intervalCap: 1,
});

const browserUA =
  "user-agent: Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Mobile Safari/537.36";

export const fetchEtherscanTokenTitle = async (
  address: string,
): Promise<string | undefined> => {
  Log.debug(`fetching etherscan token title for ${address}`);
  const html = await fetchTokenTitleQueue
    .add(() =>
      fetch(`https://etherscan.io/token/${address}`, {
        headers: { "User-Agent": browserUA },
      }),
    )
    .then((res) => {
      if (res === undefined) {
        Log.debug(`fetch token page for ${address} timed out`);
        // Queue works with a timeout that returns undefined when hit.
        return undefined;
      }

      Log.debug(`fetched token page, status: ${res?.status}`);

      // Etherscan seems to 403 when we request too much.
      if (res.status === 403) {
        Log.info(`fetch etherscan token page for ${address}, 403 - forbidden`, {
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
  twitterHandle: string | null,
  category: string | null,
): Promise<void> =>
  sql`
    UPDATE contracts
    SET
      last_metadata_fetch_at = NOW(),
      name = ${name},
      image_url = ${imageUrl},
      twitter_handle = ${twitterHandle},
      category = ${category}
    WHERE address = ${address}
  `.then(() => undefined);

type ContractMetadata = {
  name: string | undefined;
  lastMetadataFetchAt: Date | undefined;
  twitterHandle: string | undefined;
  category: string | undefined;
};

const getContractMetadata = (address: string): Promise<ContractMetadata> =>
  sql<
    {
      name: string | null;
      twitterHandle: string | null;
      lastMetadataFetchAt: Date | null;
      category: string | null;
    }[]
  >`
    SELECT name, twitter_handle, last_metadata_fetch_at, category FROM contracts
    WHERE address = ${address}
  `.then((rows) => ({
    name: rows[0]?.name ?? undefined,
    lastMetadataFetchAt: rows[0]?.lastMetadataFetchAt ?? undefined,
    twitterHandle: rows[0]?.twitterHandle ?? undefined,
    category: rows[0]?.category ?? undefined,
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

  // Starts failing with constant 403s after a while. Rate-limit seems to reset daily.
  // const etherscanTokenTitle = await fetchEtherscanTokenTitle(address);
  // if (typeof etherscanTokenTitle === "string") {
  //   Log.debug(`fetched token page name: ${etherscanTokenTitle}`);
  //   return etherscanTokenTitle;
  // }

  return;
};

const getHandleAndImageAndCategory = async (
  address: string,
  existingTwitterHandle: string | undefined,
  existingCategory: string | undefined,
) => {
  const openSeaContract = await OpenSea.getContract(address);

  const twitterHandle =
    typeof existingTwitterHandle === "string"
      ? existingTwitterHandle
      : // OpenSea doesn't have inforamtion on this address.
      openSeaContract === undefined
      ? undefined
      : OpenSea.getTwitterHandle(openSeaContract);

  const category =
    existingCategory === "string"
      ? existingCategory
      : // OpenSea doesn't have inforamtion on this address.
      openSeaContract === undefined
      ? undefined
      : OpenSea.getCategory(openSeaContract);

  const imageUrl =
    typeof twitterHandle === "string"
      ? await Twitter.getImageByHandle(twitterHandle)
      : undefined;

  return [twitterHandle, imageUrl, category];
};

const addContractMetadata = async (address: string): Promise<void> => {
  const {
    name: existingName,
    lastMetadataFetchAt,
    twitterHandle: existingTwitterHandle,
    category: existingCategory,
  } = await getContractMetadata(address);

  if (
    lastMetadataFetchAt !== undefined &&
    differenceInHours(new Date(), lastMetadataFetchAt) < 3
  ) {
    // Don't attempt to fetch contract names more than once every three hours.
    return;
  }

  const timeSinceUpdate =
    lastMetadataFetchAt && DateFns.formatDistanceToNow(lastMetadataFetchAt);
  Log.debug(
    `fetching metadata for ${address}, last updated: ${timeSinceUpdate} ago, existing name: ${existingName}, existing twitter handle: ${existingTwitterHandle}`,
  );

  const getName = async () =>
    typeof existingName === "string"
      ? // Don't overwrite existing names.
        existingName
      : await getContractName(address);

  const [name, [twitterHandle, imageUrl, category]] = await Promise.all([
    getName(),
    getHandleAndImageAndCategory(
      address,
      existingTwitterHandle,
      existingCategory,
    ),
  ]);

  PerformanceMetrics.onContractIdentified();

  Log.debug(
    `updating metadata for ${address}, name: ${name}, twitterHandle: ${twitterHandle}, imageUrl: ${imageUrl}, category: ${category}`,
  );

  await updateContractMetadata(
    address,
    name ?? null,
    imageUrl ?? null,
    twitterHandle ?? null,
    category ?? null,
  );
};

export const addContractsMetadata = (addresses: Set<string>): T.Task<void> =>
  pipe(
    Array.from(addresses),
    T.traverseArray((address) => () => addContractMetadata(address)),
    T.map(() => undefined),
  );

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
