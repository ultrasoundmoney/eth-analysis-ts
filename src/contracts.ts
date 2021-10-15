import * as DateFns from "date-fns";
import * as Duration from "./duration.js";
import * as Etherscan from "./etherscan.js";
import * as Log from "./log.js";
import * as OpenSea from "./opensea.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as T from "fp-ts/lib/Task.js";
import * as Twitter from "./twitter.js";
import A from "fp-ts/lib/Array.js";
import PQueue from "p-queue";
import { TE } from "./fp.js";
import { differenceInDays, differenceInHours } from "date-fns";
import { constant, constVoid, pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { web3 } from "./eth_node.js";

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
    Etherscan.getAbi(address),
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

  const etherscanTag = await Etherscan.getName(address);
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

  const getHandleAndImageAndCategory = async (): Promise<
    [string | undefined, string | undefined, string | undefined]
  > => {
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

  const [name, [twitterHandle, imageUrl, category]] = await Promise.all([
    getName(),
    getHandleAndImageAndCategory(),
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

export const fetchMetadataQueue = new PQueue({
  timeout: Duration.milisFromSeconds(30),
  concurrency: 1,
});

export const addContractsMetadata = (addresses: Set<string>): T.Task<void> => {
  return pipe(
    Array.from(addresses),
    T.traverseSeqArray((address) =>
      constant(fetchMetadataQueue.add(constant(addContractMetadata(address)))),
    ),
    T.map(constVoid),
  );
};

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
