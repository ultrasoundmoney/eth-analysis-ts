import * as Config from "./config.js";
import * as Duration from "./duration.js";
import * as Log from "./log.js";
import fetch from "node-fetch";
import { delay } from "./delay.js";
import PQueue from "p-queue";
import { sql } from "./db.js";

type OpenseaContract = {
  address: string;
  collection: {
    twitter_username: string | null;
  } | null;
  schema_name: "ERC721" | "ERC1155" | string;
  image_url: string | null;
  name: string | null;
};

export const fetchContractQueue = new PQueue({
  concurrency: 1,
  interval: Duration.milisFromSeconds(15),
  intervalCap: 2,
});

export const getContract = async (
  address: string,
  attempt = 0,
): Promise<OpenseaContract | undefined> => {
  const res = await fetchContractQueue.add(() =>
    fetch(`https://api.opensea.io/api/v1/asset_contract/${address}`, {
      headers: { "X-API-KEY": Config.getOpenseaApiKey() },
    }),
  );

  if (res === undefined) {
    Log.debug(
      "hit timeout for opensea contract fetch on queue, returning undefined",
    );
    return undefined;
  }

  const retryDelay = Duration.milisFromSeconds(16);

  if (res.status === 504 && attempt < 3) {
    Log.warn(
      `fetch opensea contract 504, attempt ${attempt}, waiting and retrying`,
      { address },
    );

    await delay(retryDelay);
    return getContract(address, attempt + 1);
  }

  if (res.status === 504 && attempt > 2) {
    Log.warn(
      `fetch opensea contract 504, attempt ${attempt}, hit limit, returning undefined`,
    );
    return undefined;
  }

  if (res.status === 429 && attempt < 3) {
    Log.warn(
      `fetch opensea contract 429, attempt ${attempt}, waiting and retrying`,
      { address },
    );
    await delay(retryDelay);
    return getContract(address, attempt + 1);
  }

  if (res.status === 429 && attempt > 2) {
    Log.error(
      `fetch opensea contract 429, attempt ${attempt}, hit limit, slow request rate! returning undefined`,
      { address },
    );
    return undefined;
  }

  if (res.status === 503 && attempt < 3) {
    Log.warn(
      `fetch opensea contract 503, attempt ${attempt}, waiting and retrying`,
      { address },
    );

    await delay(retryDelay);
    return getContract(address, attempt + 1);
  }

  if (res.status === 503 && attempt > 2) {
    Log.warn(
      `fetch opensea contract 503, attempt ${attempt}, hit limit, returning undefined`,
    );
    return undefined;
  }

  if (res.status === 404) {
    return undefined;
  }

  // For some contracts OpenSea can't figure out the contract standard and returns a 406.
  if (res.status === 406) {
    return undefined;
  }

  if (res.status !== 200) {
    throw new Error(
      `fetch opensea contract ${address}, attempt: ${attempt}, bad response: ${res.status}`,
    );
  }

  const body = (await res.json()) as OpenseaContract;

  return body;
};

export const getTwitterHandle = (
  contract: OpenseaContract,
): string | undefined => {
  const rawTwitterHandle = contract.collection?.twitter_username ?? undefined;

  if (rawTwitterHandle === undefined) {
    Log.debug(
      `found no twitter handle in opensea contract ${contract.address}`,
    );
    return undefined;
  }

  const re1 = /^@?(\w{1,15})/;
  const re2 = /^https:\/\/twitter.com\/@?(\w{1,15})/;

  const match1 = re1.exec(rawTwitterHandle);
  if (match1 !== null) {
    Log.debug(
      `found opensea twitter handle ${match1[1]} for ${contract.address}`,
    );
    return match1[1];
  }

  const match2 = re2.exec(rawTwitterHandle);
  if (match2 !== null) {
    Log.debug(
      `found opensea twitter handle ${match2[1]} for ${contract.address}`,
    );
    return match2[1];
  }

  Log.debug(
    `opensea twitter handle regex did not match, returning as is: ${rawTwitterHandle}`,
  );
  return rawTwitterHandle;
};

export const getSchemaName = (
  contract: OpenseaContract,
): string | undefined => {
  const schemaName = contract.schema_name;

  if (
    schemaName === "ERC721" ||
    schemaName === "ERC1155" ||
    schemaName === "ERC20" ||
    schemaName === "UNKNOWN"
  ) {
    Log.debug(
      `found opensea schema name ${schemaName} for ${contract.address}`,
    );
    return schemaName;
  }

  if (typeof schemaName === "string") {
    Log.warn(
      `found unknown opensea schema name: ${schemaName} for ${contract.address}, please explicitly handle, setting schema name undefined`,
    );
    return undefined;
  }

  return undefined;
};

export const checkSchemaImpliesNft = (schemaName: unknown): boolean =>
  typeof schemaName === "string" &&
  (schemaName === "ERC721" || schemaName === "ERC1155");

export const getContractLastFetch = async (
  address: string,
): Promise<Date | undefined> => {
  const rows = await sql<{ openseaContractLastFetch: Date | null }[]>`
    SELECT opensea_contract_last_fetch
    FROM contracts
    WHERE address = ${address}
  `;

  return rows[0]?.openseaContractLastFetch ?? undefined;
};

export const setContractLastFetchNow = async (
  address: string,
): Promise<void> => {
  await sql`
    UPDATE contracts
    SET opensea_contract_last_fetch = ${new Date()}
    WHERE address = ${address}
  `;
  return undefined;
};
