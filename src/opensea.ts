import * as Duration from "./duration.js";
import * as Log from "./log.js";
import fetch from "node-fetch";
import { delay } from "./delay.js";
import PQueue from "p-queue";

type OpenSeaContract = {
  address: string;
  collection: {
    twitter_username: string | null;
  } | null;
  schema_name: "ERC721" | "ERC1155" | string;
  image_url: string | null;
};

export const fetchContractQueue = new PQueue({
  concurrency: 2,
  interval: Duration.milisFromSeconds(16),
  intervalCap: 3,
});

export const getContract = async (
  address: string,
  attempt = 0,
): Promise<OpenSeaContract | undefined> => {
  const res = await fetchContractQueue.add(() =>
    fetch(`https://api.opensea.io/api/v1/asset_contract/${address}`),
  );

  if (res === undefined) {
    Log.debug(
      "hit timeout for opensea contract fetch on queue, returning undefined",
    );
    return undefined;
  }

  if (res.status === 504 && attempt < 3) {
    Log.info(
      `fetch opensea contract 504, attempt ${attempt}, waiting 8s and retrying`,
      { address },
    );

    await delay(Duration.milisFromSeconds(8));
    return getContract(address, attempt + 1);
  }

  if (res.status === 504 && attempt > 2) {
    Log.info(
      `fetch opensea contract 504, attempt ${attempt}, hit limit, returning undefined`,
    );
    return undefined;
  }

  if (res.status === 429 && attempt < 3) {
    Log.warn(
      `fetch opensea contract 429, attempt ${attempt}, waiting 8s and retrying`,
      { address },
    );
    await delay(Duration.milisFromSeconds(8));
    return getContract(address, attempt + 1);
  }

  if (res.status === 429 && attempt > 2) {
    Log.error(
      `fetch opensea contract 429, attempt ${attempt}, hit limit, slow request rate! returning undefined`,
      { address },
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

  const body = (await res.json()) as OpenSeaContract;

  return body;
};

export const getTwitterHandle = (
  contract: OpenSeaContract,
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

export const getCategory = (contract: OpenSeaContract): string | undefined => {
  const schemaName = contract.schema_name;

  if (schemaName === "ERC721" || schemaName === "ERC1155") {
    Log.debug(
      `found opensea schema_name ${schemaName} for ${contract.address}, categorizing: nft`,
    );
    return "nft";
  }

  if (schemaName === "ERC20" || schemaName === "UNKNOWN") {
    Log.debug(
      `found known opensea schema name: ${schemaName}, for ${contract.address}, categorizing as undefined`,
    );
    return undefined;
  }

  if (typeof schemaName === "string") {
    Log.warn(
      `found unknown opensea schema name: ${schemaName} for ${contract.address}, please explicitly categorize, setting category undefined`,
    );
    return undefined;
  }

  return undefined;
};
