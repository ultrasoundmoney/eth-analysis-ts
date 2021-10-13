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
};

const fetchContractQueue = new PQueue({
  timeout: Duration.milisFromSeconds(60),
  interval: Duration.milisFromSeconds(8),
  intervalCap: 8,
});

export const getContract = async (
  address: string,
  attempt = 0,
): Promise<OpenSeaContract | undefined> => {
  const res = await fetchContractQueue.add(() =>
    fetch(`https://api.opensea.io/api/v1/asset_contract/${address}`),
  );

  if ((res.status === 429 || res.status === 504) && attempt < 2) {
    Log.warn(
      `fetch opensea contract 429, attempt ${attempt}, waiting 3s and retrying`,
    );
    await delay(Duration.milisFromSeconds(3));
    return getContract(address, attempt + 1);
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

  const re1 = /^@?\w{1,15}$/;
  const re2 = /^https:\/\/twitter.com\/@?(\w{1,15})/;

  const match1 = re1.exec(rawTwitterHandle);
  if (match1 !== null) {
    Log.debug(
      `found opensea twitter handle ${match1[0]} for ${contract.address}`,
    );
    return match1[0];
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
  const category = contract.schema_name;

  if (category === "ERC721" || category === "ERC1155") {
    return "nft";
  }

  return undefined;
};
