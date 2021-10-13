import * as Duration from "./duration.js";
import * as Log from "./log.js";
import fetch from "node-fetch";
import { delay } from "./delay.js";

type OpenSeaContract = {
  collection: {
    twitter_username: string | null;
  } | null;
};

export const getTwitterHandle = async (
  address: string,
  attempt = 0,
): Promise<string | undefined> => {
  const res = await fetch(
    `https://api.opensea.io/api/v1/asset_contract/${address}`,
  );

  if ((res.status === 429 || res.status === 504) && attempt < 2) {
    Log.warn(
      `fetch opensea contract 429, attempt ${attempt}, waiting 3s and retrying`,
    );
    await delay(Duration.milisFromSeconds(3));
    return getTwitterHandle(address, attempt + 1);
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
      `fetch opensea contract ${address}, bad response: ${res.status}`,
    );
  }

  const body = (await res.json()) as OpenSeaContract;

  const twitterHandle = body.collection?.twitter_username ?? undefined;
  Log.debug(`fetched opensea twitter handle ${twitterHandle} for ${address}`);

  return twitterHandle;
};
