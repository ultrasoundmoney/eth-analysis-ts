import * as Log from "./log.js";
import fetch from "node-fetch";

type OpenSeaContract = {
  collection: {
    twitter_username: string | null;
  } | null;
};

export const getTwitterHandle = async (
  address: string,
): Promise<string | undefined> => {
  const res = await fetch(
    `https://api.opensea.io/api/v1/asset_contract/${address}`,
  );

  // For some contracts OpenSea can't figure out the contract standard and returns a 406.
  if (res.status === 406) {
    return undefined;
  }

  if (res.status !== 200) {
    throw new Error(`fetch opensea contract, bad response: ${res.status}`);
  }

  const body = (await res.json()) as OpenSeaContract;

  const twitterHandle = body.collection?.twitter_username ?? undefined;
  Log.debug(`fetched opensea twitter handle ${twitterHandle} for ${address}`);

  return twitterHandle;
};
