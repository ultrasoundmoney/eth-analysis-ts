import * as Config from "./config.js";
import * as FetchAlt from "./fetch_alt.js";
import * as Log from "./log.js";
import * as Retry from "retry-ts";
import urlcatM from "urlcat";
import { E, pipe, T, TE } from "./fp.js";
import { sql } from "./db.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

type OpenseaContract = {
  address: string;
  collection: {
    twitter_username: string | null;
  } | null;
  schema_name: "ERC721" | "ERC1155" | string;
  image_url: string | null;
  name: string | null;
};

const makeContractUrl = (address: string): string =>
  urlcat("https://api.opensea.io/api/v1/asset_contract/:address", { address });

export class MissingStandardError extends Error {
  address: string;
  constructor(address: string, message: string | undefined) {
    super(message);
    this.address = address;
  }
}

export type GetContractError =
  | MissingStandardError
  | FetchAlt.FetchWithRetryError;

export const getContract = (
  address: string,
): TE.TaskEither<GetContractError, OpenseaContract> =>
  pipe(
    FetchAlt.fetchWithRetry(
      makeContractUrl(address),
      {
        headers: { "X-API-KEY": Config.getOpenseaApiKey() },
      },
      [200, 406],
      // Unsure about Opensea API rate-limit. Could experiment with lowering this and figuring out the exact codes we should and shouldn't retry.
      Retry.Monoid.concat(
        Retry.exponentialBackoff(2000),
        Retry.limitRetries(3),
      ),
    ),
    TE.chainW((res) => {
      // For some contracts OpenSea can't figure out the contract standard and returns a 406.
      if (res.status === 406) {
        return pipe(
          () => res.json() as Promise<{ detail: string }>,
          T.map((body) => {
            Log.debug(
              `fetch opensea contract 406, address: ${address}, body detail: ${body.detail}`,
            );
            return E.left(new MissingStandardError(address, body.detail));
          }),
        );
      }

      return pipe(() => res.json() as Promise<OpenseaContract>, T.map(E.right));
    }),
  );

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
