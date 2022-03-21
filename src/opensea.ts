import * as DateFns from "date-fns";
import * as Retry from "retry-ts";
import urlcatM from "urlcat";
import * as Config from "./config.js";
import { readOptionalFromFirstRow, sqlT } from "./db.js";
import * as FetchAlt from "./fetch_alt.js";
import { A, E, flow, O, pipe, T, TE, TO } from "./fp.js";
import * as Log from "./log.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

export type OpenseaContract = {
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

export class NotFoundError extends Error {}

export const getContract = (address: string) =>
  pipe(
    FetchAlt.fetchWithRetry(
      makeContractUrl(address),
      {
        headers: { "X-API-KEY": Config.getOpenseaApiKey() },
      },
      {
        acceptStatuses: [200, 404, 406],
        // Unsure about Opensea API rate-limit. Could experiment with lowering this and figuring out the exact codes we should and shouldn't retry.
        retryPolicy: Retry.Monoid.concat(
          Retry.exponentialBackoff(2000),
          Retry.limitRetries(5),
        ),
      },
    ),
    TE.chainW(
      (
        res,
      ): TE.TaskEither<NotFoundError | MissingStandardError, OpenseaContract> =>
        res.status === 404
          ? TE.left(
              new NotFoundError("fetch opensea metadata, contract not found"),
            )
          : res.status === 406
          ? pipe(
              () => res.json() as Promise<{ detail: string }>,
              T.map((body) => {
                Log.debug(
                  `fetch opensea contract 406, address: ${address}, body detail: ${body.detail}`,
                );
                return E.left(new MissingStandardError(address, body.detail));
              }),
            )
          : pipe(
              () => res.json() as Promise<OpenseaContract>,
              (task) => TE.fromTask(task),
            ),
    ),
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
  (typeof schemaName === "string" && schemaName === "ERC721") ||
  schemaName === "ERC1155";

export const getContractLastFetch = (address: string): TO.TaskOption<Date> =>
  pipe(
    sqlT<{ openseaContractLastFetch: Date | null }[]>`
      SELECT opensea_contract_last_fetch
      FROM contracts
      WHERE address = ${address}
    `,
    T.map(
      flow(
        A.head,
        O.chain(flow((row) => row.openseaContractLastFetch, O.fromNullable)),
      ),
    ),
  );

export const setContractLastFetchNow = (address: string): T.Task<void> =>
  pipe(
    sqlT`
    UPDATE contracts
    SET opensea_contract_last_fetch = ${new Date()}
    WHERE address = ${address}
  `,
    T.map(() => undefined),
  );

export const getIsRecentlyFetched = (address: string): T.Task<boolean> =>
  pipe(
    getContractLastFetch(address),
    TO.map((lastFetch) => DateFns.differenceInHours(new Date(), lastFetch) < 6),
    TO.getOrElseW(() => T.of(false)),
  );

const getExistingOpenseaSchemaName = (
  address: string,
): T.Task<O.Option<string>> =>
  pipe(
    sqlT<{ openseaSchemaName: string | null }[]>`
      SELECT opensea_schema_name
      FROM contracts
      WHERE address = ${address}
    `,
    T.map(
      flow(
        A.head,
        O.map((row) => row.openseaSchemaName),
        O.map(O.fromNullable),
        O.flatten,
      ),
    ),
  );

export const getSchemaImpliesNft = (
  address: string,
): T.Task<O.Option<boolean>> =>
  pipe(
    getExistingOpenseaSchemaName(address),
    T.map(
      O.map((existingOpenseaSchemaName) =>
        checkSchemaImpliesNft(existingOpenseaSchemaName),
      ),
    ),
  );

export const getExistingCategory = (address: string) =>
  pipe(
    sqlT<{ category: string | null }[]>`
      SELECT category FROM contracts WHERE address = ${address}
    `,
    T.map(readOptionalFromFirstRow("category")),
  );
