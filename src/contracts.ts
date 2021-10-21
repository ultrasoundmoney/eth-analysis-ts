import * as ContractsMetadata from "./contracts_metadata.js";
import * as OpenSea from "./opensea.js";
import * as T from "fp-ts/lib/Task.js";
import * as Twitter from "./twitter.js";
import A from "fp-ts/lib/Array.js";
import { O } from "./fp.js";
import { constant, pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";

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

export type SimpleTextColumn =
  | "category"
  | "defi_llama_category"
  | "defi_llama_twitter_handle"
  | "etherscan_name_tag"
  | "etherscan_name_token"
  | "manual_category"
  | "manual_name"
  | "manual_twitter_handle"
  | "name"
  | "opensea_image_url"
  | "opensea_name"
  | "opensea_schema_name"
  | "opensea_twitter_handle"
  | "twitter_description"
  | "twitter_image_url"
  | "twitter_name"
  | "web3_name";

export const setSimpleTextColumn = (
  columnName: SimpleTextColumn,
  address: string,
  value: string | null,
): T.Task<void> =>
  pipe(
    () => sql`
      UPDATE contracts
      SET
        ${sql({ [columnName]: value })}
      WHERE
        address = ${address}
    `,
    T.map(() => undefined),
  );

export type SimpleBooleanColumn = "supports_erc_721" | "supports_erc_1155";

export const setSimpleBooleanColumn = (
  columnName: SimpleBooleanColumn,
  address: string,
  value: boolean | null,
): T.Task<void> =>
  pipe(
    () => sql`
      UPDATE contracts
      SET
        ${sql({ [columnName]: value })}
      WHERE
        address = ${address}
    `,
    T.map(() => undefined),
  );

type MetadataComponents = {
  defiLlamaCategory: string | null;
  defiLlamaTwitterHandle: string | null;
  etherscanNameTag: string | null;
  etherscanNameToken: string | null;
  manualCategory: string | null;
  manualName: string | null;
  manualTwitterHandle: string | null;
  web3Name: string | null;
  openseaImageUrl: string | null;
  openseaName: string | null;
  openseaSchemaName: string | null;
  openseaTwitterHandle: string | null;
  supportsErc_721: boolean | null;
  supportsErc_1155: boolean | null;
  twitterImageUrl: string | null;
};

const getPreferredName = (metadata: MetadataComponents): string | null => {
  if (metadata.manualName) {
    return metadata.manualName;
  }

  if (metadata.web3Name) {
    return metadata.web3Name;
  }

  const category = getPreferredCategory(metadata);
  if (category === "nft" && typeof metadata.openseaName === "string") {
    return metadata.openseaName;
  }

  return (
    metadata.etherscanNameTag ||
    metadata.etherscanNameToken ||
    metadata.openseaName ||
    null
  );
};

const getPreferredCategory = (metadata: MetadataComponents): string | null =>
  metadata.manualCategory ||
  (OpenSea.checkSchemaImpliesNft(metadata.openseaSchemaName) ? "nft" : null) ||
  (metadata.defiLlamaCategory !== null ? "defi" : null) ||
  (metadata.supportsErc_721 === true ? "nft" : null) ||
  (metadata.supportsErc_1155 === true ? "nft" : null) ||
  null;

const getPreferredTwitterHandle = (
  metadata: MetadataComponents,
): string | null =>
  metadata.manualTwitterHandle ||
  metadata.openseaTwitterHandle ||
  metadata.defiLlamaTwitterHandle ||
  null;

const getPreferredImageUrl = (metadata: MetadataComponents): string | null =>
  metadata.twitterImageUrl || metadata.openseaImageUrl || null;

export const updatePreferredMetadata = (address: string): T.Task<void> =>
  pipe(
    () => sql<MetadataComponents[]>`
      SELECT
        etherscan_name_tag,
        etherscan_name_token,
        opensea_name,
        opensea_image_url,
        opensea_twitter_handle,
        opensea_schema_name,
        defi_llama_twitter_handle,
        defi_llama_category,
        manual_name,
        manual_twitter_handle,
        manual_category,
        twitter_image_url,
        supports_erc_721,
        supports_erc_1155,
        web3_name
      FROM contracts
      WHERE address = ${address}
    `,
    T.map((rows) => rows[0]),
    T.map(O.fromNullable),
    T.chain(
      O.match(constant(T.of(undefined)), (metadataComponents) =>
        pipe(
          () => sql`
            UPDATE contracts
            SET
              name = ${getPreferredName(metadataComponents)},
              category = ${getPreferredCategory(metadataComponents)},
              twitter_handle = ${getPreferredTwitterHandle(metadataComponents)},
              image_url = ${getPreferredImageUrl(metadataComponents)}
            WHERE address = ${address}
          `,
          T.map(() => undefined),
        ),
      ),
    ),
  );

export const setTwitterHandle = (
  address: string,
  handle: string,
): T.Task<void> =>
  pipe(
    setSimpleTextColumn("manual_twitter_handle", address, handle),
    T.chain(() => () => Twitter.getProfileByHandle(handle)),
    T.chain(() => () => ContractsMetadata.addTwitterMetadata(address, handle)),
    T.chain(() => updatePreferredMetadata(address)),
  );

export const setName = (address: string, name: string): T.Task<void> =>
  pipe(
    setSimpleTextColumn("manual_name", address, name),
    T.chain(() => updatePreferredMetadata(address)),
  );

export const setCategory = (address: string, category: string): T.Task<void> =>
  pipe(
    setSimpleTextColumn("manual_category", address, category),
    T.chain(() => updatePreferredMetadata(address)),
  );

export const setLastLeaderboardEntryToNow = async (
  addresses: string[],
): Promise<void> => {
  if (addresses.length === 0) {
    return;
  }

  await sql`
    UPDATE contracts
    SET last_leaderboard_entry = NOW()
    WHERE address IN (${addresses})
  `;
};
