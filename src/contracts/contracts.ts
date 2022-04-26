import A from "fp-ts/lib/Array.js";
import * as Db from "../db.js";
import { sql, sqlT, sqlTVoid } from "../db.js";
import { flow, NEA, O, pipe, T, TO, TOAlt } from "../fp.js";
import * as OpenSea from "../opensea.js";

export const storeContracts = flow(
  NEA.map((address: string) => ({ address })),
  (insertables) =>
    sqlTVoid`
      INSERT INTO contracts
        ${sql(insertables)}
      ON CONFLICT DO NOTHING
    `,
);

export type SimpleTextColumn =
  | "category"
  | "coingecko_categories"
  | "coingecko_image_url"
  | "coingecko_name"
  | "coingecko_twitter_handle"
  | "defi_llama_category"
  | "defi_llama_twitter_handle"
  | "etherscan_name_tag"
  | "etherscan_name_token"
  | "image_url"
  | "manual_category"
  | "manual_name"
  | "manual_twitter_handle"
  | "name"
  | "opensea_image_url"
  | "opensea_name"
  | "opensea_schema_name"
  | "opensea_twitter_handle"
  | "twitter_description"
  | "twitter_handle"
  | "twitter_id"
  | "twitter_image_url"
  | "twitter_name"
  | "web3_name";

export const setSimpleTextColumn = (
  columnName: SimpleTextColumn,
  address: string,
  value: string | null,
) =>
  pipe(
    value,
    TO.fromPredicate((v) => typeof v === "string" && v.length > 0),
    TO.chainTaskK(
      (value) =>
        sqlTVoid`
          UPDATE contracts
          SET
            ${sql({ [columnName]: value })}
          WHERE
            address = ${address}
        `,
    ),
    T.map((): void => undefined),
  );

export type SimpleBooleanColumn =
  | "supports_erc_721"
  | "supports_erc_1155"
  | "force_metadata_fetch";

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
  category: string | null;
  coingeckoImageUrl: string | null;
  coingeckoName: string | null;
  coingeckoTwitterHandle: string | null;
  defiLlamaCategory: string | null;
  defiLlamaTwitterHandle: string | null;
  etherscanNameTag: string | null;
  etherscanNameToken: string | null;
  imageUrl: string | null;
  manualCategory: string | null;
  manualName: string | null;
  manualTwitterHandle: string | null;
  name: string | null;
  openseaImageUrl: string | null;
  openseaName: string | null;
  openseaSchemaName: string | null;
  openseaTwitterHandle: string | null;
  supportsErc_1155: boolean | null;
  supportsErc_721: boolean | null;
  twitterHandle: string | null;
  twitterImageUrl: string | null;
  twitterName: string | null;
  web3Name: string | null;
};

const getOpenseaName = (metadata: MetadataComponents): string | null => {
  if (metadata.openseaName === "Unidentified contract") {
    return null;
  }

  return metadata.openseaName;
};

const getPreferredName = (metadata: MetadataComponents): string | null => {
  if (metadata.manualName) {
    return metadata.manualName;
  }

  if (metadata.web3Name) {
    return metadata.web3Name;
  }

  const category = getPreferredCategory(metadata);
  const openseaName = getOpenseaName(metadata);
  if (category === "nft" && typeof openseaName === "string") {
    return openseaName;
  }

  return (
    metadata.etherscanNameTag ||
    metadata.etherscanNameToken ||
    openseaName ||
    metadata.twitterName ||
    metadata.coingeckoName ||
    metadata.name
  );
};

const getPreferredCategory = (metadata: MetadataComponents): string | null =>
  metadata.manualCategory ||
  (OpenSea.checkSchemaImpliesNft(metadata.openseaSchemaName) ? "nft" : null) ||
  (metadata.defiLlamaCategory !== null ? "defi" : null) ||
  (metadata.supportsErc_721 === true ? "nft" : null) ||
  (metadata.supportsErc_1155 === true ? "nft" : null) ||
  metadata.category;

const getPreferredTwitterHandle = (
  metadata: MetadataComponents,
): string | null =>
  metadata.manualTwitterHandle ||
  metadata.openseaTwitterHandle ||
  metadata.coingeckoTwitterHandle ||
  metadata.defiLlamaTwitterHandle ||
  metadata.twitterHandle;

const getPreferredImageUrl = (metadata: MetadataComponents): string | null =>
  metadata.twitterImageUrl ||
  metadata.openseaImageUrl ||
  metadata.coingeckoImageUrl ||
  metadata.imageUrl;

export const updatePreferredMetadata = (address: string) =>
  pipe(
    Db.sqlT<MetadataComponents[]>`
      SELECT
        category,
        coingecko_image_url,
        coingecko_name,
        coingecko_twitter_handle,
        defi_llama_category,
        defi_llama_twitter_handle,
        etherscan_name_tag,
        etherscan_name_token,
        image_url,
        manual_category,
        manual_name,
        manual_twitter_handle,
        name,
        opensea_image_url,
        opensea_name,
        opensea_schema_name,
        opensea_twitter_handle,
        supports_erc_1155,
        supports_erc_721,
        twitter_handle,
        twitter_image_url,
        twitter_name,
        web3_name
      FROM contracts
      WHERE address = ${address}
    `,
    T.map(O.fromNullableK((rows) => rows[0])),
    TO.chainTaskK(
      (metadataComponents) =>
        Db.sqlTVoid`
          UPDATE contracts
          SET
            name = ${getPreferredName(metadataComponents)},
            category = ${getPreferredCategory(metadataComponents)},
            twitter_handle = ${getPreferredTwitterHandle(metadataComponents)},
            image_url = ${getPreferredImageUrl(metadataComponents)}
          WHERE address = ${address}
        `,
    ),
    TOAlt.doOrSkipVoid,
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

export const setContractsMinedAt = (
  addresses: NEA.NonEmptyArray<string>,
  blockNumber: number,
  date: Date,
) =>
  sqlTVoid`
    UPDATE contracts
    SET
      mined_at = ${date},
      mined_at_block = ${blockNumber}
    WHERE address IN (${addresses})
  `;

export const setContractMinedAtNull = async (address: string) => {
  await sql`
    UPDATE contracts
    SET
      mined_at = NULL,
      mined_at_block = NULL
    WHERE address = ${address}
  `;
};

const deleteContractBaseFeeSums = (
  addresses: NEA.NonEmptyArray<string>,
) => Db.sqlTVoid`
  DELETE FROM contract_base_fee_sums
  WHERE contract_address IN (${addresses})
`;

// Clean up sums when cleaning up contracts.
export const deleteContractsMinedAt = (blockNumber: number) =>
  pipe(
    Db.sqlT<{ address: string }[]>`
      SELECT address FROM contracts
      WHERE mined_at_block = ${blockNumber}
    `,
    T.map(
      flow(
        A.map((row) => row.address),
        NEA.fromArray,
      ),
    ),
    TO.chainTaskK((addresses) => deleteContractBaseFeeSums(addresses)),
    T.apSecond(Db.sqlTVoid`
      DELETE FROM contracts
      WHERE mined_at_block = ${blockNumber}
    `),
  );

export const getTwitterHandle = (address: string) =>
  pipe(
    () => sql<{ twitterHandle: string | null }[]>`
      SELECT twitter_handle FROM contracts
      WHERE address = ${address}
    `,
    T.map(flow((rows) => rows[0]?.twitterHandle, O.fromNullable)),
  );

export const getAddressesToRefetch = (): T.Task<Set<string>> =>
  pipe(
    () => sql<{ address: string }[]>`
      SELECT address FROM contracts
      WHERE force_metadata_fetch = TRUE
    `,
    T.map(A.reduce(new Set(), (set, row) => set.add(row.address))),
  );

export const getName = (address: string) =>
  pipe(
    sqlT<{ name: string | null }[]>`
      SELECT name FROM contracts
      WHERE address = ${address}
    `,
    T.map(flow((rows) => rows[0]?.name, O.fromNullable)),
  );
