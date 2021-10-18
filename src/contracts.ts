import * as ContractsMetadata from "./contracts_metadata.js";
import * as Etherscan from "./etherscan.js";
import * as Log from "./log.js";
import * as T from "fp-ts/lib/Task.js";
import * as Twitter from "./twitter.js";
import A from "fp-ts/lib/Array.js";
import { O, TE } from "./fp.js";
import { constant, pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { web3 } from "./eth_node.js";

export const getOnChainName = async (
  address: string,
): Promise<string | undefined> => {
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

  if (abi === undefined) {
    return undefined;
  }

  const contract = new web3!.eth.Contract(abi, address);
  const hasNameMethod = contract.methods["name"] !== undefined;

  if (!hasNameMethod) {
    return undefined;
  }

  return contract.methods.name().call();
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

export type SimpleColumn =
  | "category"
  | "defi_llama_category"
  | "defi_llama_twitter_handle"
  | "etherscan_name_tag"
  | "etherscan_name_token"
  | "manual_category"
  | "manual_name"
  | "manual_twitter_handle"
  | "name"
  | "on_chain_name"
  | "opensea_category"
  | "opensea_image_url"
  | "opensea_name"
  | "opensea_twitter_handle"
  | "twitter_description"
  | "twitter_image_url"
  | "twitter_name";

export const setSimpleColumn = (
  columnName: SimpleColumn,
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

type MetadataComponents = {
  onChainName: string | null;
  etherscanNameTag: string | null;
  etherscanNameToken: string | null;
  openseaName: string | null;
  openseaImageUrl: string | null;
  openseaTwitterHandle: string | null;
  openseaCategory: string | null;
  defiLlamaTwitterHandle: string | null;
  defiLlamaCategory: string | null;
  manualName: string | null;
  manualTwitterHandle: string | null;
  manualCategory: string | null;
  twitterImageUrl: string | null;
};

const getPreferredName = (metadata: MetadataComponents): string | null =>
  metadata.manualName ||
  metadata.etherscanNameTag ||
  metadata.etherscanNameToken ||
  metadata.openseaName ||
  null;

const getPreferredCategory = (metadata: MetadataComponents): string | null =>
  metadata.manualCategory ||
  metadata.openseaCategory ||
  (metadata.defiLlamaCategory !== null ? "defi" : null) ||
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
        on_chain_name,
        etherscan_name_tag,
        etherscan_name_token,
        opensea_name,
        opensea_image_url,
        opensea_twitter_handle,
        opensea_category,
        defi_llama_twitter_handle,
        defi_llama_category,
        manual_name,
        manual_twitter_handle,
        manual_category,
        twitter_image_url
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
    setSimpleColumn("manual_twitter_handle", address, handle),
    T.chain(() => () => Twitter.getProfileByHandle(handle)),
    T.chain(() => () => ContractsMetadata.addTwitterMetadata(address, handle)),
    T.chain(() => updatePreferredMetadata(address)),
  );

export const setName = (address: string, name: string): T.Task<void> =>
  pipe(
    setSimpleColumn("manual_name", address, name),
    T.chain(() => updatePreferredMetadata(address)),
  );

export const setCategory = (address: string, category: string): T.Task<void> =>
  pipe(
    setSimpleColumn("manual_category", address, category),
    T.chain(() => updatePreferredMetadata(address)),
  );
