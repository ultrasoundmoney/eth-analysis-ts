import { sqlT, sqlTVoid } from "../db.js";
import { A, pipe, T } from "../fp.js";
import * as Contracts from "./contracts.js";
import * as ContractsMetadata from "./crawl_metadata.js";

export const setTwitterHandle = (
  address: string,
  handle: string,
): T.Task<void> =>
  pipe(
    Contracts.setSimpleTextColumn("manual_twitter_handle", address, handle),
    T.chain(() => () => ContractsMetadata.addTwitterMetadata(address)),
    T.chain(() => Contracts.updatePreferredMetadata(address)),
  );

export const setName = (address: string, name: string): T.Task<void> =>
  pipe(
    Contracts.setSimpleTextColumn("manual_name", address, name),
    T.chain(() => Contracts.updatePreferredMetadata(address)),
  );

export const setCategory = (address: string, category: string): T.Task<void> =>
  pipe(
    Contracts.setSimpleTextColumn("manual_category", address, category),
    T.chain(() => Contracts.updatePreferredMetadata(address)),
  );

export const setLastManuallyVerified = (address: string) =>
  sqlTVoid`
    UPDATE contracts
    SET
      last_manually_verified = ${new Date()}
    WHERE
      address = ${address}
  `;

type RawMetadataFreshness = {
  address: string;
  openseaContractLastFetch: Date | null;
  lastManuallyVerified: Date | null;
};
type MetadataFreshness = {
  openseaContractLastFetch: Date | null;
  lastManuallyVerified: Date | null;
};
type MetadataFreshnessMap = Map<string, MetadataFreshness>;

export const getMetadataFreshness = (
  addresses: string[],
): T.Task<MetadataFreshnessMap> =>
  pipe(
    sqlT<RawMetadataFreshness[]>`
      SELECT address, opensea_contract_last_fetch, last_manually_verified FROM contracts
      WHERE address IN (${addresses})
    `,
    T.map(
      A.reduce(
        new Map<string, MetadataFreshness>(),
        (map, { address, openseaContractLastFetch, lastManuallyVerified }) =>
          map.set(address, { openseaContractLastFetch, lastManuallyVerified }),
      ),
    ),
  );
