import { sqlT, sqlTVoid } from "../db.js";
import { A, O, pipe, T, TAlt, TE } from "../fp.js";
import * as Log from "../log.js";
import * as Contracts from "./contracts.js";
import * as CopyFromSimilar from "./metadata/copy_from_similar.js";
import * as MetadataTwitter from "./metadata/twitter.js";

export const setTwitterHandle = (address: string, handle: O.Option<string>) =>
  pipe(
    Contracts.setSimpleTextColumn(
      "manual_twitter_handle",
      address,
      O.toNullable(handle),
    ),
    T.chain(() => Contracts.updatePreferredMetadata(address)),
    T.chain(() =>
      pipe(
        handle,
        O.match(
          () => T.of(undefined),
          () =>
            pipe(
              MetadataTwitter.addTwitterMetadata(address),
              TE.chainTaskK(() => Contracts.updatePreferredMetadata(address)),
              TE.match(
                (e) => {
                  Log.error("failed to update twitter metadata", e);
                },
                () => undefined,
              ),
            ),
        ),
      ),
    ),
  );

export const setName = (address: string, name: string): T.Task<void> =>
  pipe(
    TAlt.when(
      name.toLowerCase().startsWith("mev bot:"),
      pipe(
        Contracts.setIsBot(address, true),
        T.apSecond(Contracts.setSimpleTextColumn("category", address, "mev")),
      ),
    ),
    T.apSecond(
      pipe(
        name.split(":")[0],
        (nameStartsWith) =>
          name.includes(":") && nameStartsWith.length > 0
            ? O.some(nameStartsWith)
            : O.none,
        O.match(
          () => T.of(undefined),
          (nameStartsWith) =>
            CopyFromSimilar.addMetadataFromSimilar(address, nameStartsWith),
        ),
      ),
    ),
    T.apSecond(Contracts.setSimpleTextColumn("manual_name", address, name)),
    T.chain(() => Contracts.updatePreferredMetadata(address)),
  );

export const setCategory = (address: string, category: string) =>
  pipe(
    Contracts.setSimpleTextColumn(
      "manual_category",
      address,
      category === "" ? null : category,
    ),
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
      SELECT
        address,
        last_manually_verified,
        opensea_contract_last_fetch
      FROM contracts
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
