import { sqlTVoid } from "../db.js";
import { pipe, T } from "../fp.js";
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
