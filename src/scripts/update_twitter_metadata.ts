import * as Db from "../db.js";
import * as Log from "../log.js";
import { E, pipe, T, TE } from "../fp.js";
import * as MetadataTwitter from "../contracts/metadata/twitter.js";
import * as Twitter from "../twitter.js";
import * as Fetch from "../fetch.js";
import * as Queues from "../queues.js";

Log.info("adding twitter metadata for manually named contracts");

await pipe(
  Db.sqlT<{ address: string; name: string; twitterHandle: string }[]>`
    SELECT address, name FROM contracts
    WHERE manual_name ILIKE '%:%'
  `,
  T.chainFirstIOK((rows) =>
    Log.infoIO(`got ${rows.length} contracts to update`),
  ),
  T.chain(
    TE.traverseSeqArray(({ address, name }) =>
      pipe(
        Log.debugIO(`fetching twitter metadata for: ${name}`),
        T.fromIO,
        T.chain(() =>
          pipe(
            MetadataTwitter.addTwitterMetadata(address),
            TE.match(
              (e) => {
                if (
                  e instanceof Twitter.InvalidHandleError ||
                  e instanceof Twitter.ProfileNotFoundError ||
                  (e instanceof Fetch.BadResponseError && e.status === 429)
                ) {
                  Log.warn("failed to add twitter metadata", e);
                  return E.right(undefined);
                }

                if (e instanceof MetadataTwitter.NoKnownTwitterHandleError) {
                  Log.debug(`no known twitter handle for contract ${address}`);
                  return E.right(undefined);
                }

                if (e instanceof Queues.TimeoutError) {
                  Log.warn(
                    `twitter metadata request timed out for contract ${address}`,
                  );
                  return E.right(undefined);
                }

                return E.left(e);
              },
              () => E.right(undefined),
            ),
          ),
        ),
      ),
    ),
  ),
  TE.match(
    (e) => Log.error("failed to update twitter metadata", e),
    () => Log.info("done updating twitter metadata"),
  ),
)();
