import * as Db from "../db.js";
import * as Log from "../log.js";
import { pipe, T } from "../fp.js";
import * as CopyFromSimilar from "../contracts/metadata/copy_from_similar.js";

Log.info(
  "copying metadata for manually named contracts with similar names to other contracts",
);

await pipe(
  Db.sqlT<{ address: string; manualName: string }[]>`
    SELECT address, manual_name FROM contracts
    WHERE manual_name ILIKE '%:%'
  `,
  T.chainFirstIOK((rows) => Log.infoIO(`got ${rows.length} contracts to copy`)),
  T.chain(
    T.traverseSeqArray(({ address, manualName }) =>
      pipe(
        Log.debugIO(`copying for manually named: ${manualName}`),
        T.fromIO,
        T.chain(() =>
          CopyFromSimilar.addMetadataFromSimilar(
            address,
            manualName.split(":")[0],
          ),
        ),
      ),
    ),
  ),
)();

Log.info("done copying metadata");
