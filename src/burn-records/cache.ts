import { sql, sqlT, sqlTVoid } from "../db.js";
import { flow, O, OAlt, pipe, T, TAlt } from "../fp.js";
import { TimeFrameNext } from "../time_frames.js";
import * as BurnRecords from "./burn_records.js";

const maxRecordCount = 10;

export const burnRecordsCacheKey = "burn-records-cache";

export type BurnRecordsCache = {
  number: number;
  records: Record<TimeFrameNext, BurnRecords.BurnRecord[]>;
};

export const updateRecordsCache = (blockNumber: number) =>
  pipe(
    TAlt.seqSPar({
      m5: BurnRecords.getBurnRecords("m5", maxRecordCount),
      h1: BurnRecords.getBurnRecords("h1", maxRecordCount),
      d1: BurnRecords.getBurnRecords("d1", maxRecordCount),
      d7: BurnRecords.getBurnRecords("d7", maxRecordCount),
      d30: BurnRecords.getBurnRecords("d30", maxRecordCount),
      since_merge: BurnRecords.getBurnRecords("since_merge", maxRecordCount),
      since_burn: BurnRecords.getBurnRecords("since_burn", maxRecordCount),
      all: BurnRecords.getBurnRecords("since_burn", maxRecordCount),
    }),
    T.chain(
      (burnRecords) =>
        sqlTVoid`
          INSERT INTO key_value_store (
            key, value
          ) VALUES (
            ${burnRecordsCacheKey},
            ${sql.json({
              number: blockNumber,
              records: burnRecords,
            })}
          ) ON CONFLICT (key) DO UPDATE SET
            value = excluded.value
        `,
    ),
  );

export const getRecordsCache = () =>
  pipe(
    sqlT<{ value: BurnRecordsCache }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${burnRecordsCacheKey}
    `,
    T.map(
      flow(
        (rows) => rows[0]?.value,
        O.fromNullable,
        OAlt.getOrThrow("tried to get records cache before updating records"),
      ),
    ),
  );
