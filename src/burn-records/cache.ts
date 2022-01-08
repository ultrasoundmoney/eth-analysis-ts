import { sql, sqlNotifyT, sqlT } from "../db.js";
import { pipe, T, TAlt } from "../fp.js";
import { TimeFrameNext } from "../time_frames.js";
import * as BurnRecords from "./burn_records.js";

export const burnRecordsCacheKey = "burn-records-cache";

export type BurnRecordsCache = {
  number: number;
  records: Record<TimeFrameNext, BurnRecords.BurnRecord[]>;
};

export const updateRecordsCache = (blockNumber: number) =>
  pipe(
    TAlt.seqSParT({
      m5: BurnRecords.getBurnRecords("5m"),
      h1: BurnRecords.getBurnRecords("1h"),
      d1: BurnRecords.getBurnRecords("24h"),
      d7: BurnRecords.getBurnRecords("7d"),
      d30: BurnRecords.getBurnRecords("30d"),
      all: BurnRecords.getBurnRecords("all"),
    }),
    T.chain(
      (burnRecords) =>
        sqlT`
          INSERT INTO key_value_store (
            key, value
          ) VALUES (
            ${burnRecordsCacheKey},
            ${sql.json({
              number: blockNumber,
              records: burnRecords,
            })}
          ) ON CONFLICT key DO UPDATE SET
            value = excluded.value
        `,
    ),
    T.chain(() => sqlNotifyT("cache-update", burnRecordsCacheKey)),
    T.map(() => undefined),
  );

export const getRecordsCache = () =>
  pipe(
    sqlT<BurnRecordsCache[]>`
      SELECT value FROM key_value_store
      WHERE key = ${burnRecordsCacheKey}
    `,
    T.map((rows) => rows[0]),
  );
