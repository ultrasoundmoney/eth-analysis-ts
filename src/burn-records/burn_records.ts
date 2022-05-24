import * as Blocks from "../blocks/blocks.js";
import * as Db from "../db.js";
import { flow, O, pipe, T } from "../fp.js";
import { LimitedTimeFrameNext, TimeFrameNext } from "../time_frames.js";

export const maxRank = 10;

const analysisStateKey = "burn-records";

export const getLastIncludedBlock = () =>
  pipe(
    Db.sqlT<{ last: number | null }[]>`
      SELECT last FROM analysis_state
      WHERE key = ${analysisStateKey}
    `,
    T.map(flow((rows) => rows[0]?.last, O.fromNullable)),
  );

export const setLastIncludedBlock = (blockNumber: number) =>
  pipe(
    Db.sqlT`
      INSERT INTO analysis_state
        (key, last)
      VALUES
        (${analysisStateKey}, ${blockNumber})
      ON CONFLICT (key) DO UPDATE SET
        last = excluded.last
    `,
    T.map(() => undefined),
  );

export const setLastIncludedBlockIsLatest = () =>
  Db.sqlTVoid`
    INSERT INTO analysis_state
      (key, last)
    SELECT ${analysisStateKey}, MAX(number) FROM blocks
    ON CONFLICT (key) DO UPDATE SET
      last = excluded.last
  `;

const expireRecordsBefore = (
  timeFrame: LimitedTimeFrameNext,
  blockNumber: number,
) => Db.sqlT`
  DELETE FROM burn_records
  WHERE block_number < ${blockNumber}
  AND time_frame = ${timeFrame}
`;

const expireRecordsOutsideLimitedTimeFrame = (
  timeFrame: LimitedTimeFrameNext,
) =>
  pipe(
    Blocks.getEarliestBlockInTimeFrame(timeFrame),
    T.chain((earliestIncludedBlock) =>
      expireRecordsBefore(timeFrame, earliestIncludedBlock),
    ),
  );

export const expireRecordsOutsideTimeFrame = (timeFrame: TimeFrameNext) =>
  timeFrame === "all"
    ? T.of(undefined)
    : expireRecordsOutsideLimitedTimeFrame(timeFrame);

export const addRecordsFromBlockAndIncluding = (
  timeFrame: TimeFrameNext,
  blockNumber: number,
) =>
  Db.sqlT`
    WITH new_records AS (
      SELECT number, base_fee_sum FROM blocks
      WHERE number >= ${blockNumber}
      ORDER BY base_fee_sum DESC
      LIMIT ${maxRank}
    )
    INSERT INTO burn_records
      (time_frame, block_number, base_fee_sum)
      SELECT ${timeFrame}, number, base_fee_sum FROM new_records
    ON CONFLICT DO NOTHING
  `;

export const pruneRecordsBeyondRank = (
  timeFrame: TimeFrameNext,
  rank: number,
) => Db.sqlT`
  DELETE FROM burn_records
  WHERE block_number IN (
    SELECT block_number FROM burn_records
    WHERE time_frame = ${timeFrame}
    ORDER BY base_fee_sum DESC
    OFFSET ${rank}
  )
  AND time_frame = ${timeFrame}
`;

export type BurnRecord = {
  blockNumber: number;
  baseFeeSum: number;
  minedAt: Date;
};

export const getBurnRecords = (
  timeFrame: TimeFrameNext,
  count = 100,
): T.Task<BurnRecord[]> => Db.sqlT<BurnRecord[]>`
  SELECT
    block_number,
    burn_records.base_fee_sum,
    mined_at
  FROM burn_records
  JOIN blocks ON number = block_number
  WHERE time_frame = ${timeFrame}
  ORDER BY base_fee_sum DESC
  LIMIT ${count}
`;

export const expireRecordsFromAndIncluding = (blockNumber: number) =>
  Db.sqlT`
    DELETE FROM burn_records
    WHERE block_number >= ${blockNumber}
  `;
