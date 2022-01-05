import * as Blocks from "../blocks/blocks.js";
import { sqlT } from "../db.js";
import { flow, O, pipe, T } from "../fp.js";
import { LimitedTimeFrame, TimeFrame } from "../time_frames.js";

export const maxRank = 100;

export const getLastIncludedBlock = () =>
  pipe(
    sqlT<{ lastAnalyzedBlock: number | null }[]>`
      SELECT last_analyzed_block FROM analysis_state
      WHERE key = 'leaderboards'
    `,
    T.map(flow((rows) => rows[0]?.lastAnalyzedBlock, O.fromNullable)),
  );

export const setLastIncludedBlock = (blockNumber: number) =>
  pipe(
    sqlT`
      INSERT INTO analysis_state
        (key, last_analyzed_block)
      VALUES
        ('leaderboards', ${blockNumber})
      ON CONFLICT (key) DO UPDATE SET
        last_analyzed_block = excluded.last_analyzed_block
    `,
    T.map(() => undefined),
  );

export const setLastIncludedBlockIsLatest = () =>
  sqlT`
    INSERT INTO analysis_state
      (key, last_analyzed_block)
    SELECT 'leaderboards', MAX(number) FROM blocks
    ON CONFLICT (key) DO UPDATE SET
      last_analyzed_block = excluded.last_analyzed_block
  `;

const expireRecordsBefore = (
  timeFrame: LimitedTimeFrame,
  blockNumber: number,
) => sqlT`
  DELETE FROM burn_records
  WHERE block_number < ${blockNumber}
  AND time_frame = ${timeFrame}
`;

const expireRecordsOutsideLimitedTimeFrame = (timeFrame: LimitedTimeFrame) =>
  pipe(
    Blocks.getEarliestBlockInTimeFrame(timeFrame),
    T.chain((earliestIncludedBlock) =>
      expireRecordsBefore(timeFrame, earliestIncludedBlock),
    ),
  );

export const expireRecordsOutsideTimeFrame = (timeFrame: TimeFrame) =>
  pipe(
    timeFrame === "all"
      ? T.of(undefined)
      : expireRecordsOutsideLimitedTimeFrame(timeFrame),
  );

export const addRecordsFromBlockAndIncluding = (
  timeFrame: TimeFrame,
  blockNumber: number,
) =>
  sqlT`
    WITH new_records AS (
      SELECT number, base_fee_sum FROM blocks
      WHERE number >= ${blockNumber}
    )
    INSERT INTO burn_records
      (time_frame, block_number, base_fee_sum)
      SELECT ${timeFrame}, number, base_fee_sum FROM new_records
  `;

export const pruneRecordsBeyondRank = (
  timeFrame: TimeFrame,
  rank: number,
) => sqlT`
  DELETE FROM burn_records
  WHERE burn_record_id IN (
    SELECT burn_record_id FROM burn_records
    WHERE time_frame = ${timeFrame}
    ORDER BY base_fee_sum DESC
    OFFSET ${rank}
  )
`;

export const getBurnRecords = (timeFrame: TimeFrame) => sqlT<
  {
    blockNumber: number;
    baseFeeSum: number;
  }[]
>`
  SELECT block_number, base_fee_sum FROM burn_records
  WHERE time_frame = ${timeFrame}
  ORDER BY base_fee_sum DESC
`;

export const expireRecordsFromAndIncluding = (blockNumber: number) =>
  sqlT`
    DELETE FROM burn_records
    WHERE block_number >= ${blockNumber}
  `;
