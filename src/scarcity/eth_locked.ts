import * as DateFns from "date-fns";
import { sqlT } from "../db.js";
import { flow, O, pipe, T } from "../fp.js";

export type EthLocked = {
  timestamp: Date;
  ethLocked: number;
};

const ethLockedKey = "eth-locked";

export const getLastEthLocked = () =>
  pipe(
    sqlT<{ value: { timestamp: number; ethLocked: number } }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${ethLockedKey}
    `,
    T.map(
      flow(
        (rows) => rows[0],
        O.fromNullable,
        O.map((row) => ({
          timestamp: DateFns.fromUnixTime(row.value.timestamp),
          ethLocked: row.value.ethLocked,
        })),
      ),
    ),
  );
