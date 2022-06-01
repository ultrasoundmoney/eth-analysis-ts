import * as DateFns from "date-fns";
import { sqlT } from "../db.js";
import { flow, O, pipe, T } from "../fp.js";

export type EthLocked = {
  timestamp: Date;
  eth: number;
};

const ethInDefiCacheKey = "eth-in-defi";

export const getLastEthInDefi = () =>
  pipe(
    sqlT<{ value: { timestamp: number; eth: number } }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${ethInDefiCacheKey}
    `,
    T.map(
      flow(
        (rows) => rows[0],
        O.fromNullable,
        O.map((row) => ({
          timestamp: DateFns.fromUnixTime(row.value.timestamp),
          eth: row.value.eth,
        })),
      ),
    ),
  );
