import * as DateFns from "date-fns";
import * as Db from "./db.js";
import { ethFromGwei } from "./eth_units.js";
import { A, pipe, T } from "./fp.js";
import * as Log from "./log.js";

export const genesisTimestamp = DateFns.fromUnixTime(1606824023);

const getTimestampFromSlot = (slot: number) =>
  DateFns.addSeconds(genesisTimestamp, slot * 12);

const getDayTimestampFromSlot = (slot: number) =>
  pipe(slot, getTimestampFromSlot, DateFns.startOfDay);

export const storeValidatorSumForDay = (
  slot: number,
  validatorBalanceSum: bigint,
) =>
  pipe(
    getDayTimestampFromSlot(slot),
    T.of,
    T.chainFirstIOK((dateAt) =>
      Log.debugIO(
        `storing validator sum for day, slot: ${slot}, date_at: ${dateAt.toISOString()}, gwei: ${validatorBalanceSum}`,
      ),
    ),
    T.chain(
      (dateAt) =>
        Db.sqlTVoid`
            INSERT INTO eth_in_validators
              ${Db.values({
                date_at: dateAt,
                gwei: validatorBalanceSum,
              })}
            ON CONFLICT (date_at) DO NOTHING
          `,
    ),
  );

export const getEthInValidatorsByDay = () =>
  pipe(
    Db.sqlT<{ dateAt: Date; gwei: string }[]>`
      SELECT date_at, gwei FROM eth_in_validators
    `,
    T.map(
      A.map((row) => ({
        t: DateFns.getUnixTime(row.dateAt),
        v: pipe(row.gwei, Number, ethFromGwei),
      })),
    ),
  );
