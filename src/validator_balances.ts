import * as DateFns from "date-fns";
import * as Db from "./db.js";
import { A, pipe, T } from "./fp.js";

const genesisTimestamp = DateFns.fromUnixTime(1606824023);

const getTimestampFromSlot = (slot: number) =>
  DateFns.addSeconds(genesisTimestamp, slot * 12);

export const storeValidatorSumForDay = (
  slot: number,
  validatorBalanceSum: bigint,
) =>
  pipe(
    Db.sqlTVoid`
      INSERT INTO eth_in_validators
        ${Db.values({
          date_at: pipe(slot, getTimestampFromSlot, DateFns.startOfDay),
          gwei: validatorBalanceSum,
        })}
      ON CONFLICT (date_at) DO NOTHING
    `,
  );

export const getEthInValidatorsByDay = () =>
  pipe(
    Db.sqlT<{ dateAt: Date; gwei: string }[]>`
      SELECT date_at, gwei FROM eth_in_validators
    `,
    T.map(
      A.map((row) => ({
        t: DateFns.getUnixTime(row.dateAt),
        v: Number(row.gwei),
      })),
    ),
  );
