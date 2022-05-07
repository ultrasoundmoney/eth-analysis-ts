import { Num, Ordering, pipe } from "./fp.js";
import * as DateFns from "date-fns";

export const genesisTimestamp = DateFns.fromUnixTime(1606824023);

export const getTimestampFromSlot = (slot: number) =>
  DateFns.addSeconds(genesisTimestamp, slot * 12);

export const getStartOfDayFromSlot = (slot: number) =>
  pipe(slot, getTimestampFromSlot, (dt) => {
    dt.setUTCHours(0, 0, 0, 0);
    return dt;
  });

export const getDayOfMonthFromSlot = (slot: number) =>
  pipe(slot, getTimestampFromSlot, (dt) => dt.getUTCDate());

export const getIsFirstOfDaySlot = (slot: number) =>
  pipe(
    Num.Ord.compare(slot, 0),
    Ordering.match(
      () => {
        throw new Error("slots should be > 0");
      },
      () => true,
      () =>
        pipe(
          slot,
          getDayOfMonthFromSlot,
          (dayOfMonth) => dayOfMonth !== getDayOfMonthFromSlot(slot - 1),
        ),
    ),
  );
