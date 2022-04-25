import { MapS, O, pipe } from "../../fp.js";
import * as DateFns from "date-fns";

export const getLastAttempt = (retryMap: Map<string, Date>, address: string) =>
  pipe(
    retryMap,
    MapS.lookup(address),
    O.map(
      (lastAttempted) =>
        DateFns.differenceInHours(new Date(), lastAttempted) < 6,
    ),
  );

export const getShouldRetry = (
  retryMap: Map<string, Date>,
  address: string,
  forceRefetch: boolean,
) =>
  forceRefetch ||
  pipe(
    retryMap,
    MapS.lookup(address),
    O.map(
      (lastAttempted) =>
        DateFns.differenceInHours(new Date(), lastAttempted) < 6,
    ),
    O.getOrElseW(() => true),
  );
