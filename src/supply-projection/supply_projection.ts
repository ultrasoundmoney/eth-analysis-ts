import * as DateFns from "date-fns";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import * as BeaconBalances from "../beacon_balances.js";
import * as BeaconIssuance from "../beacon_issuance.js";
import * as Duration from "../duration.js";
import { ethFromGwei } from "../eth_units.js";
import { A, MapN, O, pipe, T, TE } from "../fp.js";
import * as Glassnode from "../glassnode.js";
import * as Log from "../log.js";
import { queueOnQueue } from "../queues.js";
import { UnixTimestamp } from "../time.js";

// Update this module to store results periodically in our DB.
// Have serving services pull them out and serve only.

const inputsCache = new QuickLRU<"inputs", string>({
  maxSize: 1,
  maxAge: Duration.millisFromHours(4),
});

const inputsKey = "inputs";

const inputsQueue = new PQueue({
  concurrency: 1,
});

const getCachedInputs = () => pipe(inputsCache.get(inputsKey), O.fromNullable);

const addBeaconIssuanceToSupply = (
  validatorBalancesByDay: { t: UnixTimestamp; v: number }[],
  supplyData: Glassnode.SupplyData,
) =>
  pipe(
    validatorBalancesByDay,
    A.map((dataPoint) => [dataPoint.t, dataPoint.v] as [number, number]),
    (entries) => new Map(entries),
    (map) =>
      pipe(
        supplyData,
        A.map((dataPoint) =>
          pipe(
            map,
            MapN.lookup(dataPoint.t),
            O.match(
              () => dataPoint,
              (validatorBalance) => ({
                t: dataPoint.t,
                v: dataPoint.v + ethFromGwei(validatorBalance),
              }),
            ),
          ),
        ),
      ),
  );

const getFreshInputs = () =>
  pipe(
    TE.Do,
    TE.apS("inContractsByDay", Glassnode.getLockedEthData()),
    TE.apSW("stakedData", Glassnode.getStakedData()),
    TE.apSW(
      "inBeaconValidatorsByDay",
      pipe(
        BeaconBalances.getValidatorBalancesByDay(),
        T.map(
          A.map((row) => ({
            t: DateFns.getUnixTime(row.timestamp),
            v: pipe(row.gwei, Number, ethFromGwei),
          })),
        ),
        TE.fromTask,
      ),
    ),
    TE.apSW(
      "beaconIssuanceByDay",
      pipe(
        BeaconIssuance.getIssuanceByDay(),
        T.map(
          A.map((issuanceDay) => ({
            t: DateFns.getUnixTime(issuanceDay.timestamp),
            v: pipe(issuanceDay.gwei, Number, ethFromGwei),
          })),
        ),
        TE.fromTask,
      ),
    ),
    TE.bindW("supplyByDay", ({ beaconIssuanceByDay }) =>
      pipe(
        Glassnode.getCirculatingSupplyData(),
        TE.map((supplyData) =>
          addBeaconIssuanceToSupply(beaconIssuanceByDay, supplyData),
        ),
      ),
    ),
    // Deprecate supplyData, lockedData, stakedData after prod frontend has switched to new supply projection inputs.
    TE.map(
      ({
        supplyByDay,
        inContractsByDay,
        stakedData,
        inBeaconValidatorsByDay,
      }) =>
        JSON.stringify({
          supplyData: supplyByDay,
          supplyByDay,
          lockedData: inContractsByDay,
          inContractsByDay,
          stakedData,
          inBeaconValidatorsByDay,
        }),
    ),
  );

const getAndCacheInputs = () =>
  pipe(
    getFreshInputs(),
    TE.chainFirstIOK((inputs) => () => {
      inputsCache.set(inputsKey, inputs);
    }),
    TE.chainFirstIOK(
      () => () =>
        Log.debug("returning freshly cached supply projection inputs"),
    ),
  );

const getCachedOrFreshInputs = () =>
  pipe(
    getCachedInputs(),
    O.match(getAndCacheInputs, (v) =>
      pipe(
        TE.right(v),
        TE.chainFirstIOK(
          () => () => Log.debug("returning cached supply projection inputs"),
        ),
      ),
    ),
  );

export const getInputs = () =>
  pipe(getCachedOrFreshInputs(), queueOnQueue(inputsQueue));
