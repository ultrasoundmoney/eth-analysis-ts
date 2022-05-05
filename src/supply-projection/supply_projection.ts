import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import * as Duration from "../duration.js";
import { O, pipe, TE } from "../fp.js";
import * as Glassnode from "../glassnode.js";
import * as Log from "../log.js";
import { queueOnQueue } from "../queues.js";
import { getEthInValidatorsByDay } from "../validator_balances.js";
import { serializeBigInt } from "../json.js";

const inputsCache = new QuickLRU<"inputs", string>({
  maxSize: 1,
  maxAge: Duration.millisFromHours(4),
});

const inputsKey = "inputs";

const inputsQueue = new PQueue({
  concurrency: 1,
});

const getCachedInputs = () => pipe(inputsCache.get(inputsKey), O.fromNullable);

const getFreshInputs = () =>
  pipe(
    TE.Do,
    TE.apS("supplyData", Glassnode.getCirculatingSupplyData()),
    TE.apS("lockedData", Glassnode.getLockedEthData()),
    TE.apS("stakedData", Glassnode.getStakedData()),
    TE.apSW("inValidators", pipe(getEthInValidatorsByDay(), TE.fromTask)),
    TE.map(({ supplyData, lockedData, stakedData, inValidators }) =>
      JSON.stringify(
        {
          supplyData,
          lockedData,
          stakedData,
          inValidators,
        },
        serializeBigInt,
      ),
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
