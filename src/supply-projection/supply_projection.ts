import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import * as Duration from "../duration.js";
import { O, pipe, T, TE } from "../fp.js";
import * as Glassnode from "../glassnode.js";
import * as Log from "../log.js";

const inputsCache = new QuickLRU<"inputs", string>({
  maxSize: 1,
  maxAge: Duration.millisFromHours(4),
});

const inputsKey = "inputs";

const inputsQueue = new PQueue({
  concurrency: 1,
});

const addT =
  <A>(task: T.Task<A>): T.Task<A> =>
  () =>
    inputsQueue.add(task);

const getCachedInputs = () => pipe(inputsCache.get(inputsKey), O.fromNullable);

const getFreshInputs = () =>
  pipe(
    TE.Do,
    TE.apS("supplyData", Glassnode.getCirculatingSupplyData()),
    TE.apS("lockedData", Glassnode.getLockedEthData()),
    TE.apS("stakedData", Glassnode.getStakedData()),
    TE.map(({ supplyData, lockedData, stakedData }) =>
      JSON.stringify({
        supplyData,
        lockedData,
        stakedData,
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

export const getInputs = () => pipe(getCachedOrFreshInputs(), addT);
