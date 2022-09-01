import { setInterval } from "timers/promises";
import * as Db from "./db.js";
import * as Duration from "./duration.js";
import * as ExecutionNode from "./execution_node.js";
import { O, pipe, T, TO } from "./fp.js";
import { getLatestGroupedAnalysisNumber } from "./grouped_analysis_1.js";
import * as KeyValueStore from "./key_value_store.js";
import * as Log from "./log.js";

export const blockLagCacheKey = "block-lag";

const updateCurrentBlockLag = () =>
  pipe(
    T.Do,
    T.apS("latestBlockNumber", () => ExecutionNode.getLatestBlockNumber()),
    T.apS("latestGroupedAnalysisNumber", getLatestGroupedAnalysisNumber()),
    T.apS("lastBlockLag", KeyValueStore.getValue<number>(blockLagCacheKey)),
    T.map(({ lastBlockLag, latestBlockNumber, latestGroupedAnalysisNumber }) =>
      pipe(
        latestGroupedAnalysisNumber,
        O.map((latestGroupedAnalysisNumber) =>
          latestBlockNumber - latestGroupedAnalysisNumber < 0
            ? 1
            : latestBlockNumber - latestGroupedAnalysisNumber,
        ),
        O.chain((blockLag) =>
          pipe(
            lastBlockLag,
            O.match(
              // We never stored a block lag, broadcast the new one.
              () => O.some(blockLag),
              (lastBlockLag) =>
                // If we have a block lag stored, only broadcast a new one if it changed.
                blockLag === lastBlockLag ? O.none : O.some(blockLag),
            ),
          ),
        ),
      ),
    ),
    TO.chainTaskK((blockLag) =>
      KeyValueStore.storeValue(blockLagCacheKey, blockLag),
    ),
    TO.chainTaskK(() => {
      Log.debug("sending block-lag cache update");
      return Db.sqlTNotify("cache-update", blockLagCacheKey);
    }),
  );

const intervalIterator = setInterval(Duration.millisFromSeconds(2), Date.now());

const continuouslyUpdate = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    await updateCurrentBlockLag()();
  }
};

export const init = async () => {
  await updateCurrentBlockLag()();
  continuouslyUpdate();
};
