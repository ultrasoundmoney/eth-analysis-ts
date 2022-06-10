import { setInterval } from "timers/promises";
import * as Db from "./db.js";
import * as Duration from "./duration.js";
import * as ExecutionNode from "./execution_node.js";
import { O, pipe, T, TO } from "./fp.js";
import { getLatestGroupedAnalysisNumber } from "./grouped_analysis_1.js";
import * as KeyValueStore from "./key_value_store.js";

export const blockLagCacheKey = "block-lag";

const updateCurrentBlockLag = () =>
  pipe(
    T.Do,
    T.apS("latestBlockNumber", () => ExecutionNode.getLatestBlockNumber()),
    T.apS("latestGroupedAnalysisNumber", getLatestGroupedAnalysisNumber()),
    T.map(({ latestBlockNumber, latestGroupedAnalysisNumber }) =>
      pipe(
        latestGroupedAnalysisNumber,
        O.map((latestGroupedAnalysisNumber) =>
          latestBlockNumber - latestGroupedAnalysisNumber < 0
            ? 1
            : latestBlockNumber - latestGroupedAnalysisNumber,
        ),
      ),
    ),
    TO.chainTaskK((blockLag) =>
      KeyValueStore.storeValue(blockLagCacheKey, blockLag),
    ),
    TO.chainTaskK(() => Db.sqlTNotify("cache-update", blockLagCacheKey)),
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
