import * as Log from "../log.js";
import * as Db from "../db.js";
import * as ExecutionNode from "../execution_node.js";
import { O, pipe, T, TO } from "../fp.js";

Log.info("syncing execution chain supply deltas");

await Db.runMigrations();

const getLastSyncedBlockNumber = (): TO.TaskOption<number> =>
  pipe(
    Db.sqlT<{ max: number }[]>`
      SELECT MAX(block_number) FROM execution_supply_deltas
    `,
    T.map(Db.readFromFirstRow("max")),
  );

// eslint-disable-next-line no-constant-condition
while (true) {
  await pipe(
    T.Do,
    T.apS("latestBlockNumber", () => ExecutionNode.getLatestBlockNumber()),
    T.apS("lastSyncedBlockNumber", getLastSyncedBlockNumber()),
    T.bind("supplyDeltas", ({ lastSyncedBlockNumber }) =>
      pipe(
        lastSyncedBlockNumber,
        O.match(
          () => () => ExecutionNode.getNSupplyDeltas(5000, 0),
          (lastSyncedBlockNumber) => () =>
            ExecutionNode.getNSupplyDeltas(10, lastSyncedBlockNumber + 1),
        ),
      ),
    ),
    T.chainFirstIOK(({ lastSyncedBlockNumber }) =>
      Log.debugIO(
        `last synced: ${pipe(
          lastSyncedBlockNumber,
          O.getOrElse(() => 0),
        )}`,
      ),
    ),
    T.chain(
      ({ supplyDeltas }) =>
        Db.sqlTVoid`
          INSERT INTO execution_supply_deltas (
            block_number,
            fee_burn,
            fixed_reward,
            hash,
            self_destruct,
            supply_delta,
            uncles_reward
          )
          ${Db.values(supplyDeltas)}
        `,
    ),
  )();
}
