import * as BeaconNode from "./beacon_node.js";
import * as BeaconStates from "./beacon_states.js";
import * as Config from "./config.js";
import * as Db from "./db.js";
import { A, B, NEA, O, pipe, T, TAlt, TE, TEAlt } from "./fp.js";
import * as Log from "./log.js";

Log.info("analyze beacon states starting");

await Db.runMigrations();

// const getIsBeaconBlocksEmpty = () =>
//   pipe(
//     Db.sqlT`
//     SELECT state_root FROM beacon_blocks
//     LIMIT 1
//   `,
//     T.map((rows) => rows.length === 0),
//   );

// const blocksQueue = new PQueue({ concurrency: 1 });

// BeaconNode.subscribeNewFinalizedCheckpoints((finalizedCheckpoint) =>
//   blocksQueue.add(syncChainTo(finalizedCheckpoint)),
// );

const getDepositsSumFromBlock = (block: BeaconNode.BeaconBlock) =>
  pipe(
    block.body.deposits,
    A.map((deposit) => deposit.data.amount),
    A.reduce(0n, (sum, num) => sum + num),
  );

const sumValidatorBalances = (
  validatorBalances: BeaconNode.ValidatorBalance[],
) =>
  pipe(
    validatorBalances,
    A.reduce(0n, (sum, validatorBalance) => sum + validatorBalance.balance),
  );

const getDepositSumAggregated = (block: BeaconNode.BeaconBlock) =>
  pipe(
    BeaconStates.getParentDepositSumAggregated(block),
    TE.map(
      (parentDepositSumAggregated) =>
        parentDepositSumAggregated + getDepositsSumFromBlock(block),
    ),
  );

const storeBeaconBlockWithBlockData = (
  header: BeaconNode.BeaconHeader,
  block: BeaconNode.BeaconBlock,
  stateRoot: string,
  validatorBalanceSum: bigint,
  depositSumAggregated: bigint,
) =>
  pipe(
    BeaconStates.getBlockExists(block.parent_root),
    T.chain(
      B.match(
        () =>
          TE.left(
            new BeaconStates.MissingParentError(
              `failed to store block ${header.root}, slot: ${header.header.message.slot}, parent ${header.header.message.parent_root} is missing`,
            ),
          ),
        () =>
          pipe(
            BeaconStates.storeBeaconStateWithBlock(
              stateRoot,
              header.header.message.slot,
              validatorBalanceSum,
              header.root,
              header.header.message.parent_root,
              depositSumAggregated,
              getDepositsSumFromBlock(block),
            ),
            (task) => TE.fromTask(task),
          ),
      ),
    ),
  );

// Unsafe post finality checkpoint as retrieving state root and then a header might yield a header from a different state root. A safe version would establish a state root chain, and only fetch new data using that chain.
const syncSlot = (slot: number) =>
  pipe(
    TE.Do,
    TE.apS("stateRoot", BeaconNode.getStateRootBySlot(slot)),
    TE.apSW(
      "headerBlockDeposits",
      pipe(
        BeaconNode.getHeaderBySlot(slot),
        TE.chain(
          O.match(
            // No header, no block or depositSumAggregated.
            () => TE.right(O.none),
            // Header, so there must be a block and depositSumAggregated.
            (header) =>
              pipe(
                TE.Do,
                TE.apS("block", BeaconNode.getBlockByRoot(header.root)),
                TE.bindW("depositSumAggregated", ({ block }) =>
                  getDepositSumAggregated(block),
                ),
                TE.map(({ block, depositSumAggregated }) =>
                  O.some({ header, block, depositSumAggregated }),
                ),
              ),
          ),
        ),
      ),
    ),
    TE.bindW("validatorBalanceSum", ({ stateRoot }) =>
      pipe(
        BeaconNode.getValidatorBalances(stateRoot),
        TE.map(sumValidatorBalances),
      ),
    ),
    TE.chainW(({ headerBlockDeposits, stateRoot, validatorBalanceSum }) =>
      pipe(
        headerBlockDeposits,
        O.match(
          () =>
            pipe(
              BeaconStates.storeBeaconState(
                stateRoot,
                slot,
                validatorBalanceSum,
              ),
              (task) => TE.fromTask<void, never>(task),
            ),
          ({ header, depositSumAggregated, block }) =>
            storeBeaconBlockWithBlockData(
              header,
              block,
              stateRoot,
              validatorBalanceSum,
              depositSumAggregated,
            ),
        ),
      ),
    ),
    TEAlt.chainFirstLogDebug(() => `synced slot ${slot}`),
  );

type SlotRange = { from: number; to: number };

const fastSyncSlots = (slotRange: SlotRange) =>
  pipe(
    NEA.range(slotRange.from, slotRange.to),
    TE.traverseSeqArray((slot) => syncSlot(slot)),
    TEAlt.concatAllVoid,
  );

const fastSyncFromLastSlot = (
  lastState: BeaconStates.BeaconState,
  lastFinalizedBlock: BeaconNode.BeaconBlock,
) =>
  fastSyncSlots({
    from: lastState.slot + 1,
    to: lastFinalizedBlock.slot,
  });

const fastSyncFromGenesis = (lastFinalizedBlock: BeaconNode.BeaconBlock) =>
  fastSyncSlots({
    from: 0,
    to: lastFinalizedBlock.slot,
  });

// To be sure we're syncing the right chain, we'd have to start at the current head, find our common parent, rollback blocks we have outside that chain, and then roll forward from the common parent to the head. How to find the common parent is unclear. Instead, we assume slots before the last finalized checkpoint are stable, and sync by slot.
await TAlt.when(
  Config.getUseFastBeaconSync(),
  pipe(
    TE.Do,
    TE.apS("lastFinalizedBlock", BeaconNode.getLastFinalizedBlock()),
    TE.apSW(
      "lastState",
      pipe(BeaconStates.getLastState(), (task) =>
        TE.fromTask<O.Option<BeaconStates.BeaconState>, never>(task),
      ),
    ),
    TE.chainW(({ lastState, lastFinalizedBlock }) =>
      pipe(
        lastState,
        O.match(
          () => fastSyncFromGenesis(lastFinalizedBlock),
          (lastState) => fastSyncFromLastSlot(lastState, lastFinalizedBlock),
        ),
      ),
    ),
    TE.match(
      (e) => {
        Log.error("failed beacon states fast-sync", e);
      },
      () => Log.debug("beacon states fast-sync successful"),
    ),
  ),
)();

await Db.closeConnection();
