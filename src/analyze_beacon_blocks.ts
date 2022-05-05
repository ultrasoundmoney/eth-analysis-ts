import * as BeaconNode from "./beacon_node.js";
import * as BeaconBlocks from "./beacon_states.js";
import * as Config from "./config.js";
import * as Db from "./db.js";
import { A, B, flow, NEA, O, OAlt, pipe, T, TAlt, TE, TEAlt } from "./fp.js";
import * as Log from "./log.js";

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

const getDepositSumAggregated = (
  header: BeaconNode.BeaconHeader,
  block: BeaconNode.BeaconBlock,
) =>
  pipe(
    BeaconBlocks.getParentDepositSumAggregated(header),
    TE.map(
      flow(
        (parentDepositSumAggregated) =>
          parentDepositSumAggregated + getDepositsSumFromBlock(block),
        O.some,
      ),
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
    BeaconBlocks.getBlockExists(block.parent_root),
    T.chain(
      B.match(
        () =>
          TE.left(
            new BeaconBlocks.MissingParentError(
              `failed to store block ${header.root}, slot: ${block.slot}, parent ${block.parent_root} is missing`,
            ),
          ),
        () =>
          pipe(
            BeaconBlocks.storeBeaconBlock(
              stateRoot,
              header.header.message.slot,
              validatorBalanceSum,
              header.root,
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
    TE.apSW("header", BeaconNode.getHeaderBySlot(slot)),
    TE.bindW("block", ({ header }) =>
      pipe(
        header,
        O.match(
          () => TE.right(O.none),
          (header) =>
            pipe(BeaconNode.getBlockByRoot(header.root), TE.map(O.some)),
        ),
      ),
    ),
    TE.bindW("depositSumAggregated", ({ header, block }) =>
      pipe(
        OAlt.seqT(header, block),
        O.match(
          () => TE.right(O.none),
          ([header, block]) => getDepositSumAggregated(header, block),
        ),
      ),
    ),
    TE.bindW("validatorBalanceSum", ({ stateRoot }) =>
      pipe(
        BeaconNode.getValidatorBalances(stateRoot),
        TE.map(sumValidatorBalances),
      ),
    ),
    TE.chainW(
      ({
        block,
        depositSumAggregated,
        header,
        stateRoot,
        validatorBalanceSum,
      }) =>
        pipe(
          OAlt.seqT(header, depositSumAggregated, block),
          O.match(
            () =>
              pipe(
                BeaconBlocks.storeBeaconBlock(
                  stateRoot,
                  slot,
                  validatorBalanceSum,
                ),
                (task) => TE.fromTask<void, never>(task),
              ),
            ([header, depositSumAggregated, block]) =>
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
  );

type SlotRange = { from: number; to: number };

const fastSyncSlots = (slotRange: SlotRange) =>
  pipe(
    NEA.range(slotRange.from, slotRange.to),
    TE.traverseSeqArray((slot) => syncSlot(slot)),
    TEAlt.concatAllVoid,
  );

const fastSyncFromLastSlot = (
  lastState: BeaconBlocks.BeaconState,
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

// To be sure we're syncing the right chain, we'd have to start at the current head, find our common parent, rollback blocks we have outside that chain, and roll forward to the head. This simple approach always work. To do this with few or no blocks in our table, one would traverse ~4M slots to confirm what our current common parent is. That would be slow, to speed things along, we have a fast sync mode that syncs to the current finalized checkpoint from genisis if our DB is empty or assumes the latest block in our DB is a common parent if our DB is not empty.
await TAlt.when(
  Config.getUseFastBeaconSync(),
  pipe(
    TE.Do,
    TE.apS("lastFinalizedBlock", BeaconNode.getLastFinalizedBlock()),
    TE.apSW(
      "lastState",
      pipe(BeaconBlocks.getLastBeaconState(), (task) =>
        TE.fromTask<O.Option<BeaconBlocks.BeaconState>, never>(task),
      ),
    ),
    TE.chain(({ lastState: lastState, lastFinalizedBlock }) =>
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
