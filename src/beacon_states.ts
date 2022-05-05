import { BeaconHeader } from "./beacon_node.js";
import * as Db from "./db.js";
import { A, E, flow, O, pipe, T, TE } from "./fp.js";

export type BeaconState = {
  stateRoot: string;
  slot: number;
  blockRoot: string;
};

export const getLastBeaconState = () =>
  pipe(
    Db.sqlT<BeaconState[]>`
      SELECT state_root, slot, block_root FROM beacon_states
      ORDER BY slot DESC
      LIMIT 1
    `,
    T.map(A.head),
  );

export const storeBeaconBlock = (
  stateRoot: string,
  slot: number,
  validatorBalanceSum: bigint,
  blockRoot?: string,
  depositSumAggregated?: bigint,
  depositsSum?: bigint,
) =>
  pipe(
    Db.sqlTVoid`
      INSERT INTO beacon_states
        ${Db.values({
          state_root: stateRoot,
          slot,
          block_root: blockRoot ?? null,
          validator_balance_sum: String(validatorBalanceSum),
          deposit_sum: depositsSum === undefined ? null : String(depositsSum),
          deposit_sum_aggregated:
            depositSumAggregated === undefined
              ? null
              : String(depositSumAggregated),
        })}
      `,
  );

const genesisParentRoot =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

export class MissingParentError extends Error {}

export const getParentDepositSumAggregated = (header: BeaconHeader) =>
  header.header.message.parent_root === genesisParentRoot
    ? TE.right(0n)
    : pipe(
        Db.sqlT<{ depositSumAggregated: string }[]>`
          SELECT deposit_sum_aggregated FROM beacon_states
          WHERE block_root = ${header.header.message.parent_root}
        `,
        T.map(
          flow(
            Db.readOptionalFromFirstRow("depositSumAggregated"),
            O.map(BigInt),
            E.fromOption(
              () =>
                new MissingParentError(
                  `failed to get deposit_sum_aggregated for parent ${header.header.message.parent_root} of block ${header.root}`,
                ),
            ),
          ),
        ),
      );

export const getBlockExists = (blockRoot: string) =>
  blockRoot === genesisParentRoot
    ? T.of(true)
    : pipe(
        Db.sqlT<{ exists: boolean }[]>`
          SELECT EXISTS(
            SELECT block_root FROM beacon_states
            WHERE block_root = ${blockRoot}
          )
        `,
        T.map((rows) => rows[0].exists),
      );
