import * as BeaconNode from "./beacon_node.js";
import * as DateFns from "date-fns";
import * as Db from "./db.js";
import { A, E, flow, O, OAlt, pipe, T, TE, TOAlt } from "./fp.js";
import { getLastEthSupply } from "./scarcity/eth_supply.js";
import { genesisTimestamp } from "./validator_balances.js";

export type BeaconState = {
  stateRoot: string;
  slot: number;
  blockRoot: string;
};

export type BeaconStateWithBlockRow = BeaconState & {
  blockRoot: string;
  depositSum: string;
  depositSumAggregated: string;
  parentRoot: string;
  validatorBalanceSum: string;
};

export type BeaconStateWithBlock = BeaconState & {
  blockRoot: string;
  depositSum: bigint;
  depositSumAggregated: bigint;
  parentRoot: string;
  validatorBalanceSum: bigint;
};

export const getLastState = () =>
  pipe(
    Db.sqlT<BeaconState[]>`
      SELECT state_root, slot, block_root FROM beacon_states
      ORDER BY slot DESC
      LIMIT 1
    `,
    T.map(A.head),
  );

export const getLastStateWithBlock = () =>
  pipe(
    Db.sqlT<BeaconStateWithBlock[]>`
      SELECT
        block_root,
        deposit_sum,
        deposit_sum_aggregated,
        parent_root,
        slot,
        state_root,
        validator_balance_sum
      FROM beacon_states
      WHERE block_root IS NOT NULL
      ORDER BY slot DESC
      LIMIT 1
    `,
    T.map(
      flow(
        A.head,
        O.map((row) => ({
          ...row,
          depositSum: BigInt(row.depositSum),
          depositSumAggregated: BigInt(row.depositSumAggregated),
          validatorBalanceSum: BigInt(row.validatorBalanceSum),
        })),
      ),
    ),
  );

export const storeBeaconState = (
  stateRoot: string,
  slot: number,
  validatorBalanceSum: bigint,
) =>
  Db.sqlTVoid`
    INSERT INTO beacon_states
      ${Db.values({
        state_root: stateRoot,
        slot,
        validator_balance_sum: String(validatorBalanceSum),
      })}
    `;

export const storeBeaconStateWithBlock = (
  stateRoot: string,
  slot: number,
  validatorBalanceSum: bigint,
  blockRoot: string,
  parentRoot: string,
  depositSumAggregated: bigint,
  depositsSum: bigint,
) => Db.sqlTVoid`
  INSERT INTO beacon_states
    ${Db.values({
      state_root: stateRoot,
      slot,
      block_root: blockRoot ?? null,
      parent_root: parentRoot ?? null,
      validator_balance_sum: String(validatorBalanceSum),
      deposit_sum: depositsSum === undefined ? null : String(depositsSum),
      deposit_sum_aggregated:
        depositSumAggregated === undefined
          ? null
          : String(depositSumAggregated),
    })}
`;

const genesisParentRoot =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

export class MissingParentError extends Error {}

export const getParentDepositSumAggregated = (block: BeaconNode.BeaconBlock) =>
  block.parent_root === genesisParentRoot
    ? TE.right(0n)
    : pipe(
        Db.sqlT<{ depositSumAggregated: string }[]>`
          SELECT deposit_sum_aggregated FROM beacon_states
          WHERE block_root = ${block.parent_root}
        `,
        T.map(
          flow(
            Db.readOptionalFromFirstRow("depositSumAggregated"),
            O.map(BigInt),
            E.fromOption(
              () =>
                new MissingParentError(
                  `failed to get deposit_sum_aggregated for parent ${block.parent_root} of block in slot ${block.slot}, with state root ${block.state_root}`,
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
