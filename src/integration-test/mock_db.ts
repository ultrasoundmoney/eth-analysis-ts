import * as Blocks from "../blocks/blocks.js";
import * as SamplesBlocks from "../samples/blocks.js";
import * as Db from "../db.js";
import { A, flow, pipe, T, TAlt } from "../fp.js";
import * as SamplesContractBaseFees from "../samples/contract_base_fees.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(BigInt.prototype as any).toJSON = function () {
  return this.toString() + "n";
};

const insertableFromBlock = (block: Blocks.BlockDb) => ({
  base_fee_per_gas: block.baseFeePerGas,
  base_fee_sum: block.baseFeeSum,
  eth_price: block.ethPrice,
  gas_used: block.gasUsed,
  hash: block.hash,
  mined_at: block.minedAt,
  number: block.number,
});

export const insertTestBlocks = async (blocks: Blocks.BlockDb[]) =>
  pipe(
    blocks,
    A.map(insertableFromBlock),
    (blocks) => Db.sql`
      INSERT INTO blocks
        ${Db.sql(blocks)}
    `,
  );

export const insertSingleBlock = async () => {
  const block = await SamplesBlocks.getSingleBlock();

  await insertTestBlocks([block]);

  return async () => {
    await Db.sql`DELETE FROM blocks`;
    await Db.sql`DELETE FROM analysis_state`;
  };
};

export const resetTables = () =>
  pipe(
    TAlt.seqTSeq(
      Db.sqlTVoid`DELETE FROM contract_base_fees`,
      Db.sqlTVoid`DELETE FROM contracts`,
      Db.sqlTVoid`DELETE FROM blocks`,
      Db.sqlTVoid`DELETE FROM analysis_state`,
    ),
    TAlt.concatAllVoid,
  );

export const seedM5Blocks = () =>
  pipe(
    () => SamplesBlocks.getM5Blocks(),
    T.map(A.map(insertableFromBlock)),
    T.chain(
      (m5Blocks) => Db.sqlTVoid`
        INSERT INTO blocks ${Db.sql(m5Blocks)}
      `,
    ),
  );

const seedM5Contracts = () =>
  pipe(
    SamplesContractBaseFees.getM5ContractBaseFees(),
    T.map(
      flow(
        A.map((row) => row.contract_address),
        (rows) => new Set(rows),
        (set) => Array.from(set.values()),
        A.map((address) => ({ address })),
      ),
    ),
    T.chain(
      (addresses) => Db.sqlTVoid`
        INSERT INTO contracts ${Db.sql(addresses)}
      `,
    ),
  );

export const seedM5ContractBaseFees = () =>
  pipe(
    T.Do,
    T.apS("seedBlocks", seedM5Blocks()),
    T.apS("seedContracts", seedM5Contracts()),
    T.bind("m5ContractBaseFees", () =>
      SamplesContractBaseFees.getM5ContractBaseFees(),
    ),
    T.chain(
      ({ m5ContractBaseFees }) => Db.sqlTVoid`
        INSERT INTO contract_base_fees ${Db.sql(m5ContractBaseFees)}
      `,
    ),
  );
