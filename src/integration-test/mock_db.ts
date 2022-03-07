import * as Blocks from "../blocks/blocks.js";
import * as BlockSamples from "../block_samples.js";
import * as Db from "../db.js";
import { A, pipe } from "../fp.js";

const insertableFromBlock = (block: Blocks.BlockDb) => ({
  base_fee_sum: block.baseFeeSum,
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
  const block = await BlockSamples.getSingleBlock();

  await insertTestBlocks([block]);

  return {
    reset: async () => {
      Db.sql`DELETE FROM blocks`;
      Db.sql`DELETE FROM analysis_state`;
    },
  };
};
