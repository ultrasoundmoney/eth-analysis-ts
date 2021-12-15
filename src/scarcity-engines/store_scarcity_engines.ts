import { getEthLocked } from "./store_eth_locked.js";
import { getEthStaked } from "./store_eth_staked.js";
import * as Etherscan from "../etherscan.js";
import * as FeeBurn from "../fee_burn.js";
import { sql } from "../db.js";
import { BlockDb } from "../blocks/blocks.js";

export const onNewBlock = async (block: BlockDb) => {
  const ethStaked = getEthStaked();
  const ethLocked = getEthLocked();
  const ethSupply = Etherscan.getEthSupply();
  const ethBurned = FeeBurn.getAllFeesBurned().eth;

  const scarcityEngines = {
    engines: [
      {
        name: "staked",
        amount: ethStaked,
        timestamp: new Date("2020-11-03T00:00:00.000Z"),
      },
      {
        name: "locked",
        amount: ethLocked,
        timestamp: new Date("2017-09-02T00:00:00.000Z"),
      },
    ],
    ethSupply: ethSupply,
    ethBurned: ethBurned,
  };

  await sql`
    INSERT INTO derived_block_stats (
      block_number,
      scarcity_engines
    ) VALUES (
      ${block.number},
      ${sql.json(scarcityEngines)}
    ) ON CONFLICT (block_number) DO UPDATE
    SET scarcity_engines = excluded.scarcity_engines
  `;
};
