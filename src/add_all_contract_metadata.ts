import * as Eth from "./eth_node.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import { addWeb3Metadata } from "./contracts_metadata.js";

await Eth.connect();

const rows = await sql<{ contractAddress: string }[]>`
  SELECT * FROM contract_base_fee_sums
  JOIN contracts ON contract_address = address
  WHERE base_fee_sum > 1e18
  AND supports_erc_721 IS NULL
  AND supports_erc_1155 IS NULL
`;

const addresses = rows.map((row) => row.contractAddress);

Log.debug(`${addresses.length} addresses to go`);

for (const address of addresses) {
  await addWeb3Metadata(address);
  Log.debug(`stored ${address}`);
}
