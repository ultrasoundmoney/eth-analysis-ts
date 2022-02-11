import { readFile, writeFile } from "fs/promises";
import * as Contracts from "./contracts/contracts.js";
import { addWeb3Metadata } from "./contracts/crawl_metadata.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";
import * as Log from "./log.js";

const rows = await sql<{ contractAddress: string }[]>`
  SELECT * FROM contract_base_fee_sums
  JOIN contracts ON contract_address = address
  AND supports_erc_721 IS NULL
  AND supports_erc_1155 IS NULL
`;

const addresses = rows.map((row) => row.contractAddress);

Log.info(`${addresses.length} addresses to go`);

type Metadata = {
  supportsErc_721: boolean | null;
  supportsErc_1155: boolean | null;
};

const addAddressDone = async (address: string): Promise<void> => {
  const doneAddresses: string[] = JSON.parse(
    await readFile("./done_addresses.json", "utf8"),
  );

  await writeFile(
    "./done_addresses.json",
    JSON.stringify([...doneAddresses, address]),
  );
};

const previouslyDoneAddresses = new Set(
  await JSON.parse(await readFile("./done_addresses.json", "utf8")),
);

for (const address of addresses) {
  if (previouslyDoneAddresses.has(address)) {
    Log.debug(`skipping previously done ${address}`);
    continue;
  }
  await addWeb3Metadata(address);
  const [metadata] = await sql<Metadata[]>`
    SELECT supports_erc_721, supports_erc_1155 FROM contracts WHERE address = ${address}
  `;
  Log.debug(
    `stored ${address}, ERC721=${metadata.supportsErc_721}, ERC1155=${metadata.supportsErc_1155}`,
  );
  await Contracts.updatePreferredMetadata(address)();
  await addAddressDone(address);
}

Log.info("done!");

EthNode.closeConnection();
sql.end();
