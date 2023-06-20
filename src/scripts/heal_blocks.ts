import fs from "fs/promises";
import makeEta from "simple-eta";
import * as Blocks from "../blocks/blocks.js";
import * as ContractBaseFees from "../contract_base_fees.js";
import * as Duration from "../duration.js";
import * as EthPrices from "../eth-prices/index.js";
import { A, pipe, T, TEAlt, TOAlt } from "../fp.js";
import * as Log from "../log.js";
import * as Transactions from "../transactions.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let eta: any | undefined = undefined;
let blocksDone = 0;

await pipe(
  () => fs.readFile("./blocks_202202211320.json", "utf8") as Promise<string>,
  T.map((text: string) => JSON.parse(text) as number[]),
  T.map(A.filter((num) => num > 13403024)),
  T.chainIOK((rows) => () => {
    eta = makeEta({
      max: rows.length,
    });
    return rows;
  }),
  T.chain(
    T.traverseSeqArray((blockNumber) =>
      pipe(
        T.Do,
        T.apS(
          "block",
          pipe(
            Blocks.getBlockSafe(blockNumber),
            TOAlt.expect(
              `while reanalyzing block ${blockNumber} came back null`,
            ),
          ),
        ),
        T.bind("transactionReceipts", ({ block }) =>
          pipe(
            Transactions.transactionReceiptsFromBlock(block),
            TEAlt.getOrThrow,
          ),
        ),
        T.chain(({ block, transactionReceipts }) =>
          pipe(
            T.Do,
            // Contracts marked as mined in a block that was rolled back are possibly wrong. Reanalyze 'contract mined at' data if we want very high confidence.
            T.bind("deleteContractBaseFees", () =>
              ContractBaseFees.deleteContractBaseFees(blockNumber),
            ),
            T.bind("deleteBlock", () => Blocks.deleteBlock(blockNumber)),
            // Add block
            T.bind("ethPrice", () =>
              pipe(
                EthPrices.getEthPrice(
                  block.timestamp,
                  Duration.millisFromMinutes(2),
                ),
                TEAlt.getOrThrow,
              ),
            ),
            T.chain(
              ({ ethPrice }) =>
                () =>
                  Blocks.storeBlock(
                    block,
                    transactionReceipts,
                    ethPrice.ethusd,
                  ),
            ),
            T.chainIOK(() => () => {
              blocksDone++;
              eta!.report(blocksDone);
              if (blocksDone % 10 === 0 && blocksDone !== 0) {
                Log.debug(`eta: ${eta!.estimate().toFixed(0)}s`);
              }
            }),
          ),
        ),
      ),
    ),
  ),
)();
