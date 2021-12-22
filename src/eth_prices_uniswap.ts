import BigNumber from "bignumber.js";
import * as DateFns from "date-fns";
import * as Blocks from "./blocks/blocks.js";
import * as ContractsWeb3 from "./contracts_web3.js";
import * as Errors from "./errors.js";
import { EthPrice } from "./etherscan.js";
import { pipe, T, TAlt, TE, TEAlt } from "./fp.js";
import * as Log from "./log.js";

// TODO: slot0 seems slow to update, observations seem to update more regularly.

const usdcEthUniPool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8";
const usdtEthUniPool = "0x4e68ccd3e89f51c3074ca5072bbac773960dfa36";
const daiEthUniPool = "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8";

const Q192 = new BigNumber(2).pow(192);

type Slot0 = {
  sqrtPriceX96: number;
  tick: string;
  observationIndex: string;
  observationCardinality: string;
  observationCardinalityNext: string;
  feeProtocol: string;
  unlocked: boolean;
};

// type UniPoolAbi = AbiItem[];

// const getSqrtPriceX96Type = (abi: UniPoolAbi) => {
//   return pipe(
//     abi,
//     A.filter((abiInterface) => abiInterface.name === "slot0"),
//     A.head,
//     O.chain(
//       flow(
//         (abiItem) => abiItem.outputs,
//         O.fromNullable,
//         O.chain(
//           flow(
//             A.filter((output) => output.name === "sqrtPriceX96"),
//             A.head,
//           ),
//         ),
//       ),
//     ),
//     O.map((output) => output.type),
//     O.match(
//       () => {
//         throw new Error("failed to find output type for uni abi sqrtPriceX96");
//       },
//       (type) => type,
//     ),
//   );
// };

// See: https://docs.uniswap.org/sdk/guides/fetching-prices
// NOTE: setting blockHeight does not work.
const getUniPoolSqrtPriceX96 = (
  uniPoolAddress: string,
  blockHeight?: number,
): T.Task<BigNumber> =>
  pipe(
    ContractsWeb3.getContract(uniPoolAddress),
    TE.chain((contract) =>
      TE.tryCatch(
        () =>
          contract.methods.slot0().call({
            defaultBlock: blockHeight ?? "latest",
          }) as Promise<Slot0>,
        Errors.errorFromUnknown,
      ),
    ),
    TE.map((slot0) => new BigNumber(slot0.sqrtPriceX96)),
    TEAlt.getOrThrow,
  );

const calcMedian = (values: number[]) => {
  if (values.length === 0) {
    throw new Error("can't calculate median for zero numbers");
  }

  values.sort((a: number, b: number) => a - b);

  const half = Math.floor(values.length / 2);

  if (values.length % 2) {
    return values[half];
  }

  return (values[half - 1] + values[half]) / 2.0;
};

// TODO: figure out why these pools are calculated differently.
// The inversion probably depends on which is token0.
export const getMedianEthPrice = (blockHeight?: number): T.Task<EthPrice> =>
  pipe(
    TAlt.seqTParT(
      pipe(
        () => Blocks.getBlockWithRetry(blockHeight ?? "latest"),
        T.map((block) => {
          if (block === undefined) {
            Log.error(
              "failed to get block, using current time as stand-in for block time",
            );
            return new Date();
          }

          return DateFns.fromUnixTime(block.timestamp);
        }),
      ),
      pipe(
        getUniPoolSqrtPriceX96(usdcEthUniPool, blockHeight),
        T.map((sqrtPriceX96) => {
          const ethUsdc = Q192.div(sqrtPriceX96.pow(2))
            // Not clear why the number is 10**12 lower than expected.
            .times(10 ** 12)
            .toNumber();

          return ethUsdc;
        }),
      ),
      pipe(
        getUniPoolSqrtPriceX96(usdtEthUniPool, blockHeight),
        T.map((sqrtPriceX96) => {
          const ethUsdt = sqrtPriceX96
            .pow(2)
            .div(Q192)
            // Not clear why the number is 10**12 lower than expected.
            .times(10 ** 12)
            .toNumber();

          return ethUsdt;
        }),
      ),
      pipe(
        getUniPoolSqrtPriceX96(daiEthUniPool, blockHeight),
        T.map((sqrtPriceX96) => {
          // Not clear why inverted.
          const ethDai = Q192.div(sqrtPriceX96.pow(2)).toNumber();

          return ethDai;
        }),
      ),
    ),
    T.map(([priceDate, ethUsdcPrice, ethUsdtPrice, ethDaiPrice]) => {
      return {
        ethusd: calcMedian([ethUsdcPrice, ethUsdtPrice, ethDaiPrice]),
        timestamp: priceDate,
      };
    }),
  );
