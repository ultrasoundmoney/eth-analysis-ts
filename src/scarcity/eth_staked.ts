import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import * as EthNode from "../eth_node.js";
import { WeiBI } from "../eth_units.js";
import * as Format from "../format.js";
import { O, OAlt, pipe, T, TAlt } from "../fp.js";

const eth2DepositAddress = "0x00000000219ab540356cbb839cbe05303d7705fa";

export type EthStaked = {
  timestamp: Date;
  ethStaked: WeiBI;
};

let lastEthStaked: O.Option<EthStaked> = O.none;

const updateEthStaked = () =>
  pipe(
    () => EthNode.getBalance(eth2DepositAddress),
    TAlt.chainFirstLogDebug(
      (balance) =>
        `got eth staked from deposit contract, balance: ${Format.ethFromWei(
          balance,
        )} ETH`,
    ),
    T.map((balance) => {
      lastEthStaked = O.some({
        timestamp: new Date(),
        ethStaked: balance,
      });
    }),
  );

export const getLastEthStaked = (): EthStaked =>
  pipe(lastEthStaked, OAlt.getOrThrow("tried to get eth staked before init"));

const intervalIterator = setInterval(Duration.millisFromMinutes(1), Date.now());

const continuouslyUpdate = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    await updateEthStaked()();
  }
};

export const init = async () => {
  await updateEthStaked()();
  continuouslyUpdate();
};
