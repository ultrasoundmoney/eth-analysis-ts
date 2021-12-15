import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import * as EthNode from "../eth_node.js";
import { WeiBI } from "../eth_units.js";
import * as Log from "../log.js";

const eth2DepositAddress = "0x00000000219ab540356cbb839cbe05303d7705fa";

type LastEthStaked = {
  timestamp: Date;
  ethStaked: WeiBI;
};

let lastEthStaked: undefined | LastEthStaked = undefined;

const updateEthStaked = async () => {
  const balanceHex = await EthNode.getBalance(eth2DepositAddress);
  const balance = BigInt(balanceHex);

  Log.debug(`got eth staked from deposit contract, balance: ${balance}`);

  lastEthStaked = {
    timestamp: new Date(),
    ethStaked: balance,
  };
};

export const getLastEthStaked = () => {
  return lastEthStaked;
};

const intervalIterator = setInterval(Duration.millisFromMinutes(1), Date.now());

const continuouslyUpdate = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    await updateEthStaked();
  }
};

export const init = async () => {
  await updateEthStaked();
  continuouslyUpdate();
};
