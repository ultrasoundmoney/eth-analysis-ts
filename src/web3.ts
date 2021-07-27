import { createAlchemyWeb3 } from "@alch/alchemy-web3";

const lokiRopstenNodeWs = "ws://18.220.53.200:8546/";
const jasperMainnetNodeWs = "ws://18.219.176.5:8546/";

export const web3 = createAlchemyWeb3(
  // "wss://eth-ropsten.alchemyapi.io/v2/Ocbe7IDoukMM0J2AQ4m92r9s9tG4W60N",
  lokiRopstenNodeWs,
);

// const web3 = createAlchemyWeb3(
//   "https://eth-mainnet.alchemyapi.io/v2/Z6_3CNslo_t0o0yvHs6fel4UqRMo5Ixu",
// );

export const eth = web3.eth;
