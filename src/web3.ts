import { createAlchemyWeb3 } from "@alch/alchemy-web3";
import Config from "./config.js";

// "wss://eth-ropsten.alchemyapi.io/v2/Ocbe7IDoukMM0J2AQ4m92r9s9tG4W60N",
// "wss://eth-mainnet.alchemyapi.io/v2/Z6_3CNslo_t0o0yvHs6fel4UqRMo5Ixu",

const ropstenNodeWs = "ws://18.220.53.200:8546/";
const mainnetNodeWs = "ws://18.219.176.5:8546/";

export const web3 = createAlchemyWeb3(
  Config.chain === "ropsten" ? ropstenNodeWs : mainnetNodeWs,
);

export const eth = web3.eth;
export const closeWeb3Ws = () => {
  // The websocket connection keeps the process from exiting. Alchemy doesn't expose any method to close the connection. We use undocumented values.
  if (
    typeof eth.currentProvider !== "string" &&
    eth.currentProvider !== null &&
    "ws" in eth.currentProvider
  ) {
    (
      eth.currentProvider as { stopHeartbeatAndBackfill: () => void }
    ).stopHeartbeatAndBackfill();
    (
      eth.currentProvider as { ws: { disposeSocket: () => void } }
    ).ws.disposeSocket();
  }
};
