import { createAlchemyWeb3 } from "@alch/alchemy-web3";
import Config from "./config.js";

const ropstenUrl =
  "wss://eth-ropsten.alchemyapi.io/v2/Ocbe7IDoukMM0J2AQ4m92r9s9tG4W60N";
const mainnetUrl =
  "wss://eth-mainnet.alchemyapi.io/v2/Z6_3CNslo_t0o0yvHs6fel4UqRMo5Ixu";

export const web3 = createAlchemyWeb3(
  Config.chain === "ropsten" ? ropstenUrl : mainnetUrl,
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

(async () => {
  console.log(
    await eth.getTransactionReceipt(
      "0x13bc37171628d19bf73f28a4d44ae8612ab598f972848bdbb410d7ce0f9f6aeb",
    ),
  );
})();
