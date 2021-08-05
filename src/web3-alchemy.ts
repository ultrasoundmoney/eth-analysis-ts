// import { createAlchemyWeb3 } from "@alch/alchemy-web3";
const createAlchemyWeb3 = (_: string) => ({
  eth: {
    currentProvider: {
      ws: { disposeSocket: () => undefined },
      stopHeartbeatAndBackfill: () => undefined,
    },
  },
  _,
});

const mainnetUrl =
  "wss://eth-mainnet.alchemyapi.io/v2/Z6_3CNslo_t0o0yvHs6fel4UqRMo5Ixu";

export const web3 = createAlchemyWeb3(mainnetUrl);

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
