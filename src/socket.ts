import { Server } from "http";
import WebSocket from "ws";
const { Server: WebSocketServer } = WebSocket;
import * as A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import { Socket } from "net";
import * as Log from "./log.js";
import { sql } from "./db.js";

export const startWebSocketServer = (server: Server) => {
  const wss = new WebSocketServer({ noServer: true });

  server.on("upgrade", (request, socket, head) => {
    if (request.url === "/fees/base-fee-feed") {
      wss.handleUpgrade(request, socket as Socket, head, (ws) => {
        wss.emit("connection", ws, request);
      });
    } else {
      socket.destroy();
    }
  });

  // json, number: number, baseFeePerGas: number, totalFeesBurned: number
  type BaseFeeListener = (blockFeeUpdate: string) => void;

  const baseFeeListeners: Map<string, BaseFeeListener> = new Map();

  const addBaseFeeListener = (id: string, fn: BaseFeeListener): void => {
    baseFeeListeners.set(id, fn);
  };

  const removeBaseFeeListener = (id: string) => {
    baseFeeListeners.delete(id);
  };

  let lastFeeUpdates: string[] = [];
  let lastLeaderboardUpdate: string | undefined = undefined;

  const onBaseFeeUpdate = (payload: string | undefined) => {
    if (payload === undefined) {
      Log.warn("got undefined payload on base-fee-updates channel");
      return;
    }

    baseFeeListeners.forEach((fn) => {
      fn(payload);
    });
  };

  sql.listen("base-fee-updates", (payload) => {
    if (JSON.parse(payload!).type === "base-fee-update") {
      lastFeeUpdates.push(payload!);
      if (lastFeeUpdates.length > 7) {
        lastFeeUpdates = pipe(lastFeeUpdates, A.takeRight(7));
      }
    }
    if (JSON.parse(payload!).type === "leaderboard-update") {
      lastLeaderboardUpdate = payload;
    }
    onBaseFeeUpdate(payload);
  });

  wss.on("error", (error) => Log.error("wss error", { error }));

  wss.on("connection", (ws, req) => {
    const id = req.socket.remoteAddress;

    if (id === undefined) {
      Log.error(
        "socket has no remote address, can't id connection, dropping ws",
      );
      return;
    }

    addBaseFeeListener(id, (payload) => ws.send(payload));

    // To make sure clients immediately have the last known state we send it on connect.
    lastFeeUpdates.forEach((blockUpdatePayload) => ws.send(blockUpdatePayload));

    if (typeof lastLeaderboardUpdate === "string") {
      ws.send(lastLeaderboardUpdate);
    }

    ws.on("close", () => {
      removeBaseFeeListener(id);
    });
  });
};
