import { WebSocketServer } from "ws";
import { pipe } from "../fp.js";
import * as Log from "../log.js";

const sendAll = (wss: WebSocketServer, data: string) => {
  Log.debug(`sending ws message to ${wss.clients.size} listeners`);
  for (const ws of wss.clients) {
    ws.send(data);
  }
};

export const listen = (port: number) =>
  pipe(
    new WebSocketServer({ port }),
    (wss) => (data: string) => sendAll(wss, data),
  );
