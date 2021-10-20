import * as Log from "./log.js";
import Config from "./config.js";
import fetch from "node-fetch";
import { pipe, T } from "./fp.js";

const apiUrl =
  Config.env === "prod" || Config.env === "staging"
    ? "http://serve-fam"
    : "https://api.ultrasound.money";

export type FamDetails = {
  bio: string | null;
  famFollowerCount: number;
  followersCount: number;
  handle: string;
  name: string;
};

export const getDetails = (handles: string[]): T.Task<FamDetails[]> => {
  if (handles.length === 0) {
    return T.of([]);
  }

  return pipe(
    () =>
      fetch(`${apiUrl}/fam/details`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ handles }),
      }),
    T.chain((res) => {
      if (res.status === 500) {
        // This happens sometimes, no need to crash but should figure out the issue on the fam service side.
        Log.error("fetch fam details 500 response, returning empty list");
        return T.of([]);
      }

      if (res.status !== 200) {
        throw new Error(
          `bad response fetching fam details, status: ${res.status}`,
        );
      }

      return () => res.json() as Promise<FamDetails[]>;
    }),
  );
};
