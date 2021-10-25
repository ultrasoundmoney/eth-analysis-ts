import * as Log from "./log.js";
import { config } from "./config.js";
import fetch from "node-fetch";
import { pipe, T } from "./fp.js";

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
      fetch(`${config.famServiceUrl}/fam/details`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ handles }),
      }),
    T.chain((res) => {
      if (res.status === 500 || res.status === 502) {
        // This happens sometimes, no need to crash but should figure out the issue on the fam service side.
        Log.error(
          "fetch fam details 500 or 502 response, returning empty list",
        );
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
