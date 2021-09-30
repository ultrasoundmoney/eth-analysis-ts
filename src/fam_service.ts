import Config from "./config.js";
import fetch from "node-fetch";
import { pipe, T } from "./fp.js";

const apiUrl =
  Config.env === "prod" || Config.env === "staging"
    ? "http://serve-fam"
    : "https://api.ultrasound.money";

export type FamDetails = {
  bio: string | null;
  followerCount: number;
  famFollowerCount: number;
  handle: string;
};

export const getDetails = (handles: string[]): T.Task<FamDetails[]> => {
  console.log("getting details for", handles);
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
      if (res.status !== 200) {
        throw new Error(
          `bad response fetching fam details, status: ${res.status}`,
        );
      }
      return () => res.json() as Promise<FamDetails[]>;
    }),
  );
};
