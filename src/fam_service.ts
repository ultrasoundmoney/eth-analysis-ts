import * as Retry from "retry-ts";
import * as Config from "./config.js";
import * as FetchAlt from "./fetch_alt.js";
import { NEA, pipe } from "./fp.js";

export type Linkable = {
  display_url: string;
  end: number;
  expanded_url: string;
  start: number;
};

export type TwitterDetails = {
  bio: string | null;
  famFollowerCount: number;
  followerCount: number;
  /**
   * @deprecated
   */
  followersCount: number;
  handle: string;
  isInFam: boolean | null;
  links: Linkable[] | null;
  name: string;
  /**
   * @deprecated
   */
  twitterDescription: string | null;
  twitterId: string;
};

const detailsByIdsUrl = `${Config.getFamServiceUrl()}/fam/leaderboards-details/ids`;

export const getDetailsByIds = (twitterIds: NEA.NonEmptyArray<string>) =>
  pipe(
    FetchAlt.fetchWithRetryJson<TwitterDetails[]>(
      detailsByIdsUrl,
      {
        body: JSON.stringify({ twitterIds }),
        headers: { "Content-Type": "application/json" },
        method: "POST",
      },
      {
        retryPolicy: Retry.Monoid.concat(
          Retry.exponentialBackoff(200),
          Retry.limitRetries(2),
        ),
      },
    ),
  );

const detailsByHandlesUrl = `${Config.getFamServiceUrl()}/fam/leaderboards-details/handles`;

export const getDetailsByHandles = (handles: NEA.NonEmptyArray<string>) =>
  pipe(
    FetchAlt.fetchWithRetryJson<TwitterDetails[]>(
      detailsByHandlesUrl,
      {
        body: JSON.stringify({ handles }),
        headers: { "Content-Type": "application/json" },
        method: "POST",
      },
      {
        retryPolicy: Retry.Monoid.concat(
          Retry.exponentialBackoff(200),
          Retry.limitRetries(2),
        ),
      },
    ),
  );
