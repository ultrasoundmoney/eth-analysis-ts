import * as Retry from "retry-ts";
import * as Config from "./config.js";
import * as FetchAlt from "./fetch_alt.js";
import { NEA, pipe, TE } from "./fp.js";

export type LinkableUrl = {
  display_url: string;
  end: number;
  expanded_url: string;
  start: number;
};

export type LinkableMention = {
  start: number;
  end: number;
  username: string;
};

export type LinkableCashtag = { start: number; end: number; tag: string };

export type LinkableHashtag = { start: number; end: number; tag: string };

export type Linkables = {
  cashtags?: LinkableCashtag[];
  hashtags?: LinkableHashtag[];
  mentions?: LinkableMention[];
  urls?: LinkableUrl[];
};

export type TwitterDetails = {
  bio: string | undefined;
  famFollowerCount: number;
  followerCount: number;
  /**
   * @deprecated use followerCount
   */
  followersCount: number;
  handle: string;
  isInFam: boolean | undefined;
  links: Linkables | undefined;
  name: string;
  /**
   * @deprecated use bio
   */
  twitterDescription: string | null;
  twitterId: string;
};

const detailsByIdsUrl = `${Config.getFamServiceUrl()}/fam/leaderboards-details/ids`;

export const getDetailsByIds = (twitterIds: NEA.NonEmptyArray<string>) =>
  pipe(
    FetchAlt.fetchWithRetryJson(
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
    TE.map((u) => u as TwitterDetails[]),
  );

const detailsByHandlesUrl = `${Config.getFamServiceUrl()}/fam/leaderboards-details/handles`;

export const getDetailsByHandles = (handles: NEA.NonEmptyArray<string>) =>
  pipe(
    FetchAlt.fetchWithRetryJson(
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
    TE.map((u) => u as TwitterDetails[]),
  );
