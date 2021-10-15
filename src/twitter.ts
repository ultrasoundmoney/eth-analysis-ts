import * as Duration from "./duration.js";
import * as Log from "./log.js";
import fetch from "node-fetch";
import { getTwitterToken } from "./config.js";
import urlcatM from "urlcat";
import PQueue from "p-queue";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

type UserTwitterApiRaw = {
  profile_image_url: string;
};

const getProfileImage = (
  profile: UserTwitterApiRaw | undefined,
): string | undefined =>
  typeof profile?.profile_image_url === "string"
    ? profile.profile_image_url.replace("normal", "reasonably_small")
    : undefined;

const makeProfileByUsernameUrl = (handle: string) =>
  urlcat("https://api.twitter.com", "/2/users/by/username/:username", {
    username: handle,
    "user.fields": ["profile_image_url"].join(","),
  });

const apiErrorTypes = {
  notFound: "https://api.twitter.com/2/problems/resource-not-found",
} as const;

type ApiWrapper<A> = { data: A } | { errors: ApiError[] };

type ApiError = {
  title: string;
  type: string;
  detail: string;
  value?: string;
};

export const fetchProfileQueue = new PQueue({
  concurrency: 2,
  intervalCap: 10,
  interval: Duration.milisFromSeconds(10),
});

// Fetching profiles is on a 900 / 15min rate-limit, or 1/s.
export const getProfileByHandle = async (
  handle: string,
): Promise<UserTwitterApiRaw | undefined> => {
  const res = await fetchProfileQueue.add(() =>
    fetch(makeProfileByUsernameUrl(handle), {
      headers: {
        Authorization: `Bearer ${getTwitterToken()}`,
      },
    }),
  );

  // If a handle invalid chars twitter will return a 404 without a body.
  if (res.status === 404) {
    Log.warn(`fetch twitter profile ${handle}, invalid handle, 404`);
    return undefined;
  }

  if (res.status !== 200) {
    Log.error(`fetch twitter profile ${handle}, bad response ${res.status}`);
    return undefined;
  }

  // A 200 response may still contain Api Errors
  const body = (await res.json()) as ApiWrapper<UserTwitterApiRaw>;

  if ("errors" in body) {
    // Twitter Api can return multiple errors but let us deal with one at a time.
    const apiError = body.errors[0];
    if (apiError.type === apiErrorTypes.notFound) {
      Log.warn(
        `fetch twitter profile ${handle}, valid handle, but not found, 404`,
      );
      return undefined;
    }
    Log.error(
      `fetch twitter profile ${handle}, API error, ${apiError.title}, ${apiError.detail}`,
    );
    return undefined;
  }

  if (body.data === undefined) {
    Log.error("fetch twitter profile ${handle}, unexpected json response", {
      body,
    });
    return undefined;
  }

  return body.data;
};

export const getImageByHandle = async (
  handle: string,
): Promise<string | undefined> => {
  const profile = await getProfileByHandle(handle);
  return getProfileImage(profile);
};
