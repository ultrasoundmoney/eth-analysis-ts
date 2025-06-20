import * as Retry from "retry-ts";
import urlcatM from "urlcat";
import { getTwitterToken } from "./config.js";
import * as Fetch from "./fetch.js";
import { B, O, pipe, TE, TO } from "./fp.js";
import * as Log from "./log.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

type UserTwitterApiRaw = {
  description: string | null;
  id: string;
  name: string;
  profile_image_url: string;
};

const makeProfileByUsernameUrl = (handle: string) =>
  urlcat("https://api.twitter.com", "/2/users/by/username/:username", {
    username: handle,
    "user.fields": ["profile_image_url", "name", "description"].join(","),
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

// Fetching profiles is on a 900 / 15min rate-limit, or 1/s.
// export const fetchProfileQueue = new PQueue({
//   concurrency: 2,
//   intervalCap: 10,
//   interval: Duration.millisFromSeconds(10),
// });

export class GetProfileApiBadResponseError extends Error {}
export class GetProfileApiError extends Error {}
export class InvalidHandleError extends Error {}
export class ProfileNotFoundError extends Error {}
export class UnexpectedJsonResponse extends Error {}
export type GetProfileByHandleError =
  | GetProfileApiBadResponseError
  | GetProfileApiError
  | InvalidHandleError
  | ProfileNotFoundError
  | UnexpectedJsonResponse;

export const getProfileByHandle = (handle: string) =>
  pipe(
    Fetch.fetchWithRetry(
      makeProfileByUsernameUrl(handle),
      {
        headers: {
          Authorization: `Bearer ${getTwitterToken()}`,
        },
      },
      {
        acceptStatuses: [200, 404],
        retryPolicy: Retry.Monoid.concat(
          Retry.exponentialBackoff(2000),
          Retry.limitRetries(9),
        ),
      },
    ),
    TE.chainW((res) => {
      // If a handle invalid chars twitter will return a 404 without a body.
      if (res.status === 404) {
        return TE.left(
          new InvalidHandleError(
            `fetch twitter profile ${handle}, invalid handle, 404`,
          ),
        );
      }

      if (res.status !== 200) {
        return TE.left(
          new GetProfileApiBadResponseError(
            `fetch twitter profile ${handle}, bad response ${res.status}`,
          ),
        );
      }

      return pipe(
        Fetch.decodeJsonResponse(res),
        TE.map((body) => body as ApiWrapper<UserTwitterApiRaw>),
      );
    }),
    TE.chainW((body) => {
      if ("errors" in body) {
        // Twitter Api can return multiple errors but let us deal with one at a time.
        const apiError = body.errors[0];
        if (apiError.type === apiErrorTypes.notFound) {
          return TE.left(
            new ProfileNotFoundError(
              `fetch twitter profile ${handle}, valid handle, but not found, 404`,
            ),
          );
        }
        return TE.left(
          new GetProfileApiError(
            `fetch twitter profile ${handle}, API error, ${apiError.title}, ${apiError.detail}`,
          ),
        );
      }

      return TE.right(body.data);
    }),
  );

export const getProfileImage = (profile: UserTwitterApiRaw) =>
  pipe(
    typeof profile?.profile_image_url === "string" &&
      profile.profile_image_url.length !== 0,
    B.match(
      () => TO.none,
      () =>
        pipe(
          Fetch.fetch(profile.profile_image_url.replace("normal", "400x400")),
          TE.map(() => profile.profile_image_url.replace("normal", "400x400")),
          TE.alt(() =>
            pipe(
              Fetch.fetch(
                profile.profile_image_url.replace("normal", "reasonably_small"),
              ),
              TE.map(() =>
                profile.profile_image_url.replace("normal", "reasonably_small"),
              ),
            ),
          ),
          TE.alt(() =>
            pipe(
              Fetch.fetch(profile.profile_image_url),
              TE.map(() => profile.profile_image_url),
            ),
          ),
          TE.match(
            (e) => {
              Log.error("expected to find at least one working image", e);
              return O.none;
            },
            (profileImageUrl) => O.some(profileImageUrl),
          ),
        ),
    ),
  );
