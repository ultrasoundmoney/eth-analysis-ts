export class RateLimitError extends Error {}
export class JsonDecodeError extends Error {}

export const decodeErrorFromUnknown = (e: unknown): JsonDecodeError =>
  e instanceof Error ? e : new JsonDecodeError(String(e));
