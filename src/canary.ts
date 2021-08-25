import { milisFromSeconds } from "./duration.js";

type CanaryType = "block" | "leaderboard";
const cage: Record<CanaryType, NodeJS.Timeout | undefined> = {
  block: undefined,
  leaderboard: undefined,
};

const durationMilis = milisFromSeconds(120);

export const releaseCanary = (type: CanaryType): void => {
  cage[type] = setTimeout(() => {
    throw new Error(`canary dead, no block for ${durationMilis / 1000}s`);
  }, durationMilis);
};

export const resetCanary = (type: CanaryType) => {
  const timerId = cage[type];
  if (timerId) {
    timerId.refresh();
  }
};
