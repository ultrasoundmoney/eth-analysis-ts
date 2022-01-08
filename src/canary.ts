import * as Duration from "./duration.js";
import * as Log from "./log.js";

type CanaryType = "block" | "leaderboard";
const cage: Record<CanaryType, NodeJS.Timeout | undefined> = {
  block: undefined,
  leaderboard: undefined,
};

const durationMilis = Duration.millisFromSeconds(180);

export const releaseCanary = (type: CanaryType): void => {
  cage[type] = setTimeout(() => {
    Log.alert(`canary dead, no block for ${durationMilis / 1000}s`);
  }, durationMilis);
};

export const resetCanary = (type: CanaryType) => {
  const timerId = cage[type];
  if (timerId) {
    timerId.refresh();
  }
};
