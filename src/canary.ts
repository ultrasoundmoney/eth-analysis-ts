import { milisFromSeconds } from "./duration.js";

let timerId: NodeJS.Timeout | undefined = undefined;

const durationMilis = milisFromSeconds(120);

export const renewCanary = () => {
  if (timerId) {
    clearTimeout(timerId);
  }

  timerId = setTimeout(() => {
    throw new Error(`canary dead, no block for ${durationMilis / 1000}s`);
  }, durationMilis);
};
