import fetch from "node-fetch";
import * as Duration from "./duration.js";
import * as Log from "./log.js";

const opsGenieApiKey = "5ee39424-ba9e-4206-93f8-50a240ab84b2";

type CanaryType = "block" | "leaderboard";
const cage: Record<CanaryType, NodeJS.Timeout | undefined> = {
  block: undefined,
  leaderboard: undefined,
};

const durationMilis = Duration.millisFromSeconds(180);

export const releaseCanary = (type: CanaryType): void => {
  cage[type] = setTimeout(async () => {
    Log.alert(`canary dead, no block for ${durationMilis / 1000}s`);

    const res = await fetch("https://api.opsgenie.com/v2/alerts", {
      method: "POST",
      headers: {
        Authorization: `GenieKey ${opsGenieApiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        message: "no block for more than 3 minutes!",
      }),
    });

    if (!res.ok) {
      const body = (await res.json()) as {
        message: string;
        took: number;
        requestId: string;
      };
      throw new Error(`OpsGenie alert request failed! ${body.message}`);
    }
  }, durationMilis);
};

export const resetCanary = (type: CanaryType) => {
  const timerId = cage[type];
  if (timerId) {
    timerId.refresh();
  }
};
