import ProgressBar from "progress";

let bar: ProgressBar | undefined = undefined;

export const onBlockAnalyzed = () => {
  bar?.tick();
};

export const start = (blocksToAnalyzeCount: number) => {
  if (blocksToAnalyzeCount === undefined) {
    throw new Error("blocks to analyze not known");
  }

  bar = new ProgressBar(">> [:bar] :rate/s :percent :etas", {
    total: blocksToAnalyzeCount,
  });

  const timer = setInterval(() => {
    if (bar?.complete) {
      clearInterval(timer);
    }
  }, 100);
};
