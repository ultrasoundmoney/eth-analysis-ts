import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function";
import fs from "fs/promises";
// eslint-disable-next-line node/no-unpublished-import
import { parseHTML } from "linkedom";
import fetch from "node-fetch";
// eslint-disable-next-line node/no-unpublished-import
import puppeteer, { Browser, Page } from "puppeteer";
import { URL } from "url";
import * as BaseFees from "./base_fees.js";
import { BaseFeeBurner, BlockBaseFees } from "./base_fees.js";
import { sql } from "./db.js";
import { delay } from "./delay.js";
import * as Log from "./log.js";

const getBaseFeeBurners = async () => {
  const baseFeesPerBlock = await sql<{ baseFees: BlockBaseFees }[]>`
      SELECT base_fees
      FROM base_fees_per_block
  `.then((rows) => {
    return rows.map((row) => row.baseFees);
  });

  const contractNameMap = await BaseFees.getContractNameMap();

  return pipe(
    baseFeesPerBlock,
    A.map((baseFees) => baseFees.contract_use_fees),
    // We merge Record<address, baseFees>[] here.
    A.reduce({} as Record<string, number>, (agg, contractBaseFeeMap) => {
      Object.entries(contractBaseFeeMap as Record<string, number>).forEach(
        ([address, fee]) => {
          const sum = agg[address] || 0;
          agg[address] = sum + fee;
        },
      );
      return agg;
    }),
    Object.entries,
    A.map(([address, fees]) => ({
      address,
      fees,
      id: address,
      image: undefined,
      name: contractNameMap[address],
    })),
    A.sort<BaseFeeBurner>({
      compare: (first, second) =>
        first.fees === second.fees ? 0 : first.fees > second.fees ? -1 : 1,
      equals: (first, second) => first.fees === second.fees,
    }),
  );
};

let browser: Browser | undefined = undefined;

const fetchEtherscanName = async (
  address: string,
): Promise<string | undefined> => {
  const html = await fetch(`https://blockscan.com/address/${address}`).then(
    (res) => res.text(),
  );
  const { document } = parseHTML(html);
  const etherscanPublicName = document.querySelector(".badge-secondary") as {
    innerText: string;
  } | null;

  return etherscanPublicName?.innerText;
};

let googlePage: Page | undefined = undefined;
const guessOriginFromName = async (name: string): Promise<string> => {
  if (browser === undefined) {
    browser = await puppeteer.launch();
  }

  if (googlePage === undefined) {
    googlePage = await browser.newPage();
    await googlePage.setUserAgent(
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    );
  }

  await googlePage.goto(`https://www.google.com/search?q=${name}`);

  const firstResultUrl = (await googlePage.$eval(
    // eslint-disable-next-line quotes
    "#search a",
    (e) => (e as { href: string }).href,
  )) as unknown as string;

  const possibleOrigin = new URL(firstResultUrl).origin;

  return possibleOrigin;
};

let findIconPage: Page | undefined = undefined;

const findIconUrl = async (origin: string): Promise<string | undefined> => {
  const healMaybeUrl = (mUrl: string) =>
    mUrl.startsWith("http")
      ? mUrl
      : mUrl.startsWith("/")
      ? `${origin}${mUrl}`
      : `${origin}/${mUrl}`;

  try {
    const mManifestRes = await fetch(`${origin}/manifest.webmanifest`);
    if (mManifestRes.status === 200) {
      Log.debug("> /manifest.webmanifest found!");
      try {
        const manifest = await mManifestRes.json();
        const mIcon: { src: string } | undefined = manifest?.icons?.pop();
        if (mIcon !== undefined) {
          return healMaybeUrl(mIcon.src);
        }
      } catch {
        Log.debug(`> error on manifest json decode for ${origin}`);
      }
    }

    const mManifest2Res = await fetch(`${origin}/manifest.json`);
    if (mManifest2Res.status === 200) {
      Log.debug("> /manifest.json found");
      try {
        const manifest2 = await mManifest2Res.json();
        const mIcon = manifest2?.icons?.pop();
        if (mIcon !== undefined) {
          return healMaybeUrl(mIcon.src);
        }
      } catch {
        Log.debug(`> error on manifest json decode for ${origin}`);
      }
    }

    const mManifest3Res = await fetch(`${origin}/site.webmanifest`);
    if (mManifest3Res.status === 200) {
      Log.debug("> /manifest.json found");
      try {
        const manifest3 = await mManifest3Res.json();
        const mIcon = manifest3?.icons?.pop();
        if (mIcon !== undefined) {
          return healMaybeUrl(mIcon.src);
        }
      } catch {
        Log.debug(`> error on manifest json decode for ${origin}`);
      }
    }

    // Some sites use proctection against reading the manifest programatically 🙈.
    if (browser === undefined) {
      browser = await puppeteer.launch();
    }

    if (findIconPage === undefined) {
      findIconPage = await browser.newPage();
      // Bypasses cloudflare detection.
      await findIconPage.setUserAgent(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
      );
    }

    try {
      const res = await findIconPage.goto(`${origin}/manifest.json`);
      if (res.status() === 200) {
        Log.debug("> found /manifest.json with puppeteer");
        try {
          const manifest4 = await res.json();
          const mIcon = manifest4?.icons?.pop();
          if (mIcon !== undefined) {
            return healMaybeUrl(mIcon.src);
          }
        } catch {
          Log.debug("> error decoding manifest.json found by puppeteer");
        }
      }
    } catch {
      Log.debug("> error navigating to manifest page");
    }

    const html = await fetch(origin).then((res) => res.text());
    const { document } = parseHTML(html);
    const mIconUrl =
      // eslint-disable-next-line quotes
      document.querySelector(`link[rel="icon"]`) as { href: string } | null;
    if (typeof mIconUrl?.href === "string") {
      // eslint-disable-next-line quotes
      Log.debug(`> link tag with rel="icon" found!`);
      return healMaybeUrl(mIconUrl.href);
    }

    const mIcon2Url =
      // eslint-disable-next-line quotes
      document.querySelector(`link[rel="shortcut icon"]`) as {
        href: string;
      } | null;
    if (typeof mIcon2Url?.href === "string") {
      // eslint-disable-next-line quotes
      Log.debug(`> link tag with rel="shortcut icon" found!`);
      return healMaybeUrl(mIcon2Url.href);
    }
  } catch (error) {
    Log.error("> unexpected error finding icon", error);
  }

  return undefined;
};

const identifyContracts = async () => {
  const baseFeeBurners = await getBaseFeeBurners();
  Log.info(`> ${baseFeeBurners.length} contracts to identify`);
  Log.info("> limiting to top 1000");
  for (const baseFeeBurner of baseFeeBurners.slice(0, 1000)) {
    // Woah, slow down cowboy 🤠! Unfortunately we start with a delay as there are continue statements below preventing us from delaying at the end.
    await delay(4000);

    Log.info(
      `> trying to identify ${baseFeeBurner.name} - ${baseFeeBurner.address}`,
    );
    const name = await fetchEtherscanName(baseFeeBurner.address);
    if (name === undefined) {
      Log.warn(
        `> failed to find etherscan name for ${baseFeeBurner.name} - ${baseFeeBurner.address}`,
      );
      continue;
    }

    // Better odds guessing the origin without the contract bit. We usually end up on etherscan if we include that.
    const colonIndex = name.indexOf(":");
    const shortname = name.includes(":") ? name.slice(0, colonIndex) : name;
    Log.debug(`> shortname: ${shortname}`);

    const getImageExists = async () => {
      try {
        await fs.access(`dapp_image_guesses/${shortname}.png`);
        return true;
      } catch {
        // do nothing
      }

      try {
        await fs.access(`dapp_image_guesses/${shortname}.ico`);
        return true;
      } catch {
        // do nothing
      }

      try {
        await fs.access(`dapp_image_guesses/${shortname}.svg`);
        return true;
      } catch {
        // do nothing
      }

      return false;
    };

    // If we have an image already let's skip.
    if (await getImageExists()) {
      continue;
    }

    const origin = await guessOriginFromName(shortname);
    Log.debug(`> origin: ${origin}`);

    const iconUrl = await findIconUrl(origin);
    if (iconUrl === undefined) {
      Log.warn(
        `> failed to find icon for ${baseFeeBurner.name} - ${baseFeeBurner.address}`,
      );
      continue;
    }
    Log.debug(`> icon: ${iconUrl}`);

    const iconRes = await fetch(iconUrl);
    if (iconRes.status === 200) {
      const iconBuffer = await iconRes.buffer();
      const imageExt = new URL(iconUrl).pathname.slice(-3);
      const imageName = `${shortname
        .toLowerCase()
        .replaceAll(" ", "-")}.${imageExt}`;
      Log.info(`> found ${imageName}`);
      await fs.writeFile(`dapp_image_guesses/${imageName}`, iconBuffer);
      continue;
    }

    Log.warn(
      `> failed to fetch iconUrl for ${baseFeeBurner.name} - ${baseFeeBurner.address}`,
    );
  }

  if (browser !== undefined) {
    await browser.close();
  }
  await sql.end();
};

identifyContracts();
