import { FeeBreakdown, sumFeeMaps } from "./base_fees";
import { sql } from "./db";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import { pipe } from "fp-ts/lib/function.js";
import A from "fp-ts/lib/Array.js";
import ObjectsToCsv from "objects-to-csv";

const exportDelphiData = async () => {
  const targetContracts = [
    "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b",
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
    "0x1a2a1c938ce3ec39b6d47113c7955baa9dd454f2",
    "0xdac17f958d2ee523a2206206994597c13d831ec7",
    "0xe592427a0aece92de3edee1f18e0157c05861564",
    "0xad9fd7cb4fc7a0fbce08d64068f60cbde22ed34c",
    "0x881d40237659c251811cec9c364ef91dc08d300c",
    "0x4a8b01e437c65fa8612e8b699266c0e0a98ff65c",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    "0xe4cfae3aa41115cb94cff39bb5dbae8bd0ea9d41",
  ];

  const cMap = {
    opensea: "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b",
    univ2: "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
    axie: "0x1a2a1c938ce3ec39b6d47113c7955baa9dd454f2",
    tether: "0xdac17f958d2ee523a2206206994597c13d831ec7",
    univ3: "0xe592427a0aece92de3edee1f18e0157c05861564",
    vox: "0xad9fd7cb4fc7a0fbce08d64068f60cbde22ed34c",
    metamask: "0x881d40237659c251811cec9c364ef91dc08d300c",
    spacepoggers: "0x4a8b01e437c65fa8612e8b699266c0e0a98ff65c",
    usdcoin: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
    punks: "0xe4cfae3aa41115cb94cff39bb5dbae8bd0ea9d41",
  };

  const names: Record<string, string> = {};
  for (const address of targetContracts) {
    const name = await sql<
      { name: string }[]
    >`SELECT name FROM contracts WHERE address = ${address}`.then(
      (rows) => rows[0].name,
    );
    names[address] = name;
  }

  const feesPerDayRaw = await sql<{ fees: number; date: Date }[]>`
    SELECT SUM(base_fee_sum) AS fees, DATE_TRUNC('day', mined_at) AS date
    FROM blocks
    GROUP BY date
  `;
  const feesPerDay = pipe(
    feesPerDayRaw,
    NEA.groupBy((match) => String(match.date.getTime())),
  );

  const blocks = await sql<{ baseFees: FeeBreakdown; date: Date }[]>`
    SELECT base_fees, DATE_TRUNC('day', mined_at) AS date
    FROM blocks
  `;

  const segmentedBlocks = pipe(
    blocks,
    NEA.groupBy((match) => String(match.date.getTime())),
  );

  const rows = [];
  for (const [day, sBlocks] of Object.entries(segmentedBlocks)) {
    const dayFees = feesPerDay[day];
    const sumMap = pipe(
      sBlocks,
      A.map((sBlock) => sBlock.baseFees.contract_use_fees),
      sumFeeMaps,
    );

    const openseaFees = sumMap[cMap.opensea] || 0;
    const univ2 = sumMap[cMap.univ2] || 0;
    const axie = sumMap[cMap.axie] || 0;
    const tether = sumMap[cMap.tether] || 0;
    const univ3 = sumMap[cMap.univ3] || 0;
    const vox = sumMap[cMap.vox] || 0;
    const metamask = sumMap[cMap.metamask] || 0;
    const spacepoggers = sumMap[cMap.spacepoggers] || 0;
    const usdcoin = sumMap[cMap.usdcoin] || 0;
    const punks = sumMap[cMap.punks] || 0;
    const totalTop =
      openseaFees +
      univ2 +
      axie +
      tether +
      univ3 +
      vox +
      metamask +
      spacepoggers +
      usdcoin +
      punks;

    const totalAll = dayFees[0].fees;
    const totalWithoutTop = totalAll - totalTop;

    rows.push({
      day,
      totalAll,
      totalWithoutTop,
      openseaFees,
      univ2,
      axie,
      tether,
      univ3,
      vox,
      metamask,
      spacepoggers,
      usdcoin,
      punks,
    });
  }

  const csv = new ObjectsToCsv(rows);
  csv.toDisk("./delphi.csv");
};

exportDelphiData();
