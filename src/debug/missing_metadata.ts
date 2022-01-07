import fetch from "node-fetch";
const defillamaProtocols = await fetch("https://api.llama.fi/protocols")
  .then((res) => res.json())
  .then((body: any) =>
    body.filter((protocol: any) => typeof protocol.address === "string"),
  )
  .then((protocols: any[]) =>
    protocols.reduce(
      (map: Map<string, any>, protocol: any) =>
        map.set(protocol.address.toLowerCase(), protocol),
      new Map(),
    ),
  );
const res = await fetch("https://api.ultrasound.money/fees/burn-leaderboard");

const body = (await res.json()) as any;

const keys = [
  "leaderboard1h",
  "leaderboard5m",
  "leaderboard7d",
  "leaderboard24h",
  "leaderboard30d",
  "leaderboardAll",
];

for (const key of keys) {
  const entries = body[key];
  const contractsWithoutCategory = entries.filter(
    (entry: any) => entry.category === null && entry.name !== entry.address,
  );
  console.log(`entries without category in ${key}`);
  for (const entry of contractsWithoutCategory) {
    console.log(`contract: ${entry.address}`);
    console.log(`https://etherscan.io/address/${entry.address}`);
    console.log(
      `https://api.opensea.io/api/v1/asset_contract/${entry.address}`,
    );
    console.log(
      `defillama metadata exists: ${defillamaProtocols.has(entry.address)}`,
    );
    console.log();
  }
}
