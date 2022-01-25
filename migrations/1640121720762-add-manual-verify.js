export async function up(client) {
  await client`
    CREATE TABLE IF NOT EXISTS contracts (
      address text NOT NULL,
      "name" text NULL,
      is_bot bool NULL DEFAULT false,
      category text NULL,
      twitter_handle text NULL,
      image_url text NULL,
      on_chain_name text NULL,
      etherscan_name_tag text NULL,
      etherscan_name_token text NULL,
      opensea_name text NULL,
      opensea_twitter_handle text NULL,
      opensea_category text NULL,
      defi_llama_twitter_handle text NULL,
      defi_llama_category text NULL,
      manual_name text NULL,
      manual_twitter_handle text NULL,
      manual_category text NULL,
      opensea_image_url text NULL,
      twitter_image_url text NULL,
      twitter_name text NULL,
      twitter_description text NULL,
      opensea_contract_last_fetch timestamptz NULL,
      opensea_schema_name text NULL,
      last_leaderboard_entry timestamptz NULL,
      web3_name text NULL,
      supports_erc_721 bool NULL,
      supports_erc_1155 bool NULL,
      web3_supports_erc_721 bool NULL,
      web3_supports_erc_1155 bool NULL,
      mined_at timestamptz NULL,
      mined_at_block int4 NULL,
      force_metadata_fetch bool NULL,
      CONSTRAINT contracts_pkey PRIMARY KEY (address)
    )
  `;

  await client`
    CREATE INDEX IF NOT EXISTS force_metadata_fetch_idx ON public.contracts USING btree (force_metadata_fetch)
  `;

  await client`
    ALTER TABLE "contracts"
      ADD COLUMN last_manually_verified timestamptz;
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE "contracts"
      DROP COLUMN last_manually_verified;
  `;
}
