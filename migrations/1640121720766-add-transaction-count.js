export async function up(client) {
  await client`
    CREATE TABLE IF NOT EXISTS contract_base_fees (
      block_number int4 NOT NULL,
      contract_address text NOT NULL,
      base_fees float8 NULL,
      CONSTRAINT contract_burn_pkey PRIMARY KEY (block_number, contract_address),
      CONSTRAINT contract_burn_block_number_fkey FOREIGN KEY (block_number) REFERENCES public.blocks("number"),
      CONSTRAINT contract_burn_contract_address_fkey FOREIGN KEY (contract_address) REFERENCES public.contracts(address)
    );
  `;
  await client`
    ALTER TABLE contract_base_fees
    ADD COLUMN transaction_count float8
  `;
}

export async function down(client) {
  await client`
    ALTER TABLE contract_base_fees
    DROP COLUMN transaction_count
  `;
}
