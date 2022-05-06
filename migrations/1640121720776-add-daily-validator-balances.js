export async function up(client) {
  await client`
    CREATE TABLE eth_in_validators (
      timestamp timestamptz NOT NULL,
      gwei numeric NOT NULL,
      CONSTRAINT eth_in_validators_pk PRIMARY KEY ("timestamp")
    );
  `;
}

export async function down(client) {
  await client`
    DROP TABLE eth_in_validators
  `;
}
