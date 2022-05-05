export async function up(client) {
  await client`
    CREATE TABLE eth_in_validators (
      date_at date NOT NULL,
      gwei numeric NOT NULL,
      CONSTRAINT eth_in_validators_pk PRIMARY KEY ("date_at")
    );
  `;
}

export async function down(client) {
  await client`
    DROP TABLE eth_in_validators
  `;
}
