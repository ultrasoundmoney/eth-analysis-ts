export async function up(client) {
  await client`
    TRUNCATE TABLE beacon_issuance;
  `;

  await client`
    TRUNCATE TABLE eth_in_validators;
  `;

  await client`
    TRUNCATE TABLE beacon_states;
  `;
}

export async function down() {}
