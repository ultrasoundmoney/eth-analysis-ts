export async function up(client) {
  await client`
    CREATE TABLE public.deflationary_streaks (
      block_number int NOT NULL,
      post_merge boolean NOT NULL,
      count int NOT NULL,
      CONSTRAINT deflationary_streaks_pk PRIMARY KEY (block_number,post_merge),
      CONSTRAINT deflationary_streaks_fk FOREIGN KEY (block_number) REFERENCES public.blocks("number")
    );
  `;

  await client`
    DELETE FROM key_value_store
    WHERE key = 'deflationary-streak-post-merge'
  `;

  await client`
    DELETE FROM key_value_store
    WHERE key = 'deflationary-streak-pre-merge'
  `;

  await client`
    DELETE FROM analysis_state
    WHERE key = 'deflationary-streaks'
  `;
}

export async function down(client) {
  await client`
    DROP TABLE deflationary_streaks
  `;
}
