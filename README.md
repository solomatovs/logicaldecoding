# Logical Replication

A project to play with and test Postgres logical replication with Rust.

## What

The main test is in `types/mod.rs`.

This test attempts to perform deterministic simulation by first attaching the `logicalreplication` listener to an empty database then:

1. Deterministically produce random batches of transactions and maintaining their impact on an in-memory representation of the table.
2. Applying the batched transactions to the Postgres database.
3. Listening to the logical replication stream and trying to apply them to a second in-memory representation of the table.
4. Stopping the test after `n` iterations and then testing that all three representations align.

## How to

1. Start postgres with logical replication mode - see the `docker-compose.yaml` and the `Dockerfile` for configuration.
2. Run `cargo install sqlx-cli` to set up the [sqlx](https://github.com/launchbadge/sqlx) command line utility to allow database migrations.
3. Run `sqlx migrate run` to set up the intial database.
4. Run `cargo test`.

## Acknowledgements

Thank you to:

- `rust-postgres`: https://github.com/sfackler/rust-postgres/issues/116
- `postgres-decoderbufs`: https://github.com/debezium/postgres-decoderbufs
- this example: https://github.com/debate-map/app/blob/afc6467b6c6c961f7bcc7b7f901f0ff5cd79d440/Packages/app-server-rs/src/pgclient.rs