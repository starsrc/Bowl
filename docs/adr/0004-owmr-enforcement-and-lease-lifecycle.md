# Enforce OWMR via Partial Unique Index and Lease Timeouts

The One-Writer Multi-Reader (OWMR) access model prohibits concurrent mutating mounts across clients. We decided to enforce write exclusivity in PostgreSQL using a partial unique index on `disk_attachments (disk_id) WHERE read_only = false` alongside a 10-second lease timeout with 3-second heartbeat intervals in `StorageService`. This guarantees write exclusivity at the database schema level and ensures automatic lease recovery if a writer node terminates abruptly.
