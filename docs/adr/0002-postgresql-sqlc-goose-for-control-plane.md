# Use PostgreSQL, sqlc, and goose for Control Plane Persistence

The virtual disk control plane (`DiskService`) requires ACID guarantees for quota, lease, and disk lifecycle state transitions. We decided to use PostgreSQL as the primary database, sqlc for type-safe SQL query code generation, and goose for database schema migration management. This aligns with existing starsrc ecosystem conventions, guarantees zero-reflection query performance, and enforces chronological, reproducible database migrations.
