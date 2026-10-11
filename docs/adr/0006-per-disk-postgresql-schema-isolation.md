# Isolate JuiceFS Metadata Tables per Disk via PostgreSQL Schemas

Each virtual disk maintains an independent Inode hierarchy, directory tree, and slice indices. We decided to isolate JuiceFS metadata for each virtual disk in an individual PostgreSQL schema named `disk_<uuid>`. This avoids table name collisions, eliminates the operational overhead of spawning discrete databases, and enables clean tenant data pruning via `DROP SCHEMA CASCADE` upon disk deletion.
