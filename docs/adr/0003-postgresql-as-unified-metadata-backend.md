# Use PostgreSQL as Unified Metadata Backend

Bowl requires metadata persistence for both high-level virtual disk management and low-level POSIX file system Inodes. We decided to use PostgreSQL as the unified metadata backend for both `DiskService` and the JuiceFS metadata engine within `StorageService`. This consolidates database infrastructure and backup procedures while providing robust transaction semantics and eliminating the operational complexity of managing disparate embedded databases.
