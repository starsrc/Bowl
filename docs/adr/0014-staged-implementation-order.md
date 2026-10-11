# Implement Control Plane Before Data Plane

`StorageService` requires an authoritative registration source for virtual disk validation, capacity quota checks, and lease attachment records. We decided to implement `DiskService` (`services/disk`) first, complete with database migrations, sqlc queries, and Connect-RPC endpoints, before proceeding to `StorageService` (`services/storage`). This establishes a verified control plane foundation and unblocks end-to-end lease synchronization tests for the subsequent data plane.
