# Bowl Storage

Distributed cloud-native file storage system providing virtual disk lifecycle management and POSIX-compliant FUSE data access.

## Language

**Disk**:
A top-level virtual disk entity representing allocated storage capacity, Inode quota, and lifecycle state for a tenant.
_Avoid_: Volume, drive, bucket

**Mount Session**:
An active client connection context established upon mounting a virtual disk, identified by a unique session ID.
_Avoid_: Connection, mount point, client session

**OWMR (One-Writer Multi-Reader)**:
A concurrency access model where only one client holds the exclusive write lease while multiple clients mount concurrently in read-only mode.
_Avoid_: Multi-writer, shared-disk

**Write Lease**:
An exclusive, time-bounded lease granted to a single client permitting mutating file system operations on a virtual disk.
_Avoid_: Lock, write token, write permission

**Disk Attachment**:
The registered binding between a virtual disk and a client, recording mount mode and active lease duration.
_Avoid_: Disk mapping, mount binding, connection record

**Disk Metadata Schema**:
A dedicated PostgreSQL schema isolating JuiceFS metadata tables for a single virtual disk.
_Avoid_: Metadata database, disk namespace

**File Handle (fh)**:
A server-allocated unique identifier tracking an open file state, I/O mode, and lease ownership within a mount session.
_Avoid_: File descriptor, inode handle, file pointer

**Cascade Physical Purge**:
The asynchronous, eventually consistent reclamation of all physical data blocks in object storage and local NVMe staging triggered upon virtual disk deletion.
_Avoid_: Lazy delete, soft purge, partial cleanup

**Disk Cleanup Task**:
A persistent background task record tracking the physical purge progress and status of a deleted virtual disk across object storage and NVMe staging.
_Avoid_: GC job, delete thread, async hook


