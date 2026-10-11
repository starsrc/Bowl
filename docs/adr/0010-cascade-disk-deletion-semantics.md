# Enforce Protected and Force Cascading Deletion for Disks

Deleting active virtual disks risks catastrophic I/O errors and corrupts running applications. We decided that `DeleteDisk` with `force = false` rejects deletion when active client attachments exist. When `force = true`, the service forcefully revokes attachments, purges database records, drops the associated PostgreSQL schema (`disk_<uuid>`) with `CASCADE`, and deletes local slice directories to reclaim disk capacity.
