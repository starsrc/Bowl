# Synchronize Data Plane Mount Operations with Control Plane Attachments

`DiskService` serves as the authoritative source for tenant-facing disk status, while `StorageService` manages active FUSE client sessions. We decided that `StorageService.Mount` and `StorageService.Unmount` synchronously invoke `DiskService.AttachDisk` and `DiskService.DetachDisk` respectively. This guarantees that control plane status queries (`GetDisk`, `ListDisks`) accurately reflect live client attachments and active write lease holders without asynchronous reconciliation lag.
