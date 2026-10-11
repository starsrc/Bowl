# Separate Control Plane and Data Plane into Independent Services

Bowl requires both tenant-facing disk lifecycle management and low-latency, high-throughput FUSE file system I/O. We decided to split `DiskService` (control plane) and `StorageService` (data plane) into independent microservice processes located in `services/disk` and `services/storage`. This ensures data plane I/O saturation or crashes do not compromise control plane administrative availability, and permits independent scaling and operational policies.
