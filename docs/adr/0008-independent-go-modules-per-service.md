# Isolate Service Modules via Independent Go Modules

`StorageService` relies on JuiceFS and its extensive dependency tree, whereas `DiskService` requires only database access and Connect-RPC primitives. We decided to structure `services/disk` and `services/storage` as independent Go modules (`go.mod`), coordinated via the root `go.work`. This keeps the control plane lightweight, minimizes attack surfaces, and prevents JuiceFS dependencies from bloating the administrative microservice.
