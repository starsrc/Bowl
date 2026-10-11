# Use Connect-RPC for Unified Transport Protocol

Clients require efficient RPC communication over both standard gRPC and HTTP/1.1 REST/JSON. We decided to expose all service endpoints using Connect-RPC (`connectrpc.com/connect/v2`) mounted on Go's standard `net/http` server with HTTP/2 enabled. This delivers native compatibility with gRPC clients and browser/debugging HTTP clients over a single port without requiring separate gateway proxies.
