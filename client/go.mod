module github.com/starsrc/bowl/client

go 1.26.5

replace github.com/starsrc/bowl => ..

require (
	connectrpc.com/connect v1.21.1
	github.com/google/uuid v1.6.0
	github.com/hanwen/go-fuse/v2 v2.12.1
	github.com/starsrc/bowl v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.12.1
	golang.org/x/net v0.62.0
	golang.org/x/sys v0.49.0
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.2 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260928230214-8a89bd6388cc // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
