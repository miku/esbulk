module github.com/miku/esbulk

require (
	github.com/AdamKorcz/go-118-fuzz-build v0.0.0-20230306123547-8075edf89bb0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/containerd/containerd v1.7.0 // indirect
	github.com/docker/docker v20.10.23+incompatible // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/klauspost/compress v1.16.3 // indirect
	github.com/klauspost/pgzip v1.2.5
	github.com/moby/term v0.0.0-20221205130635-1aeaba878587 // indirect
	github.com/opencontainers/runc v1.1.4 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/segmentio/encoding v0.3.6
	github.com/sethgrid/pester v1.2.0
	github.com/testcontainers/testcontainers-go v0.17.0
	golang.org/x/tools v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20230322174352-cde4c949918d // indirect
	google.golang.org/grpc v1.54.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)

replace github.com/docker/docker => github.com/docker/docker v20.10.3-0.20221013203545-33ab36d6b304+incompatible // 22.06 branch

go 1.11
