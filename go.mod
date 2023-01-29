module github.com/miku/esbulk

require (
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/containerd/containerd v1.6.16 // indirect
	github.com/docker/docker v20.10.23+incompatible // indirect
	github.com/klauspost/compress v1.15.15 // indirect
	github.com/klauspost/pgzip v1.2.5
	github.com/moby/term v0.0.0-20221205130635-1aeaba878587 // indirect
	github.com/opencontainers/runc v1.1.4 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/segmentio/encoding v0.3.6
	github.com/sethgrid/pester v1.2.0
	github.com/testcontainers/testcontainers-go v0.17.0
	golang.org/x/tools v0.5.0 // indirect
	google.golang.org/genproto v0.0.0-20230127162408-596548ed4efa // indirect
	google.golang.org/grpc v1.52.3 // indirect
)

replace github.com/docker/docker => github.com/docker/docker v20.10.3-0.20221013203545-33ab36d6b304+incompatible // 22.06 branch

go 1.11
