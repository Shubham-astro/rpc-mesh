## Initial RPC mesh routing engine

- Round-robin load balancer
- Health check polling
- Prometheus metrics export
- Support for Devnet/Testnet/Mainnet


## Github Repo Structure
rpc-mesh/
├── main.go
├── go.mod
├── go.sum
├── router/
│   ├── load_balancer.go
│   ├── health_check.go
│   └── types.go
├── metrics/
│   ├── prometheus.go
│   └── collector.go
├── config/
│   └── config.go
├── tests/
│   ├── load_balancer_test.go
│   └── health_check_test.go
├── Dockerfile
├── docker-compose.yml
├── .gitignore
├── README.md
├── Makefile
└── docs/
├── ARCHITECTURE.md
└── DEPLOYMENT.md
