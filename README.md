## Initial RPC mesh routing engine

- Round-robin load balancer
- Health check polling
- Prometheus metrics export
- Support for Devnet/Testnet/Mainnet


## Github Repo Structure
rpc-mesh/
├── main.go
├── router/
│   ├── load_balancer.go
│   └── health_check.go
├── metrics/
│   └── prometheus.go
├── Dockerfile
├── docker-compose.yml
└── README.md
