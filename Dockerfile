# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build
WORKDIR /src

# Dependency layer cached separately from source. Editing a .go file
# doesn't re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a static binary with no libc dependency, which is
# what lets the final stage be a scratch-like image.
# -s -w strips the symbol table and DWARF data — roughly 30% smaller, and
# nothing you'd use in production debugging (pprof works fine without them).
RUN CGO_ENABLED=0 GOOS=linux go build \
      -ldflags="-s -w" \
      -trimpath \
      -o /out/rpc-mesh .

FROM cgr.dev/chainguard/static:latest

# Chainguard static ships CA certificates and a nonroot user. The certs are
# not optional here — every upstream is HTTPS, and a scratch image without
# them fails with "x509: certificate signed by unknown authority" on the
# first probe, which reads like a network problem and isn't.
COPY --from=build /out/rpc-mesh /rpc-mesh

EXPOSE 8080
USER nonroot
ENTRYPOINT ["/rpc-mesh"]