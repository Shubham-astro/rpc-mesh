package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shubham-astro/rpc-mesh/config"
	"github.com/shubham-astro/rpc-mesh/router"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error:\n%v", err)
	}

	pool, err := router.NewPool(cfg)
	if err != nil {
		log.Fatalf("pool error: %v", err)
	}

	mux := http.NewServeMux()

	// Liveness: is this process up? Deliberately does NOT depend on upstream
	// health — if all upstreams are down, the orchestrator restarting
	// rpc-mesh fixes nothing and just adds churn.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		resp := struct {
			MaxSlot   uint64                 `json:"max_slot"`
			Endpoints []router.EndpointState `json:"endpoints"`
		}{
			MaxSlot:   pool.MaxSlot(),
			Endpoints: pool.Snapshot(),
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(resp); err != nil {
			log.Printf("status encode: %v", err)
		}
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	// NotifyContext cancels ctx on SIGINT/SIGTERM. Container orchestrators
	// send SIGTERM then SIGKILL after a grace period — draining in between
	// is what makes deploys not drop requests.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("rpc-mesh listening on :%s with %d endpoint(s)", cfg.Port, len(cfg.Endpoints))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutdown signal received, draining…")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
		os.Exit(1)
	}
	log.Println("stopped cleanly")
}