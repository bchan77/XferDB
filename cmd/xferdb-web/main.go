// Command xferdb-web serves the XferDB web UI. It is a separate process
// from `xferdb server`: it holds no migration state and no source/target DB
// credentials, so it stays up if the API/engine process crashes (e.g. OOM
// during a large migration). It serves the embedded static assets and
// reverse-proxies /api/* to the API server, so the browser only ever talks
// to this process's origin — no CORS configuration, and the API server can
// live on a private network unreachable from the browser directly.
package main

import (
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

//go:embed static
var staticFS embed.FS

func main() {
	addr := flag.String("addr", ":3000", "Address for the web UI to listen on")
	apiAddr := flag.String("api-addr", envOr("XFERDB_API_ADDR", "http://localhost:8080"),
		"Address of the XferDB API server to proxy /api requests to")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	flag.Parse()

	log := newLogger(*logLevel)

	target, err := url.Parse(*apiAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid --api-addr %q: %v\n", *apiAddr, err)
		os.Exit(1)
	}

	static, err := staticHandler()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/api/", apiProxy(target, log))
	mux.Handle("/", static)

	log.Info("web.started", "addr", *addr, "api_addr", target.String())
	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// apiProxy reverse-proxies /api/* (including the WebSocket upgrade at
// /api/v1/migrations/{id}/ws once that lands) to the XferDB API server.
func apiProxy(target *url.URL, log *slog.Logger) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Warn("api.proxy_error", "path", r.URL.Path, "error", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"api server unreachable"}`))
	}
	return proxy
}

// staticHandler serves the embedded SPA assets, falling back to index.html
// for paths with no matching file so client-side routing works.
func staticHandler() (http.Handler, error) {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("static fs: %w", err)
	}
	fileServer := http.FileServer(http.FS(sub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}
		if _, err := fs.Stat(sub, path); err != nil {
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/index.html"
			fileServer.ServeHTTP(w, r2)
			return
		}
		fileServer.ServeHTTP(w, r)
	}), nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}
