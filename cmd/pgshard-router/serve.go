package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/andrew01234567890/pgshard/internal/cli"
	"github.com/andrew01234567890/pgshard/internal/pgwire"
)

// serve runs the wire-protocol front end against the fake executor. It exists
// so the protocol layer is exercisable end to end before the router's query
// engine lands; every session is trust-authenticated.
func serve(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runServe(ctx, args, stdout, stderr)
}

// runServe listens until ctx is cancelled, then drains and returns.
func runServe(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pgshard-router serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", "127.0.0.1:5432", "address to listen on")
	certFile := fs.String("tls-cert", "", "TLS certificate file (enables SSLRequest)")
	keyFile := fs.String("tls-key", "", "TLS private key file")
	drain := fs.Duration("drain-timeout", 30*time.Second, "time to wait for active queries on shutdown")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cli.ExitOK
		}
		return cli.ExitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "pgshard-router serve: unexpected argument %q\n", fs.Arg(0))
		return cli.ExitUsage
	}
	if (*certFile == "") != (*keyFile == "") {
		fmt.Fprintln(stderr, "pgshard-router serve: --tls-cert and --tls-key must be given together")
		return cli.ExitUsage
	}
	cfg := pgwire.Config{
		Authenticator: pgwire.TrustAuthenticator{},
		NewExecutor:   func(pgwire.SessionInfo) (pgwire.Executor, error) { return pgwire.NewFakeExecutor(), nil },
		ServerVersion: "18.6 (pgshard)",
		Logger:        slog.New(slog.NewTextHandler(stderr, nil)),
	}
	if *certFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
			return cli.ExitUsage
		}
		cfg.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}
	srv, err := pgwire.NewServer(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitUsage
	}
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitNotReady
	}
	fmt.Fprintf(stdout, "pgshard-router serve: listening on %s (fake executor)\n", l.Addr())
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(l) }()
	select {
	case err := <-errc:
		fmt.Fprintf(stderr, "pgshard-router serve: %v\n", err)
		return cli.ExitNotReady
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *drain)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintf(stderr, "pgshard-router serve: shutdown: %v\n", err)
	}
	<-errc
	return cli.ExitOK
}
