package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fourjuaneight/at-cleanup/dashboard"
	"github.com/fourjuaneight/at-cleanup/pkg/endpoints"
	"github.com/fourjuaneight/at-cleanup/pkg/store"
	"github.com/fourjuaneight/at-cleanup/telemetry"
	"github.com/fourjuaneight/at-cleanup/version"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	slogecho "github.com/samber/slog-echo"
	"github.com/urfave/cli/v2"
	"golang.org/x/time/rate"
)

func main() {
	app := cli.App{
		Name:    "search",
		Usage:   "bluesky repo cleanup service",
		Version: version.String(),
	}

	app.Flags = []cli.Flag{
		telemetry.CLIFlagDebug,
		&cli.StringFlag{
			Name:    "listen-address",
			Usage:   "listen address for HTTP server",
			Value:   "0.0.0.0:8080",
			EnvVars: []string{"LISTEN_ADDRESS"},
		},
		&cli.StringFlag{
			Name:    "database-url",
			Usage:   "postgres connection string",
			Value:   "postgres://bsky:bsky@localhost:5432/bsky?sslmode=disable",
			EnvVars: []string{"DATABASE_URL"},
		},
		&cli.StringFlag{
			Name:    "magic-header-val",
			Usage:   "magic header value for protected endpoints",
			Value:   "",
			EnvVars: []string{"MAGIC_HEADER_VAL"},
		},
		&cli.StringFlag{
			Name:    "token-encryption-key",
			Usage:   "hex-encoded 32-byte AES-256-GCM key for encrypting refresh tokens at rest",
			Value:   "",
			EnvVars: []string{"TOKEN_ENCRYPTION_KEY"},
		},
	}

	app.Action = Search

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
		os.Exit(1)
	}
}

func Search(cctx *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	logger := telemetry.StartLogger(cctx)
	logger.Info("starting repo cleanup service",
		"version", version.Version,
		"commit", version.GitCommit)

	if cctx.String("magic-header-val") == "" {
		logger.Warn("MAGIC_HEADER_VAL is not set — rate-limit bypass header will not be sent to PDS")
	}

	var encKey []byte
	if keyHex := cctx.String("token-encryption-key"); keyHex != "" {
		var err error
		encKey, err = hex.DecodeString(keyHex)
		if err != nil || len(encKey) != 32 {
			return fmt.Errorf("TOKEN_ENCRYPTION_KEY must be a valid hex-encoded 32-byte (64 hex chars) AES-256 key")
		}
	} else {
		logger.Warn("TOKEN_ENCRYPTION_KEY is not set — refresh tokens will be stored in plaintext in Postgres")
	}

	logger.Info("connecting to postgres", "url", cctx.String("database-url"))
	chStore, err := store.NewStore(cctx.String("database-url"), encKey)
	if err != nil {
		return fmt.Errorf("failed to create postgres store: %w", err)
	}
	defer chStore.Close()
	logger.Info("connected to postgres")

	api, err := endpoints.NewAPI(
		logger,
		chStore,
		cctx.String("magic-header-val"),
	)
	if err != nil {
		return fmt.Errorf("failed to create API: %w", err)
	}

	logger.Info("starting cleanup daemon")
	go api.RunCleanupDaemon(ctx)

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())
	e.Use(slogecho.New(logger))
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodOptions},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentLength, echo.HeaderContentType},
	}))

	// Tight per-IP limit on the credential-accepting endpoint. Each request
	// triggers a real PDS login, so abuse here directly costs user accounts.
	cleanupRateLimiter := middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
		Store: middleware.NewRateLimiterMemoryStoreWithConfig(
			middleware.RateLimiterMemoryStoreConfig{
				Rate:      rate.Limit(2),
				Burst:     5,
				ExpiresIn: 5 * time.Minute,
			},
		),
		IdentifierExtractor: func(c echo.Context) (string, error) {
			return c.RealIP(), nil
		},
		DenyHandler: func(c echo.Context, id string, err error) error {
			return c.JSON(http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded, please try again later"})
		},
		ErrorHandler: func(c echo.Context, err error) error {
			return c.JSON(http.StatusForbidden, map[string]string{"error": "could not identify request"})
		},
	})

	e.GET("/repo/cleanup", api.GetCleanupStatus)
	e.POST("/repo/cleanup", api.CleanupOldRecords, cleanupRateLimiter)
	e.DELETE("/repo/cleanup", api.CancelCleanupJob)
	e.GET("/repo/cleanup/stats", api.GetCleanupStats)

	distFS, err := fs.Sub(dashboard.FS, "dist")
	if err != nil {
		return fmt.Errorf("failed to sub dashboard FS: %w", err)
	}
	e.GET("/*", echo.WrapHandler(http.FileServer(http.FS(distFS))))

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("starting HTTP server", "listen_address", cctx.String("listen-address"))
		if err := e.Start(cctx.String("listen-address")); err != nil && err != http.ErrServerClosed {
			serverErr <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	select {
	case <-signals:
		logger.Info("shutting down on signal")
	case <-ctx.Done():
		logger.Info("shutting down on context done")
	case err := <-serverErr:
		logger.Error("server error", "error", err)
		return err
	}

	logger.Info("beginning graceful shutdown")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := e.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to shut down HTTP server gracefully", "error", err)
		return err
	}

	logger.Info("shut down successfully")
	return nil
}
