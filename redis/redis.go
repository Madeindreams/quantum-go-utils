// redisredis.go
package redis

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Addr         string        // "localhost:6379"
	Username     string        // optional
	Password     string        // optional
	DB           int           // default 0
	TLS          bool          // enable TLS
	DialTimeout  time.Duration // default 5s
	ReadTimeout  time.Duration // default 3s
	WriteTimeout time.Duration // default 3s
}

// NewClient creates and pings a Redis client.
func NewClient(ctx context.Context, cfg Config) (*redis.Client, error) {
	if cfg.Addr == "" {
		cfg.Addr = "localhost:6379"
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 3 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 3 * time.Second
	}

	opts := &redis.Options{
		Addr:         cfg.Addr,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	rdb := redis.NewClient(opts)

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}

	return rdb, nil
}
