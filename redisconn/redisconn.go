// Package redisconn abre a conexão com o Redis usada para coordenar as
// instâncias (tasks do ECS) em execução.
package redisconn

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// Connect reads the address from REDIS_ADDR, creates the client and checks it with a PING.
func Connect(ctx context.Context) (*redis.Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		return nil, errors.New("variável REDIS_ADDR não definida")
	}
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 5 * time.Second,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}
