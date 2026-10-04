// Package redisconn abre a conexão com o Redis usada para coordenar as
// instâncias (tasks do ECS) em execução.
package redisconn

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const addr = "localhost:6379"

// Connect cria o cliente e confirma a conexão com um PING.
func Connect(ctx context.Context) (*redis.Client, error) {
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
