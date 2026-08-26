package repo

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

type Handler interface {
	Handle([][]byte, int) error
}

type Client struct {
	rds redis.Client
}

const (
	PREFIX_PENDING_MESSAGE = "pending_message:"
	FETCH_SCRIPT_FILENAME  = "static/fetch.lua"
	ACK_SCRIPT_FILENAME    = "static/ack.lua"
)

var fetchScript *redis.Script
var ackScript *redis.Script

func LoadScript(filename string) *redis.Script {
	data, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	return redis.NewScript(string(data))
}

func init() {
	fetchScript = LoadScript(FETCH_SCRIPT_FILENAME)
	ackScript = LoadScript(ACK_SCRIPT_FILENAME)
}

func (c *Client) Save(ctx context.Context, id string, data []byte) error {
	err := c.rds.RPush(ctx, PREFIX_PENDING_MESSAGE+id, data).Err()
	if err != nil {
		return fmt.Errorf("unable to enqueue redis: %w", err)
	}
	return nil
}

// 双队列
func (c *Client) Fetch(ctx context.Context, id string, messNum int, handler Handler) error {
	// c.rds.
}
