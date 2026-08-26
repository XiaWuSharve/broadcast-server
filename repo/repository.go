package repo

import (
	"context"
	"fmt"

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
)

func (c *Client) Save(ctx context.Context, id string, data []byte) error {
	err := c.rds.RPush(ctx, PREFIX_PENDING_MESSAGE+id, data).Err()
	if err != nil {
		return fmt.Errorf("unable to enqueue redis: %w", err)
	}
	return nil
}


// 双队列
func (c *Client) Fetch(ctx context.Context, id string, messNum int, handler Handler) (error) {
	c.rds.
}
