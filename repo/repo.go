package repo

import (
	"github.com/redis/go-redis/v9"
)

type Client struct {
	rds *redis.Client
}

func (c *Client) Save()
