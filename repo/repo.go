package repo

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/mq"
	"github.com/redis/go-redis/v9"
)

type Handler interface {
	Handle([]*datas.Cache) int
}

type Client struct {
	rds      redis.Client
	consumer mq.Consumer[datas.Cache]
	decoder  datas.Decoder[*datas.Cache]
}

const (
	PREFIX_PENDING_MESSAGE = "pending_message:"
	PREFIX_MIN_OFFSET      = "min_offset:"
	PREFIX_MAX_OFFSET      = "max_offset:"
	FETCH_SCRIPT_FILENAME  = "static/fetch.lua"
	ACK_SCRIPT_FILENAME    = "static/ack.lua"
	PUSH_SCRIPT_FILENAME   = "static/push.lua"
)

var fetchScript *redis.Script
var ackScript *redis.Script
var pushScript *redis.Script

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
	pushScript = LoadScript(PUSH_SCRIPT_FILENAME)
}

func (c *Client) Save(ctx context.Context, id string, data []byte) error {
	_, err := pushScript.Run(ctx, c.rds, []string{id}, data).Int()
	if err != nil {
		return fmt.Errorf("unable to enqueue redis: %w", err)
	}
	return nil
}

// 双队列
func (c *Client) Fetch(ctx context.Context, id string, requestOffset int, handler Handler) error {
	key := []string{id}
	messageStrings, err := fetchScript.Run(ctx, c.rds, key, requestOffset).StringSlice()
	if err != redis.Nil {
		return fmt.Errorf("failed to execute fetch script: %w", err)
	}
	messages := make([]*datas.Cache, len(messageStrings))
	for i, v := range messageStrings {
		messages[i], err = c.decoder.Parse([]byte(v))
		if err != nil {
			return fmt.Errorf("failed to parse message: %w", err)
		}
	}
	ackedOffset := handler.Handle(messages)
	_, err = ackScript.Run(ctx, c.rds, key, ackedOffset).Int64()
	if err != redis.Nil {
		return fmt.Errorf("failed to execute ack script: %w", err)
	}
	return nil
}

func (c *Client) GetRange(ctx context.Context, id string) (int64, int64, error) {
	data, err := c.rds.MGet(ctx, PREFIX_MIN_OFFSET+id, PREFIX_MAX_OFFSET+id).Result()
	if err != nil {
		return 0, 0, err
	}
	if data[0] == nil {
		data[0] = "0"
	}
	if data[1] == nil {
		data[1] = "0"
	}
	minOffset, _ := strconv.ParseInt(data[0].(string), 10, 64)
	maxOffset, _ := strconv.ParseInt(data[1].(string), 10, 64)
	return minOffset, maxOffset, nil
}
