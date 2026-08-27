package repo

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/redis/go-redis/v9"
)

type Fetcher struct {
	rds     *redis.Client
	decoder datas.Decoder[*datas.Cache]
	state   atomic.Pointer[State]
}

type State struct {
	requestOffset int64
	cancel        context.CancelFunc
}

var ErrWeakRequest = errors.New("requestOffset is weak than previous request, will ignore")

func NewFetcher(rds *redis.Client) *Fetcher {
	f := &Fetcher{
		rds: rds,
	}
	f.state.Store(&State{requestOffset: -1})
	return f
}

func (f *Fetcher) MergeRequest(id string, requestOffset int64, handler Handler) error {
	for {
		old := f.state.Load()
		if requestOffset <= old.requestOffset {
			return ErrWeakRequest
		}
		ctx, cancel := context.WithCancel(context.Background())
		new := &State{
			requestOffset: requestOffset,
			cancel:        cancel,
		}
		if f.state.CompareAndSwap(old, new) {
			if old.cancel != nil {
				old.cancel()
			}
			return f.Fetch(ctx, id, requestOffset, handler)
		}
		cancel()
	}
}

func (f *Fetcher) Fetch(ctx context.Context, id string, requestOffset int64, handler Handler) error {
	key := []string{id}
	messageStrings, err := fetchScript.Run(ctx, f.rds, key, requestOffset).StringSlice()
	if err != redis.Nil {
		return fmt.Errorf("failed to execute fetch script: %w", err)
	}
	messages := make([]*datas.Cache, len(messageStrings))
	for i, v := range messageStrings {
		messages[i], err = f.decoder.Parse([]byte(v))
		if err != nil {
			return fmt.Errorf("failed to parse message: %w", err)
		}
	}
	ackedOffset := handler.Handle(messages)
	_, err = ackScript.Run(ctx, f.rds, key, ackedOffset).Int64()
	if err != redis.Nil {
		return fmt.Errorf("failed to execute ack script: %w", err)
	}
	return nil
}
