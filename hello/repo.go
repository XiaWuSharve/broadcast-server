package hello

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/aliyun/aliyun-tablestore-go-sdk/timeline"
)

type SyncStore struct {
	syncStore timeline.MessageStore
	adapter   timeline.MessageAdapter
}

type MyAdapter struct {
}

// Marshal implements [timeline.MessageAdapter].
func (m *MyAdapter) Marshal(msg timeline.Message) (*timeline.ColumnMap, error) {
	store, ok := msg.(*datas.Store)
	if !ok {
		return nil, timeline.ErrUnexpected
	}
	col := timeline.NewColumnMap()
	col.AddStringColumn("msgId", store.MsgId)
	col.AddAnyColumn("payload", store.Payload)
	return col, nil
}

// Unmarshal implements [timeline.MessageAdapter].
func (m *MyAdapter) Unmarshal(cols *timeline.ColumnMap) (timeline.Message, error) {
	panic("unimplemented")
}

var _ timeline.MessageAdapter = (*MyAdapter)(nil)

func NewSyncStore() (*SyncStore, error) {
	option := timeline.StoreOption{
		Endpoint:  config.Cfg.Endpoint,
		Instance:  config.Cfg.Instance,
		TableName: "sync",
		AkId:      config.Cfg.AkId,
		AkSecret:  config.Cfg.AkSecret,
		TTL:       -1, // Data time to alive, eg: almost one year
	}
	syncStore, err := timeline.NewDefaultStore(option)
	if err != nil {
		return nil, fmt.Errorf("cannot create sync store: %w", err)
	}
	if err := syncStore.Sync(); err != nil {
		return nil, fmt.Errorf("cannot sync sync store: %w", err)
	}
	return &SyncStore{syncStore: syncStore, adapter: &MyAdapter{}}, nil
}

func (st *SyncStore) Push(ctx context.Context, store *datas.Store) error {
	tm, err := timeline.NewTmLine(store.ReceiverId, st.adapter, st.syncStore)
	if err != nil {
		return fmt.Errorf("cannot create timeline: %w", err)
	}
	if _, err := tm.Store(store); err != nil {
		return fmt.Errorf("cannot store message: %w", err)
	}
	return nil
}

func (st *SyncStore) Pull(ctx context.Context, receiverId string, msgCount int) ([]*datas.Store, error) {
	tm, err := timeline.NewTmLine(receiverId, st.adapter, st.syncStore)
	if err != nil {
		return nil, fmt.Errorf("cannot create timeline: %w", err)
	}
	it := tm.Scan(&timeline.ScanParameter{
		From:        0,
		To:          math.MaxInt64,
		MaxCount:    msgCount,
		BufChanSize: 10,
	})
	defer it.Close()
	stores := make([]*datas.Store, msgCount)
	trueCount := 0
	for i, _ := range stores {
		entry, err := it.Next()
		if err != nil {
			if errors.Is(err, timeline.ErrorDone) {
				break
			}
			return nil, fmt.Errorf("cannot scan timeline: %w", err)
		}
		store, ok := entry.Message.(*datas.Store)
		if !ok {
			return nil, fmt.Errorf("unexpected message type: %T", entry.Message)
		}
		stores[i] = store
		trueCount++
	}
	return stores[:trueCount], nil
}
