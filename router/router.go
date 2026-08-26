package router

import (
	"github.com/XiaWuSharve/whisperly/client"
	"github.com/XiaWuSharve/whisperly/config"
	"github.com/XiaWuSharve/whisperly/datas"
	"github.com/XiaWuSharve/whisperly/utils"
)

type Router struct {
	// only shared memory structure are allowed to be coroutine safe (to avoid lock acquiring)
	table *utils.ShardMap[int64, client.Conn]
}

// coroutine safe
func (r *Router) AddConn(c client.Conn) int64 {
	id := datas.GenId()
	r.table.Set(id, c)
	return id
}

// coroutine safe
func (r *Router) Find(id int64) (client.Conn, bool) {
	return r.table.Get(id)
}

var ErrClient

func (r *Router) Forward(frame *datas.SendFrame) error {
	,  := r.Find(frame.ReceiverId)
}

func New() *Router {
	return &Router{utils.NewShardMap[int64, client.Conn](config.Cfg.RegistryMaxBucketNum, utils.HashFunc)}
}
