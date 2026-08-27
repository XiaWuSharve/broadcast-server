package client

import (
	"github.com/XiaWuSharve/whisperly/utils"
)

type ConnPool struct {
	// only shared memory structure are allowed to be coroutine safe (to avoid lock acquiring)
	Conns         *utils.ShardMap[int64, Conn]
	UserId2connId *utils.ShardMap[string, int64]
}

// coroutine safe
func (p *ConnPool) AddConn(c Conn) int64 {
	p.Conns.Set(c.GetId(), c)
	return c.GetId()
}
