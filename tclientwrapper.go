package thrift_utils

import (
	"fmt"
	"time"

	"github.com/hieubq90/go-commons-pool"
)

type TClientWrapper struct {
	tag          string
	objPool      *pool.ObjectPool
	maxTryTimes  int
	reTryTimeout int
}

func (cw *TClientWrapper) GetClient() interface{} {
	if cw.objPool != nil {
		for try := 1; try <= cw.maxTryTimes; try++ {
			fmt.Printf("%s | borrow client from pool | [try: %d] [active: %d] [Idle: %d]\n", cw.tag, try, cw.objPool.GetNumActive(), cw.objPool.GetNumIdle())
			client, err := cw.objPool.BorrowObject()
			if err != nil {
				// co the sleep
				fmt.Printf("%s | no more client to borrow | [%d]\n", cw.tag, try)
				time.Sleep(time.Millisecond * time.Duration(cw.reTryTimeout))
			} else {
				return client
			}
		}
	}
	return nil
}

func (cw *TClientWrapper) ReturnClient(client interface{}) (err error) {
	fmt.Printf("%s | return client to pool\n", cw.tag)
	if cw.objPool != nil {
		err = cw.objPool.ReturnObject(client)
	}
	return
}

func (cw *TClientWrapper) DeleteClient(client interface{}) (err error) {
	fmt.Printf("%s | delete client of pool\n", cw.tag)
	if cw.objPool != nil {
		cw.objPool.Destroy(client)
	}
	return
}

func NewDefaultPoolConfig() (cfg *pool.ObjectPoolConfig) {
	cfg = pool.NewDefaultPoolConfig()
	cfg.MaxTotal = 8
	cfg.MaxIdle = 8
	cfg.MinIdle = 0
	cfg.MinEvictableIdleTimeMillis = int64(7200)
	cfg.TimeBetweenEvictionRunsMillis = int64(7200)
	cfg.TestOnBorrow = false
	return cfg
}

func NewPoolConfig(poolSize, maxIdle, minIdle int, evictionTime int64) (cfg *pool.ObjectPoolConfig) {
	cfg = pool.NewDefaultPoolConfig()
	cfg.MaxTotal = poolSize
	cfg.MaxIdle = maxIdle
	cfg.MinIdle = minIdle
	cfg.MinEvictableIdleTimeMillis = evictionTime
	cfg.TimeBetweenEvictionRunsMillis = evictionTime
	cfg.TestOnBorrow = false
	return cfg
}

func NewTClientWrapperWithDefaultConfig(tag string, f *TClientFactory) (cw *TClientWrapper) {
	cw = &TClientWrapper{
		tag:          tag,
		maxTryTimes:  5,
		reTryTimeout: 500,
		objPool:      pool.NewObjectPool(f, NewDefaultPoolConfig()),
	}
	return cw
}

func NewTClientWrapper(tag string, f *TClientFactory, maxTry, reTryTimeout int, cfg *pool.ObjectPoolConfig) (cw *TClientWrapper) {
	cw = &TClientWrapper{
		tag:          tag,
		maxTryTimes:  maxTry,
		reTryTimeout: reTryTimeout,
		objPool:      pool.NewObjectPool(f, cfg),
	}
	return cw
}
