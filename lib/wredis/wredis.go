package wredis

import (
	"errors"
	"fmt"
	"strings"
	"time"

	redis "github.com/gomodule/redigo/redis"

	"process_data/config"
)

const (
	DEFAULT_DB              = int(0)                //默认为db0
	DEFAULT_MAX_IDLE        = 1                     //默认最大空闲连接数
	DEFAULT_MAX_ACTIVE      = 10                    //默认最大活跃连接数
	DEFAULT_MAX_RETRY       = 0                     //默认命令执行失败重试次数
	DEFAULT_IDLE_TIMEOUT    = 300 * time.Second     //默认最大空闲时间为300秒
	DEFAULT_CONNECT_TIMEOUT = 30 * time.Millisecond //默认连接超时时间为30毫秒
	DEFAULT_READ_TIMEOUT    = 30 * time.Millisecond //默认读操作超时时间为30毫秒
	DEFAULT_WRITE_TIMEOUT   = 30 * time.Millisecond //默认写操作超时时间为30毫秒
)

type WRedis struct {
	Name     string
	Servers  []string
	Hasher   HashCallBack
	Pools    []*redis.Pool
	MaxRetry int
}

//WRedis.DoByHash()
//DoByHash wrap redis.DO and execute command in redis server that hashed by user defined hasher
func (c *WRedis) DoByHash(cmdName string, key string, args ...interface{}) (reply interface{}, err error) {
	if c.Hasher == nil {
		//找不到哈希函数, 报错
		return "", fmt.Errorf("cannot found hash callback function. wredis.name: %s", c.Name)
	}
	//哈希获得操作的实例下标
	index := c.Hasher(key) % uint64(len(c.Servers))
	//寻找连接池
	pool := c.Pools[index]
	if pool == nil {
		return "", fmt.Errorf("cannot found connection pool for server: %s", c.Servers[index])
	}
	//获取连接
	conn := pool.Get()
	defer conn.Close()
	//重试机制
	r, err := conn.Do(cmdName, append([]interface{}{key}, args...)...)
	if err == nil || err == redis.ErrNil {
		return r, err
	}
	for retry := 0; retry < c.MaxRetry; retry++ {
		r, err = conn.Do(cmdName, append([]interface{}{key}, args...)...)
		if err == nil || err == redis.ErrNil {
			return r, err
		}
	}
	return r, err
}

//WRedis.Do()
//DO wrap redis.Do execute redis command in designated redis server and return the result
func (c *WRedis) Do(index uint64, cmdName string, args ...interface{}) (reply interface{}, err error) {
	if index >= uint64(len(c.Servers)) {
		return "", fmt.Errorf("invalid index of redis, must less than: %d", len(c.Servers))
	}
	//寻找连接池
	pool := c.Pools[index]
	if pool == nil {
		return "", fmt.Errorf("cannot found connection pool for server: %s", c.Servers[index])
	}
	//获取连接
	conn := pool.Get()
	defer conn.Close()
	//重试机制
	r, err := conn.Do(cmdName, args...)
	if err == nil || err == redis.ErrNil {
		return r, err
	}
	for retry := 0; retry < c.MaxRetry; retry++ {
		r, err = conn.Do(cmdName, args...)
		if err == nil || err == redis.ErrNil {
			return r, err
		}
	}
	return r, err
}

//Close release underlaying resource of WRedis
func (c *WRedis) Close() error {
	//仅释放集群内各实例已创建的连接池
	if c.Pools == nil || len(c.Pools) <= 0 {
		return nil
	}
	closeErr := []string{}
	for i, p := range c.Pools {
		err := p.Close()
		if err != nil {
			closeErr = append(closeErr,
				fmt.Sprintf("fail to close connect pool for server: %d, cluster: %s, err: %s", i, c.Name, err))
		}
	}
	if len(closeErr) > 0 {
		return fmt.Errorf("fail to close Wredis, err: %s", strings.Join(closeErr, "|"))
	}
	return nil
}

/*
WRedis.Send
func (c *WRedis) Send(cmdName string, key string, args ...interface{}) error {
	fmt.Printf("call WConn.Send, cmd: %s, key: %s, args: %v \n", cmdName, key, args)
	return nil
}
*/

//WRedis.Flush

//WRedis.Receive

//WRedis.New
//New create new instance of struct WRedis, return error when any error occurs
func New(name string, servers []string, db int, hasher HashCallBack, maxIdle int, maxActive int, idleTimeout time.Duration, connectTimeout time.Duration, readTimeout time.Duration, writeTimeout time.Duration, maxRetry int) (*WRedis, error) {
	if len(name) <= 0 {
		return nil, errors.New("invalid WRedis name, name cannot be empty.")
	}
	if servers == nil || len(servers) <= 0 {
		return nil, errors.New("invalid WRedis servers, servers cannot be empty.")
	}
	// 初始化所有实例连接池
	poolSlice := []*redis.Pool{}
	for _, s := range servers {
		pool := NewPool(s, db, maxIdle, maxActive, idleTimeout, connectTimeout, readTimeout, writeTimeout)
		if pool == nil {
			return nil, fmt.Errorf("cannot create connect pool for server: %s", s)
		}
		poolSlice = append(poolSlice, pool)
	}

	return &WRedis{
		Name:     name,
		Servers:  servers,
		Hasher:   hasher,
		Pools:    poolSlice,
		MaxRetry: maxRetry,
	}, nil
}

func NewWithConfig(cfg *config.RedisCluster) (*WRedis, error) {
	// 初始化所有实例连接池
	poolSlice := []*redis.Pool{}
	for _, s := range cfg.Servers {
		pool := NewPool(s,
			cfg.Database,
			cfg.MaxIdle,
			cfg.MaxActive,
			cfg.IdleTimeout.Duration,
			cfg.DialConnectTimeout.Duration,
			cfg.DialReadTimeout.Duration,
			cfg.DialWriteTimeout.Duration)
		if pool == nil {
			return nil, fmt.Errorf("cannot create connect pool for server: %s", s)
		}
		poolSlice = append(poolSlice, pool)
	}
	hasher := HashHandler[cfg.Hasher]

	return &WRedis{
		Name:     cfg.Name,
		Servers:  cfg.Servers,
		Hasher:   hasher,
		Pools:    poolSlice,
		MaxRetry: cfg.MaxRetry,
	}, nil
}

//NewDefault create intance of WRedis, using default config
func NewDefault(name string, servers []string, hasher HashCallBack) (*WRedis, error) {
	return New(name, servers, DEFAULT_DB, hasher, DEFAULT_MAX_IDLE, DEFAULT_MAX_ACTIVE, DEFAULT_IDLE_TIMEOUT, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT, DEFAULT_WRITE_TIMEOUT, DEFAULT_MAX_RETRY)
}
