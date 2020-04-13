package config

import (
	"fmt"
	"time"

	ltime "process_data/lib/time"
)

type HashCallBack func(key string) uint64

type RedisServerConfig struct {
	Disable bool   `toml:"disable" json:"disable"`
	Address string `toml:"address" json:"address"`
}

func (c *RedisServerConfig) Validate() error {
	if len(c.Address) == 0 {
		return fmt.Errorf("redis_server.address is invalid")
	}

	return nil
}

type RedisNode struct {
	Address string `toml:"address" json:"address"`
}

type RedisCluster struct {
	Name               string         `toml:"name" json:"name"`
	Database           int            `toml:"database" json:"database"` // default 0
	Hasher             string         `toml:"hasher" json:"hasher"`
	MaxIdle            int            `toml:"max_idle" json:"max_idle"`
	MaxActive          int            `toml:"max_active" json:"max_active"`
	DialConnectTimeout ltime.Duration `toml:"dial_connect_timeout" json:"dial_connect_timeout"` //单位: ms
	DialReadTimeout    ltime.Duration `toml:"dial_read_timeout" json:"dial_read_timeout"`       //单位: ms
	DialWriteTimeout   ltime.Duration `toml:"dial_write_timeout" json:"dial_write_timeout"`     //单位: ms
	IdleTimeout        ltime.Duration `toml:"idle_timeout" json:"idle_timeout"`                 //单位: min
	Nodes              []RedisNode    `toml:"redis_node" json:"redis_node"`
	MaxRetry           int            `toml:"max_retry" json:"max_retry"` //命令执行重试次数 default 0
	Servers            []string
}

func (c *RedisCluster) Validate() error {
	if len(c.Name) == 0 {
		return fmt.Errorf("\"name\" is invalid")
	}

	if c.MaxIdle == 0 {
		c.MaxIdle = 5
	}

	if c.DialConnectTimeout.Duration == 0 {
		c.DialConnectTimeout.Duration = 5 * time.Millisecond
	}
	if c.DialReadTimeout.Duration == 0 {
		c.DialReadTimeout.Duration = 5 * time.Millisecond
	}
	if c.DialWriteTimeout.Duration == 0 {
		c.DialWriteTimeout.Duration = 5 * time.Millisecond
	}
	if c.IdleTimeout.Duration == 0 {
		c.IdleTimeout.Duration = 5 * time.Minute
	}

	if len(c.Nodes) == 0 {
		return fmt.Errorf("\"redis_node\" is invalid")
	}
	servers := []string{}
	for i, node := range c.Nodes {
		if len(node.Address) == 0 {
			return fmt.Errorf("\"redis_node\"[%d] is invalid", i)
		}
		servers = append(servers, node.Address)
	}
	c.Servers = servers

	return nil
}
