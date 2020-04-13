package wredis

//测试

import (
	"fmt"
	lt "git.intra.weibo.com/ad/adx/alaya/lib/time"
	"github.com/alicebob/miniredis"
	redis "github.com/gomodule/redigo/redis"
	"process_data/config"
	"testing"
	"time"
)

//生成配置
func generateRedisClusterConfig(nodeAddrs []string) (config.RedisCluster, error) {
	if len(nodeAddrs) <= 0 {
		return config.RedisCluster{}, fmt.Errorf("wredis cluster must contains atleast 1 node.")
	}
	Nodes := []config.RedisNode{}
	for _, addr := range nodeAddrs {
		Nodes = append(Nodes, config.RedisNode{Address: addr})
	}
	return config.RedisCluster{
		Name:               "aid_test_cluter",
		Database:           0,
		Hasher:             "FNV32",
		MaxIdle:            10,
		MaxActive:          0,
		DialConnectTimeout: lt.Duration{Duration: 1 * time.Second},
		DialReadTimeout:    lt.Duration{Duration: 1 * time.Second},
		DialWriteTimeout:   lt.Duration{Duration: 1 * time.Second},
		IdleTimeout:        lt.Duration{Duration: 5 * time.Minute},
		Nodes:              Nodes,
		MaxRetry:           3,
	}, nil
}

type MockRedisCluster struct {
	Nodes []*miniredis.Miniredis
	Addrs []string
	Num   int
}

func NewMockCluster(nodeNum int) (*MockRedisCluster, error) {
	if nodeNum <= 0 {
		return nil, fmt.Errorf("mock redis cluster must have atleast 1 node.")
	}
	mockRedis := []*miniredis.Miniredis{}
	mockAddr := []string{}
	for i := 0; i < nodeNum; i++ {
		s, err := miniredis.Run()
		if err != nil {
			return nil, fmt.Errorf("fail to init miniredis node, %d", i)
		}
		mockRedis = append(mockRedis, s)
		mockAddr = append(mockAddr, s.Addr())
	}
	return &MockRedisCluster{
		Nodes: mockRedis,
		Addrs: mockAddr,
		Num:   nodeNum,
	}, nil
}

func (mrc *MockRedisCluster) Close() error {
	if len(mrc.Nodes) <= 0 {
		return nil
	}
	for _, s := range mrc.Nodes {
		s.Close()
	}
	return nil
}

//DoByHash功能
func TestDoByHash(t *testing.T) {
	nodeNum := 8
	//初始化redis模拟集群
	mockCluster, err := NewMockCluster(nodeNum)
	if err != nil {
		t.Errorf("fail to init mock redis cluster. %s", err)
		return
	}
	defer mockCluster.Close()

	//初始化wredis
	wredisConfig, err := generateRedisClusterConfig(mockCluster.Addrs)
	if err != nil {
		t.Errorf("fail to geneate wredis config, %s", err)
		return
	}
	wredisConfig.Validate()
	wr, err := NewWithConfig(&wredisConfig)
	if err != nil {
		t.Errorf("fail to init wredis, %s", err)
		return
	}
	defer wr.Close()

	//SET
	r, err := wr.DoByHash("SET", "foo", "bar")
	if err != nil {
		t.Errorf("command SET test ==> fail, %s", err)
	} else {
		t.Logf("command SET test ==> pass!")
	}

	//GET
	r, err = redis.String(wr.DoByHash("GET", "foo"))
	if err != nil {
		t.Errorf("command GET test ==> fail, err: %s", err)
	} else {
		if r != "bar" {
			t.Errorf("command GET test ==> fail, err: %s", err)
		} else {
			t.Logf("command GET test ==> pass, key: foo, value: %s", r)
		}
	}

	//SETEX
	r, err = wr.DoByHash("SETEX", "foo", 60, "bar")
	if err != nil {
		t.Errorf("command SETEX test ==> fail, %s", err)
	} else {
		t.Logf("command SETEX test ==> pass!")
	}
}

//DoByHash并发
func TestDoByHashConcurrent(t *testing.T) {
	nodeNum := 8
	//初始化redis模拟集群
	mockCluster, err := NewMockCluster(nodeNum)
	if err != nil {
		t.Errorf("fail to init mock redis cluster. %s", err)
		return
	}
	defer mockCluster.Close()

	//初始化wredis
	wredisConfig, err := generateRedisClusterConfig(mockCluster.Addrs)
	if err != nil {
		t.Errorf("fail to geneate wredis config, %s", err)
		return
	}
	wredisConfig.Validate()
	wr, err := NewWithConfig(&wredisConfig)
	if err != nil {
		t.Errorf("fail to init wredis, %s", err)
		return
	}
	defer wr.Close()

	routines := 100
	syncChan := make(chan struct{}, 100)
	//SET
	for i := 0; i < routines; i++ {
		go func(id int, c chan struct{}) {
			_, err := wr.DoByHash("HINCRBY", "foo", "f1", 20)
			if err != nil {
				t.Errorf("routine %d: command HINCRBY test ==> fail, %s", id, err)
			}
			c <- struct{}{}
		}(i, syncChan)
	}
	//WAIT
	for i := 0; i < routines; i++ {
		<-syncChan
	}
	//GET
	r, err := redis.Int64(wr.DoByHash("HGET", "foo", "f1"))
	if err != nil {
		t.Errorf("command HGET test ==> fail, err: %s", err)
	}
	t.Logf("command HGET test ==> pass, key: foo, value: %d", r)
}

//SET性能
func BenchmarkSetByHash(b *testing.B) {
	nodeNum := 8
	//初始化redis模拟集群
	mockCluster, err := NewMockCluster(nodeNum)
	if err != nil {
		return
	}
	defer mockCluster.Close()

	//初始化wredis
	wredisConfig, err := generateRedisClusterConfig(mockCluster.Addrs)
	if err != nil {
		return
	}
	wredisConfig.Validate()
	wr, err := NewWithConfig(&wredisConfig)
	if err != nil {
		return
	}
	defer wr.Close()
	//重置计时器
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wr.DoByHash("SET", "foo", "bar")
	}
}

//GET性能
func BenchmarkGetByHash(b *testing.B) {
	nodeNum := 8
	//初始化redis模拟集群
	mockCluster, err := NewMockCluster(nodeNum)
	if err != nil {
		return
	}
	defer mockCluster.Close()

	//初始化wredis
	wredisConfig, err := generateRedisClusterConfig(mockCluster.Addrs)
	if err != nil {
		return
	}
	wredisConfig.Validate()
	wr, err := NewWithConfig(&wredisConfig)
	if err != nil {
		return
	}
	defer wr.Close()
	//重置计时器
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wr.DoByHash("GET", "foo")
	}
}
