package wredis

import (
	redis "github.com/gomodule/redigo/redis"
	"time"
)

//create new connect pool
func NewPool(server string, db int, maxIdle int, maxActive int, idleTimeout time.Duration, connectTimeout time.Duration, readTimeout time.Duration, writeTimeout time.Duration) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: idleTimeout,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", server, redis.DialConnectTimeout(connectTimeout), redis.DialReadTimeout(readTimeout), redis.DialWriteTimeout(writeTimeout))
			if err != nil {
				return nil, err
			}
			//if _, err = conn.Do("SELECT", db); err != nil {
			//	conn.Close()
			//	return nil, err
			//}
			return conn, nil
		},
		TestOnBorrow: func(conn redis.Conn, tm time.Time) error {
			// tm是上次connection归还的时间
			// Since(tm)代表当前时刻距tm的时间
			if time.Since(tm) < time.Minute {
				return nil
			}
			// 超过1min测试连接
			_, err := conn.Do("PING")
			return err
		},
	}
}
