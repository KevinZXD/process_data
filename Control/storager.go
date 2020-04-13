package Control

import (
	"errors"
)

var (
	ErrorNotFound  = errors.New("Not Found")
	ErrorSetFailed = errors.New("set key failed Error")
)

type RedisStorager interface {
    GetRedis(string) (string, error)
    SetRedis(string,string) error
	CloseRedis() error
}


