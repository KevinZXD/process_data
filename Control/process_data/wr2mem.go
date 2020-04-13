package process_data

import (
	"process_data/config"
	"process_data/lib/logging"
	"process_data/lib/wredis"
    redis "github.com/gomodule/redigo/redis"
)

type MemStorager struct {
	Logger   logging.Logger
	rediscfg *config.RedisCluster
	wr       *wredis.WRedis
}

func NewRedisStorager(lg logging.Logger, config *config.RedisCluster) (*MemStorager, error) {
	vrs := &MemStorager{Logger: lg, rediscfg: config}
	wr, err := wredis.NewWithConfig(vrs.rediscfg)
	if err != nil {
		return nil, err
	}
	vrs.wr = wr
	return vrs, nil
}




func (vrs *MemStorager) CloseRedis() error {

	err := vrs.wr.Close()
	return err

}

func (vrs *MemStorager) SetRedis(key string,value string) error {

	_, err := vrs.wr.DoByHash("SET", key,value)
	if err != nil {
		vrs.Logger.Infof("set %s failed: %s", key, err)
		return err
	}

	_, err = vrs.wr.DoByHash("EXPIRE", key, 86400*5)

	if err != nil {
		vrs.Logger.Infof("expire %s failed")
		return err
	}

	return nil
}

func (vrs *MemStorager) GetRedis(key string) (reply string, err error) {

	rt, err := redis.String(vrs.wr.DoByHash("GET", key))
	if err != nil {
		vrs.Logger.Infof("get %s failed: %s", key, err)
		return rt,err
	}

	return rt,nil
}



