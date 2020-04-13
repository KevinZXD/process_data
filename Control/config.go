package Control

import (
	"encoding/json"
	"fmt"

	"github.com/BurntSushi/toml"

	"process_data/config"
	"process_data/lib/logging"
)

type Config struct {
	Title                      string `toml:"title" json:"title"`
	Scene                      string `toml:"scene" json:"scene"`
	logging.LogConfig          `toml:"logging" json:"logging"`
	config.KafkaConsumerConfig `toml:"kafka_consumer" json:"kafka_consumer"`
	WorkerConfig               `toml:"worker" json:"worker"`
	config.RedisCluster        `toml:"redis_cluster" json:"redis_cluster"`

}

func NewConfig(fname string) (*Config, error) {
	var config Config
	if _, err := toml.DecodeFile(fname, &config); err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &config, nil
}

func (c *Config) Validate() error {
	if err := c.LogConfig.Validate(); err != nil {
		return err
	}
	if err := c.KafkaConsumerConfig.Validate(); err != nil {
		return err
	}
	if err := c.WorkerConfig.Validate(); err != nil {
		return err
	}
	if err := c.RedisCluster.Validate(); err != nil {
		return err
	}

	return nil
}

func (c *Config) String() string {

	b, err := json.MarshalIndent(c, "", "\t")
	if err != nil {
		fmt.Printf("json err: %s\n", err)
	}

	return string(b)
}
