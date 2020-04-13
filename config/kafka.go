package config

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"process_data/lib/logging"
	"strings"
	"time"
)

type KafkaConfig struct {
	Hosts            []string                `toml:"brokers" json:"brokers"`
	Timeout          *time.Duration          `toml:"timeout"`
	Metadata         MetaConfig              `toml:"metadata"`
	KeepAlive        *time.Duration          `toml:"keep_alive`
	MaxMessageBytes  *int                    `toml:"max_message_bytes`
	RequiredACKs     *int                    `toml:"required_acks`
	BrokerTimeout    *time.Duration          `toml:"broker_timeout`
	Compression      *string                 `toml:"compression"`
	CompressionMode  sarama.CompressionCodec `toml:"-"`
	CompressionLevel *int                    `toml:"compression_level"`
	MaxRetries       *int                    `toml:"max_retries`
	ClientID         string                  `toml:"client_id"`
	Username         string                  `toml:"username"`
	Password         string                  `toml:"password"`
	Codec            *string                 `toml:"codec"`
}

// MetaConfig Metadata的相关配置
type MetaConfig struct {
	Retry       MetaRetryConfig `toml:"retry"`
	RefreshFreq *time.Duration  `toml:"refresh_frequency`
}

type MetaRetryConfig struct {
	Max     *int           `toml:"max`
	Backoff *time.Duration `toml:"backoff`
}

// compressionModes 生产者的对数据的压缩算法
var compressionModes = map[string]sarama.CompressionCodec{
	"none":   sarama.CompressionNone,
	"no":     sarama.CompressionNone,
	"off":    sarama.CompressionNone,
	"gzip":   sarama.CompressionGZIP,
	"lz4":    sarama.CompressionLZ4,
	"snappy": sarama.CompressionSnappy,
}

// KafkaConsumerConfig 消费者的配置
type KafkaConsumerConfig struct {
	KafkaConfig

	Topics            []string `toml:"topics" json:"topics"`
	Routines          int      `toml:"routines"`
	ChannelBufferSize int      `toml:"channel_buffer_size"`

	GroupID         string `toml:"groupid" json:"groupid"`
	AutoOffsetReset string `toml:"auto_offset_reset" json:"auto_offset_reset"` //earliest or latest
	InitialOffset   int64
}

// KafkaConsumerMsg 消费者的配置
type KafkaConsumerMsg struct {
	Value []byte
	Type  int //value的类型，自定义
}

type KafkaConsumerMsgCh chan *KafkaConsumerMsg

func (ch KafkaConsumerMsgCh) Length() int {
	return len(ch)
}
func (ch KafkaConsumerMsgCh) Capacity() int {
	return cap(ch)
}

type KafkaProducerMsg struct {
	Value []byte
	Type  int //value的类型，自定义
}

type KafkaProducerMsgCh chan *KafkaProducerMsg

func (ch KafkaProducerMsgCh) Length() int {
	return len(ch)
}
func (ch KafkaProducerMsgCh) Capacity() int {
	return cap(ch)
}

// KafkaProducerConfig 生产者的配置
type KafkaProducerConfig struct {
	KafkaConfig

	Topics            []string         `toml:"topics" json:"topics"`
	Routines          int              `toml:"routines", json:"routines"`
	ChannelBufferSize int              `toml:"channel_buffer_size", json:"channel_buffer_size"`
	File              *logging.LogFile `toml:"local_file", json:"local_file"`
}

// ChanBufferSize   int           `toml:"channel_buffer_size`

// Validate 验证KafkaConfig的正确性
func (c *KafkaConfig) Validate() error {
	if len(c.Hosts) == 0 {
		return errors.New("no hosts configured")
	}
	if len(c.ClientID) == 0 {
		c.ClientID = "process_data"
	}
	if c.Username != "" && c.Password == "" {
		return fmt.Errorf("password must be set when username is configured")
	}
	if c.Compression != nil {
		compressionMode, ok := compressionModes[strings.ToLower(*c.Compression)]
		if !ok {
			return fmt.Errorf("compression mode '%v' unknown", c.Compression)
		}
		c.CompressionMode = compressionMode
		if *c.Compression == "gzip" {
			if c.CompressionLevel != nil {
				lvl := *c.CompressionLevel
				if lvl != sarama.CompressionLevelDefault && !(0 <= lvl && lvl <= 9) {
					return fmt.Errorf("compression_level must be between 0 and 9")
				}
			}
		}
	}

	return nil
}

func (c *KafkaConsumerConfig) Validate() error {
	if err := c.KafkaConfig.Validate(); err != nil {
		return err
	}

	if len(c.Topics) == 0 {
		return errors.New("kafka.topics is invalid")
	}
	if c.Routines == 0 {
		c.Routines = 1
	}

	if c.GroupID == "" {
		return errors.New("kafka.groupid is invalid")
	}
	if c.AutoOffsetReset == "earliest" {
		c.InitialOffset = sarama.OffsetOldest
	} else if c.AutoOffsetReset == "latest" {
		c.InitialOffset = sarama.OffsetNewest
	} else {
		return errors.New("kafka.auto_offset_reset is invalid")
	}

	if c.ChannelBufferSize == 0 {
		c.ChannelBufferSize = 10000
	}

	return nil
}
func (c *KafkaProducerConfig) Validate() error {
	if err := c.KafkaConfig.Validate(); err != nil {
		return err
	}

	if len(c.Topics) == 0 {
		return errors.New("kafka.topics is invalid")
	}
	if c.Routines == 0 {
		c.Routines = 1
	}
	if c.ChannelBufferSize == 0 {
		c.ChannelBufferSize = 10000
	}

	if len(c.File.FileName) == 0 {
		return fmt.Errorf("kafka_prodocer.local_file.filename is invalid")
	}
	if err := c.File.Validate(); err != nil {
		panic(err.Error())
	}

	return nil
}
