//Kafka消费者
package kafka

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"

	"process_data/config"
	"process_data/lib/logging"
	"process_data/lib/rtm"
)

type KafkaConsumer struct {
	Logger logging.Logger
	wg     *sync.WaitGroup

	ID         int
	cfg        *config.KafkaConsumerConfig
	outCh      config.KafkaConsumerMsgCh
	notifctnCh rtm.NotifctnCh

	consumer *cluster.Consumer

	// ${topic}为topic名字中的. - 替换为下划线
	metricTopicQPS   string // ${topic}.consume.qps
	metricTopicError string // ${topic}.consume.error

	msgType int // 消息类型，默认为0，是否启用该字段由业务方决定

	topic string
}

func NewKafkaConsumer(id int,
	kcfg *config.KafkaConsumerConfig,
	lg logging.Logger,
	ch config.KafkaConsumerMsgCh) (*KafkaConsumer, error) {

	k := &KafkaConsumer{
		cfg:        kcfg,
		Logger:     lg,
		outCh:      ch,
		ID:         id,
		notifctnCh: make(rtm.NotifctnCh),
	}
	k.topic = k.cfg.Topics[0]

	cc, err := k.newClusterConfig()
	if err != nil {
		return nil, err
	}

	brokers := k.cfg.Hosts
	topics := k.cfg.Topics
	groupID := k.cfg.GroupID

	s := k.topic
	s = strings.Replace(s, ".", "_", -1)
	s = strings.Replace(s, "-", "_", -1)
	k.metricTopicQPS = fmt.Sprintf("%s.consume.qps", s)
	k.metricTopicError = fmt.Sprintf("%s.consume.error", s)

	consumer, err := cluster.NewConsumer(brokers, groupID, topics, cc)
	if err != nil {
		k.Logger.Errorf("Topic(%s) Kafka NewConsumer Error: %s", err, k.topic)
		return nil, err
	}
	k.consumer = consumer

	return k, nil
}

// SetMsgType 设置消息类型，在Start之后调用无效
func (k *KafkaConsumer) SetMsgType(msgType int) {
	k.msgType = msgType
}

func (k *KafkaConsumer) newClusterConfig() (*cluster.Config, error) {
	cc := cluster.NewConfig()
	cc.Consumer.Return.Errors = true
	cc.Consumer.Offsets.Initial = k.cfg.InitialOffset
	cc.Group.Return.Notifications = true

	if len(k.cfg.Username) != 0 {
		cc.Net.SASL.Enable = true
		cc.Net.SASL.User = k.cfg.Username
		cc.Net.SASL.Password = k.cfg.Password
	}

	if k.cfg.Timeout != nil {
		timeout := *k.cfg.Timeout
		cc.Net.DialTimeout = timeout
		cc.Net.ReadTimeout = timeout
		cc.Net.WriteTimeout = timeout
	}

	if k.cfg.KeepAlive != nil {
		cc.Net.KeepAlive = *k.cfg.KeepAlive
	}
	if k.cfg.BrokerTimeout != nil {
		cc.Producer.Timeout = *k.cfg.BrokerTimeout
	}

	if k.cfg.Metadata.Retry.Max != nil {
		cc.Metadata.Retry.Max = *k.cfg.Metadata.Retry.Max
	}
	if k.cfg.Metadata.Retry.Backoff != nil {
		cc.Metadata.Retry.Backoff = *k.cfg.Metadata.Retry.Backoff
	}
	if k.cfg.Metadata.RefreshFreq != nil {
		cc.Metadata.RefreshFrequency = *k.cfg.Metadata.RefreshFreq
	}

	// configure producer API properties
	if k.cfg.MaxMessageBytes != nil {
		cc.Producer.MaxMessageBytes = *k.cfg.MaxMessageBytes
	}
	if k.cfg.RequiredACKs != nil {
		cc.Producer.RequiredAcks = sarama.RequiredAcks(*k.cfg.RequiredACKs)
	}

	if k.cfg.MaxRetries != nil {
		retryMax := *k.cfg.MaxRetries
		if retryMax >= 0 {
			cc.Producer.Retry.Max = retryMax
		}
	}

	cc.ClientID = k.cfg.ClientID

	if err := cc.Validate(); err != nil {
		k.Logger.Errorf("Topic(%s) Invalid kafka configuration: %v", k.topic, err)
		return nil, err
	}

	return cc, nil
}

func (k *KafkaConsumer) Start(wg *sync.WaitGroup) {
	k.Logger.Infof("Topic(%s) KafkaConsumer:%d started", k.topic, k.ID)
	k.wg = wg

	isStop := false
	for {
		select {
		case msg, ok := <-k.consumer.Messages():
			if !ok {
				time.Sleep(1 * time.Second)
				k.Logger.Errorf("Topic(%s) KafkaConsumer:%d message channel be closed", k.topic, k.ID)
				if isStop {
					return
				}
				continue
			}
			k.consumer.MarkOffset(msg, "")
			k.Logger.Debugf("%s", msg.Value)
			k.outCh <- &config.KafkaConsumerMsg{Value: msg.Value, Type: k.msgType}

		case err, ok := <-k.consumer.Errors():
			if ok {
				k.Logger.Errorf("Topic(%s) KafkaConsumer:%d consume Erros: %+v", k.topic, k.ID, err)
			}

		case err, ok := <-k.consumer.Notifications():
			if ok {
				k.Logger.Errorf("Topic(%s) KafkaConsumer:%d Rebalanced: %+v", k.topic, k.ID, err)
			}

		case n, ok := <-k.notifctnCh:
			// if n == rtm.NotifctnStop {}
			k.Logger.Errorf("Topic(%s) KafkaConsumer:%d (%d,%v) stoping", k.topic, k.ID, n, ok)
			isStop = true
			k.stop()
			return
		}
	}
}

func (k *KafkaConsumer) stop() error {
	defer func() {
		if k.wg != nil {
			k.wg.Done()
		}
	}()
	// k.Logger.Debugf("KafkaConsumer:%d stoped", k.ID)
	if err := k.consumer.Close(); err != nil {
		k.Logger.Infof("Topic(%s) KafkaConsumer:%d stop failed: %+v", k.topic, k.ID, err)
		// k.Logger.Debugf("KafkaConsumer:%d ", k.ID)
		return err
	}

	k.Logger.Infof("Topic(%s) KafkaConsumer:%d stoped", k.topic, k.ID)

	return nil
}

func (k *KafkaConsumer) Stop() error {
	k.notifctnCh <- rtm.NotifctnStop

	return nil
}
