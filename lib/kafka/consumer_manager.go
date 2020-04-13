//Kafka消费者
package kafka

import (
	"sync"

	"process_data/config"
	"process_data/lib/logging"
	"process_data/lib/rtm"
)

type KafkaConsumerManager struct {
	Logger logging.Logger
	wg     sync.WaitGroup

	ID             int
	cfg            *config.KafkaConsumerConfig
	outCh          config.KafkaConsumerMsgCh
	notifctnCh     rtm.NotifctnCh
	kafkaConsumers []*KafkaConsumer

	msgType int // 消息类型，默认为0，是否启用该字段由业务方决定

	topic string
}

func NewKafkaConsumerManager(
	kcfg *config.KafkaConsumerConfig,
	lg logging.Logger,
	ch config.KafkaConsumerMsgCh) (*KafkaConsumerManager, error) {
	km := &KafkaConsumerManager{
		cfg:    kcfg,
		Logger: lg,
		outCh:  ch,
	}
	km.topic = km.cfg.Topics[0]

	return km, nil
}

// SetMsgType 设置消息类型，在Init之后调用无效
func (km *KafkaConsumerManager) SetMsgType(msgType int) {
	km.msgType = msgType
}

func (km *KafkaConsumerManager) Init() error {
	km.kafkaConsumers = make([]*KafkaConsumer, km.cfg.Routines)
	for i := 0; i < km.cfg.Routines; i++ {
		consumer, err := NewKafkaConsumer(
			i,
			km.cfg,
			km.Logger,
			km.outCh)
		if err != nil {
			km.Logger.Errorf("Topic(%s) initKafkaConsumer fail: %s", km.topic, err)
			return err
		}
		consumer.SetMsgType(km.msgType)
		km.kafkaConsumers[i] = consumer
	}
	km.Logger.Infof("Topic(%s) init KafkaConsumer success", km.topic)

	return nil
}

func (km *KafkaConsumerManager) Start() error {
	for i := 0; i < km.cfg.Routines; i++ {
		km.wg.Add(1)
		consumer := km.kafkaConsumers[i]
		go consumer.Start(&km.wg)
	}
	return nil
}

func (km *KafkaConsumerManager) Stop() error {
	km.Logger.Infof("Topic(%s) stop KafkaConsumer", km.topic)
	for i := 0; i < km.cfg.Routines; i++ {
		consumer := km.kafkaConsumers[i]
		consumer.Stop()
		km.Logger.Infof("Topic(%s) stop KafkaConsumer:%d", km.topic, i)
	}
	km.Logger.Infof("Topic(%s) stoped KafkaConsumer", km.topic)

	km.wg.Wait()

	close(km.outCh) //非常重要
	km.Logger.Infof("Topic(%s) closed(outCh)", km.topic)

	return nil
}

func (km *KafkaConsumerManager) StopAndDoNotCloseChan() error {
	km.Logger.Infof("Topic(%s) stop KafkaConsumer", km.topic)
	for i := 0; i < km.cfg.Routines; i++ {
		consumer := km.kafkaConsumers[i]
		consumer.Stop()
		km.Logger.Infof("Topic(%s) stop KafkaConsumer:%d", km.topic, i)
	}
	km.Logger.Infof("Topic(%s) stoped KafkaConsumer", km.topic)

	km.wg.Wait()

	return nil
}
