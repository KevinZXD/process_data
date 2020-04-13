package Control

import (
	_ "encoding/json"
	_ "errors"
	"fmt"
	"process_data/config"
	"process_data/lib/logging"
	"process_data/lib/rtm"
	"sync"

	//"github.com/json-iterator/go"
	//"time"
	//"math/rand"
	//"strconv"
	_ "sort"
)
//var rjson jsoniter.API = jsoniter.ConfigCompatibleWithStandardLibrary
type WorkerConfig struct {
	Routines    int    `toml:"routines",json:"routines"`
	LogFileNum  int    `toml:"log_file_num",json:"log_file_num"`
	LogFilePath string `toml:"log_file_path",json:"log_file_path"`
}





var hLock sync.Mutex

func (c *WorkerConfig) Validate() error {
	if c.Routines == 0 {
		c.Routines = 12
	}
	if c.LogFileNum == 0 {
		c.LogFileNum = 64
	}
	if c.LogFilePath == "" {
		c.LogFilePath = "/tmp/"
	}
	return nil
}

type Worker struct {
	Scene      string
	Logger     logging.Logger
	ID         int
	cfg        *Config
	wg         *sync.WaitGroup
	WorkerCnf  WorkerConfig
	inMsgCh    config.KafkaConsumerMsgCh
	notifctnCh rtm.NotifctnCh
	rediswr    RedisStorager
}

func NewWorker(
	scene string,
	id int,
	cfg *Config,
	lg logging.Logger,
	rdstg RedisStorager,
	inCh config.KafkaConsumerMsgCh,
) (*Worker, error) {
	w := &Worker{
		Scene:      scene,
		Logger:     lg,
		inMsgCh:    inCh,
		rediswr:    rdstg,
		ID:         id,
		notifctnCh: make(rtm.NotifctnCh),
	}
	return w, nil
}

func (w *Worker) Start(wg *sync.WaitGroup) error {
	w.Logger.Infof("Worker:%d started", w.ID)
	w.wg = wg

	for {
		select {
		case msg, ok := <-w.inMsgCh:
			if !ok {
				w.Logger.Errorf("Worker:%d message channel closed", w.ID)
				w.stop()
				return nil
			}
			if err := w.process(msg); err != nil {
				continue
			}
		case n, ok := <-w.notifctnCh:
			w.Logger.Errorf("Worker:%d (%d,%v) stoping", w.ID, n, ok)
			return nil
		}
	}

	return nil
}

func (w *Worker) process(msg *config.KafkaConsumerMsg) error {
      if w.Scene == "process_data" {
      	//msg.Value 是字节数组，注意类型转换  处理业务逻辑

            fmt.Printf("consume:"+string(msg.Value)+"\n")
           }

		return nil
	}



func (w *Worker) stop() error {
	defer func() {
		if w.wg != nil {
			w.wg.Done()
		}
	}()
	w.Logger.Infof("stop worker %d ,wg Done", w.ID)
	return nil
}

func (w *Worker) Stop() error {
	return nil
}

