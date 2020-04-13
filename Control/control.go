package Control

import (
	"fmt"
	"os"
	"os/signal"
	"process_data/config"
	"process_data/Control/process_data"
	"process_data/lib/kafka"
	"process_data/lib/logging"
	"sync"
	"syscall"
)

type FreqControl struct {
	cfgFname             string
	Scene                string
	cfg                  *Config
	Logger               logging.Logger
	wg                   sync.WaitGroup
	kafkaConsumerManager *kafka.KafkaConsumerManager
	workers              []*Worker
	consumeMsgCh         config.KafkaConsumerMsgCh
	rediswr              RedisStorager
}

func New(fname string) *FreqControl {
	return &FreqControl{
		cfgFname: fname,
		Logger:   logging.DefaultLogger(),
	}
}

func (frq *FreqControl) loadServerConfig() error {
	cfg, err := NewConfig(frq.cfgFname)
	if err != nil {
		frq.Logger.Error(err)
		return err
	}
	frq.cfg = cfg
	frq.Scene = cfg.Scene
	if err := frq.cfg.Validate(); err != nil {
		frq.Logger.Errorf("load configuration failed: %s", err)
		return err
	}
	frq.Logger = logging.NewLoggerWithConfig(&frq.cfg.LogConfig)
	frq.consumeMsgCh = make(config.KafkaConsumerMsgCh, frq.cfg.KafkaConsumerConfig.ChannelBufferSize)

	return nil
}

func (frq *FreqControl) Server(isTest bool) error {
	if err := frq.loadServerConfig(); err != nil {
		return err
	}

	if isTest {
		fmt.Println(frq.cfg.String())
		fmt.Println("\ntest is successful")
		return nil
	}
	//frq.Logger = logging.NewLoggerWithConfig(&frq.cfg.LogConfig)

	if err := frq.init(); err != nil {
		frq.Logger.Errorf("load configuration failed: %s", err)
		return err
	}
	//Notify函数让signal包将输入信号转发到c。如果没有列出要传递的信号，会将所有输入信号传递到c；否则只传递列出的输入信号。
	//signal包不会为了向c发送信息而阻塞（就是说如果发送时c阻塞了，signal包会直接放弃）：调用者应该保证c有足够的缓存空间可以跟上期望的信号频率。对使用单一信号用于通知的通道，缓存为1就足够了。

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	if err := frq.start(); err != nil {
		frq.Logger.Error(err)
		return nil
	}

	for {
		select {
		case sig, _ := <-sigCh:
			frq.Logger.Errorf("Recevie signal(%s)", sig)
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT: // Stop
				frq.Logger.Errorf("Recevie signal(%s), and stoping...", sig)
				return frq.stop()
			case syscall.SIGHUP: //Reload
				frq.Logger.Errorf("Recevie signal(%s), and reloading...", sig)
				frq.reload()
			}
		}
	}

	return nil
}

func (frq *FreqControl) init() error {
	frq.Logger.Info("Init ...")
	if err := frq.initWorker(); err != nil {
		return err
	}
	if err := frq.initKafkaConsumerManager(); err != nil {
		return err
	}
	frq.Logger.Info("all Inited")
	return nil
}

func (frq *FreqControl) initKafkaConsumerManager() error {

	kcm, err := kafka.NewKafkaConsumerManager(&frq.cfg.KafkaConsumerConfig,
		frq.Logger, frq.consumeMsgCh)
	if err != nil {
		return err
	}
	frq.kafkaConsumerManager = kcm
	if err := frq.kafkaConsumerManager.Init(); err != nil {
		return err
	}
	return nil
}

func (frq *FreqControl) initWorker() error {

	var err1 error
	var rdsStorager RedisStorager            //add counterStorager for mainfeed

	switch frq.Scene {


	case "process_data":
		rdsStorager, err1 = process_data.NewRedisStorager(frq.Logger, &frq.cfg.RedisCluster)
	}
	if err1 != nil {
		frq.Logger.Infof("NEW %s RedisStorager faild:%s", frq.Scene, err1)
		return err1
	}


	frq.Logger.Infof("%s Control Redis Storager started", frq.Scene)
	frq.rediswr = rdsStorager

	frq.workers = make([]*Worker, frq.cfg.WorkerConfig.Routines)
	for i := 0; i < frq.cfg.WorkerConfig.Routines; i++ {
		worker, err := NewWorker(
			frq.Scene,
			i,
			frq.cfg,
			frq.Logger,
			frq.rediswr,
			frq.consumeMsgCh)
		if err != nil {
			frq.Logger.Errorf("initWorker fail: %s", err)
			return err
		}
		worker.WorkerCnf = frq.cfg.WorkerConfig
		frq.workers[i] = worker
	}
	frq.Logger.Info("init worker success")
	return nil
}

func (frq *FreqControl) start() error {
	frq.Logger.Info("Starting...")

	if err := frq.startWorker(); err != nil { //start  worker first then worker
		return err
	}

	if err := frq.kafkaConsumerManager.Start(); err != nil {
		return err
	}

	frq.Logger.Info("all Started")
	return nil
}
//简单使用就是在创建一个任务的时候wg.Add(1), 任务完成的时候使用wg.Done()来将任务减一。使用wg.Wait()来阻塞等待所有任务完成。
//再强制杀死进程时，触发该信号，等所有任务完成后结束
func (frq *FreqControl) startWorker() error {
	for i := 0; i < frq.cfg.WorkerConfig.Routines; i++ {
		worker := frq.workers[i]
		frq.wg.Add(1)
		go worker.Start(&frq.wg)
	}
	return nil
}

func (frq *FreqControl) stop() error { //stop consumer first then worker
	frq.Logger.Info("Stoping...")
	if err := frq.kafkaConsumerManager.Stop(); err != nil {
		return err
	}

	if err := frq.stopWorker(); err != nil {
		return err
	}
	frq.Logger.Info("Waiting")
	frq.wg.Wait()


	if err := frq.rediswr.CloseRedis(); err != nil {
		frq.Logger.Infof("redis storager.Close() failed: %s", err)
		return err
	}
	frq.Logger.Info("redis storager closed!")

	frq.Logger.Info("all Stoped")
	return nil
}

func (frq *FreqControl) stopWorker() error {
	frq.Logger.Info("stop woker")
	for i := 0; i < frq.cfg.WorkerConfig.Routines; i++ {
		worker := frq.workers[i]
		worker.Stop()
		frq.Logger.Infof("stop woker:%d", i)
	}

	return nil
}

func (frq *FreqControl) reload() error {
	frq.Logger.Info("Do nothing")
	return nil
}
