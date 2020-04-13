/*
graphite 记录指标按照指定时间间隔发送这些指标到给定的Graphite

## 指标
生成规则：${project_name}.${service_name}.${IP}.${node_names}.${metric}
示例：alaya.correlate.10_75_29_40.consumer.idx_trace_log.qps
其中

	* ${project_name} 为项目名
	* ${service_name} 为服务名
	* ${ip} 为本机IP, 把分隔符`.`替换为下划线`_`，因为Graphite存储指标时以`.`分隔
	* ${node_names} 视具体情况而定
	* ${metric} 为具体的监控指标，如：qps, success, fail, time, ...

## 刷新规则

按照 `Config.FlushInterval` 时间间隔刷新，一般为1秒，1分钟。若为1分钟，则自动对齐时间刷新。
如当前时间为10:10:20, 那么第一次刷新动作的时刻为10:11:00，且指标的时间为10:10:00。

特别注意 所有指标均会折算成每秒多少

*/

package graphite

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"process_data/lib/logging"
	lnet "process_data/lib/net"
	ltime "process_data/lib/time"
)

var (
	global       *Graphite
	once         sync.Once
	defaultIPStr string = "127_0_0_1"
)

// Config 按照 FlushInterval 刷新所有指标至Address
//	* Prefix = ${project_name}.${service_name} 需在初始化graphite时传递，若Prefix为空，则不添加${ip}
type Config struct {
	Disable       bool           `toml:"disable" json:"disable"` // 是否关闭Graphite，若关闭，则不记录指标，也不发送指标
	Address       string         `toml:"address" json:"address"` // Graphite的写入地址
	Prefix        string         `toml:"prefix" json:"prefix"`
	FlushInterval ltime.Duration `toml:"flush_interval" json:"flush_interval"` // 最小单位为time.Second
}

func (c *Config) Validate() error {
	if c.Disable {
		return nil
	}
	if !lnet.IsValidAddress("tcp", c.Address) {
		return fmt.Errorf("graphite.address %s is invalid", c.Address)
	}

	if c.FlushInterval.Duration < time.Second {
		c.FlushInterval.Duration = time.Second
	}

	return nil
}

//Metric  描述一个指标
type Metric struct {
	Name  string // 即 ${metric}
	Value int64
}

type metricDB struct {
	sync.RWMutex
	m map[string]int64
}

func newMetricDB() *metricDB {
	return &metricDB{m: make(map[string]int64)}
}

// Add key指标的值增加value, key应该为${node_names}.${metric}
func (m *metricDB) Add(key string, value int64) {
	if global.cfg.Disable {
		return
	}
	global.logger.Debugf("key %s value %d", key, value)
	m.Lock()
	m.m[key] += value
	m.Unlock()
}

// Set key指标的值变更为value
func (m *metricDB) Set(key string, value int64) {
	if global.cfg.Disable {
		return
	}
	m.Lock()
	m.m[key] = value
	m.Unlock()
}

//Delete 删除某个指标key
func (m *metricDB) Delete(key string) {
	m.Lock()
	delete(m.m, key)
	m.Unlock()
}

type Graphite struct {
	cfg         *Config
	prefix      string // perfix = Config.Prefix + "." + IP  + "."
	m           *metricDB
	chanMetrics *chanMetrics
	logger      logging.Logger
	stopCh      chan int
}

func New(addr string, prefix string, flushInterval time.Duration, logger logging.Logger) *Graphite {

	if flushInterval < time.Second {
		flushInterval = time.Second
	}

	cfg := &Config{
		Address:       addr,
		Prefix:        prefix,
		FlushInterval: ltime.Duration{Duration: flushInterval},
	}

	return NewWithConfig(cfg, logger)
}

// NewWithConfig 通过配置文件创建，调用方需要保证配置的合法性
func NewWithConfig(cfg *Config, logger logging.Logger) *Graphite {
	once.Do(func() {
		global = newGraphite(cfg, logger)
	})

	return global
}

func newGraphite(cfg *Config, logger logging.Logger) *Graphite {

	ipv4, err := lnet.GetLocalIPv4Str()
	if err != nil {
		ipv4 = "127.0.0.1"
	}
	ipv4UnderlineStr := strings.Replace(ipv4, ".", "_", -1)

	prefix := ""
	if len(cfg.Prefix) != 0 {
		prefix = cfg.Prefix + "." + ipv4UnderlineStr + "."
	}

	g := &Graphite{
		cfg:         cfg,
		prefix:      prefix,
		logger:      logger,
		m:           newMetricDB(),
		chanMetrics: newChanMetrics(),
		stopCh:      make(chan int),
	}

	global = g

	return g
}

// Start 启动定期刷新指标到时序数据库(Graphite)的服务，服务将在goroutine中运行
func (g *Graphite) Start() {
	if g.cfg.Disable {
		return
	}
	// 时间对齐 假如刷新频率为1分钟，当前时间为 10:10:08 那么需要休眠 52秒再执行
	// 之后每整分钟时刻执行
	unixNano := time.Duration(time.Now().UnixNano())
	sleepDuration := global.cfg.FlushInterval.Duration - unixNano%global.cfg.FlushInterval.Duration
	time.Sleep(sleepDuration)

	for _ = range time.Tick(global.cfg.FlushInterval.Duration) {
		select {
		case <-g.stopCh:
			break
		default:
			go graphite()
		}
	}
}

// Stop 停掉Start启动的服务
func (g *Graphite) Stop() {
	if g.cfg.Disable {
		return
	}
	close(g.stopCh)
}

// Add key指标的值增加value, key应该为${node_names}.${metric}
func (g *Graphite) Add(key string, value int64) {
	g.m.Add(key, value)
}

// Add ${node_names}.${metric}的值增加value
func (g *Graphite) AddMetric(nodeName, meitricName string, value int64) {
	g.m.Add(nodeName+"."+meitricName, value)
}

// Add ${node_names}.qps的值增加value
func (g *Graphite) AddQPS(nodeName string, value int64) {
	g.m.Add(nodeName+"."+"qps", value)
}

// Add ${node_names}.*的值增加value, *为 metrics切片
func (g *Graphite) AddMetrics(nodeName string, metrics []Metric) {
	for _, metric := range metrics {
		g.m.Add(nodeName+"."+metric.Name, metric.Value)
	}
}

// Set 设置key指标的值为value, key应该为${node_names}.${metric}
func (g *Graphite) Set(key string, value int64) {
	g.m.Set(key, value)
}

// Set 设置${node_names}.${metric}指标的值为value
func (g *Graphite) SetMetric(nodeName, meitricName string, value int64) {
	g.m.Set(nodeName+"."+meitricName, value)
}

// Set 设置${node_names}.qps指标的值为value
func (g *Graphite) SetQPS(nodeName string, value int64) {
	g.m.Set(nodeName+"."+"qps", value)
}

// Set 设置${node_names}.*指标的值为value, *为 metrics切片
func (g *Graphite) SetMetrics(nodeName string, metrics []Metric) {
	for _, metric := range metrics {
		g.m.Set(nodeName+"."+metric.Name, metric.Value)
	}
}

// resetDB 重置DB数据库
func (g *Graphite) resetDB() map[string]int64 {
	m := g.m
	g.m = newMetricDB()
	return m.m
}

// MonitorChan 添加对某个chan的监控，
// 定期(FlushInterval)写入这个chan的长度和容量至时序数据库
// nodeName即为${node_names}，写入监控的数据为 ${node_name}.length, ${node_name}.capacity
func (g *Graphite) MonitorChan(key string, channeler Channeler) {
	g.chanMetrics.Monitor(key, channeler)
}

// Add 同 Graphite.Add
func Add(key string, value int64) {
	global.Add(key, value)
}

// AddMetric 同 Graphite.AddMetric
func AddMetric(nodeName, meitricName string, value int64) {
	global.AddMetric(nodeName, meitricName, value)
}

// AddQPS 同 Graphite.AddQPS
func AddQPS(nodeName string, value int64) {
	global.AddQPS(nodeName, value)
}

// AddMetrics 同 Graphite.AddMetrics
func AddMetrics(nodeName string, metrics []Metric) {
	global.AddMetrics(nodeName, metrics)
}

// Set 同 Graphite.Set
func Set(key string, value int64) {
	global.Set(key, value)
}

// SetMetric 同 Graphite.SetMetric
func SetMetric(nodeName, meitricName string, value int64) {
	global.SetMetric(nodeName, meitricName, value)
}

// SetQPS 同 Graphite.SetQPS
func SetQPS(nodeName string, value int64) {
	global.SetQPS(nodeName, value)
}

// SetMetrics 同 Graphite.SetMetrics
func SetMetrics(nodeName string, metrics []Metric) {
	global.SetMetrics(nodeName, metrics)
}

// MonitorChan 添加对某个chan的监控，
// 定期(FlushInterval)写入这个chan的长度和容量至时序数据库
// nodeName即为${node_names}，写入监控的数据为 ${node_name}.length, ${node_name}.capacity
func MonitorChan(nodeName string, channeler Channeler) {
	global.chanMetrics.Monitor(nodeName, channeler)
}

func graphite() {
	now := time.Now().Unix()
	now = now - now%int64((global.cfg.FlushInterval.Duration/time.Second))
	flushSeconds := float64(global.cfg.FlushInterval.Duration) / float64(time.Second)

	// 无论本次发送是否成功，均重置数据库
	m := global.resetDB()

	var w *bufio.Writer
	conn, err := net.Dial("tcp", global.cfg.Address)
	if err != nil {
		global.logger.Errorf("Dial(%s) failed: %s", global.cfg.Address, err)
		w = bufio.NewWriter(global.logger.Output())
	} else {
		defer conn.Close()

		w = bufio.NewWriter(conn)
	}
	count := 0
	for k, v := range m {
		count++
		if strings.HasSuffix(k, ".time") {
			timeValue := v

			// 针对xxx.time(耗时)指标，需要除以它的QPS
			qpsKey := strings.TrimSuffix(k, ".time") + ".qps"
			qpsValue, ok := m[qpsKey]
			if ok {
				timeValue = timeValue / qpsValue
			}
			fmt.Fprintf(w, "%s%s %0.2f %d\n", global.prefix, k, float64(timeValue)/flushSeconds, now)
			global.logger.Debugf("%s%s %0.2f %d", global.prefix, k, float64(timeValue)/flushSeconds, now)

		} else {
			fmt.Fprintf(w, "%s%s_count %d %d\n", global.prefix, k, v, now)
			global.logger.Debugf("%s%s_count %d %d\n", global.prefix, k, v, now)
			fmt.Fprintf(w, "%s%s %0.2f %d\n", global.prefix, k, float64(v)/flushSeconds, now)
			global.logger.Debugf("%s%s %0.2f %d", global.prefix, k, float64(v)/flushSeconds, now)
		}
	}

	// chan的长度和容量
	chanMetrics := global.chanMetrics
	if chanMetrics.Length() != 0 {
		chanMetrics.RLock()
		for k, v := range chanMetrics.m {
			fmt.Fprintf(w, "%s%s.length %d %d\n", global.prefix, k, v.Length(), now)
			global.logger.Debugf("%s%s.length %d %d\n", global.prefix, k, v.Length(), now)
			fmt.Fprintf(w, "%s%s.capacity %d %d\n", global.prefix, k, v.Capacity(), now)
			global.logger.Debugf("%s%s.capacity %d %d\n", global.prefix, k, v.Capacity(), now)
		}
		chanMetrics.RUnlock()
	}
	if err := w.Flush(); err != nil {
		global.logger.Errorf("flush failed: %s", err)
		return
	}
	global.logger.Infof("%d metrics flushed", count)
}

func Flush() {
	graphite()
}
