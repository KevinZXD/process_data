package graphite

import (
	"sync"
)

// chanMetrics 用来监控chan的长度和容量
// 存放Channeler及指标名, string为${node_names}.${metric}
// 因需要goroutine-safe的添加，故此封装
type chanMetrics struct {
	sync.RWMutex
	m map[string]Channeler
}

func newChanMetrics() *chanMetrics {
	return &chanMetrics{m: make(map[string]Channeler)}
}

// MonitorChan 添加对某个chan的监控，
// 定期(FlushInterval)写入这个chan的长度和容量至时序数据库
// nodeName即为${node_names}，写入监控的数据为 ${node_name}.length, ${node_name}.capacity
func (cm *chanMetrics) Monitor(key string, channeler Channeler) {
	if global.cfg.Disable {
		return
	}
	global.logger.Debugf("monitor chan %s value %d", key)
	cm.Lock()
	cm.m[key] = channeler
	cm.Unlock()
}

//Delete 对某个chan的监控
func (cm *chanMetrics) Delete(key string) {
	cm.Lock()
	delete(cm.m, key)
	cm.Unlock()
}

//Length 监控chan的个数
func (cm *chanMetrics) Length() int {
	cm.RLock()
	l := len(cm.m)
	cm.RUnlock()
	return l
}
