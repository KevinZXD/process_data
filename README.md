消费kafka数据 
原理：启动两个kafka consumer 将数据拉到一个channel，启动64个协程去处理
注意：监控信号，强制杀死进程会统一等待所有worker执行完，关闭数据资源退出，不会丢失数据

部署机器：
xxxxxx

部署路径：
/data0/process_data

启动方式：
/data0/process_data/process_data_linux64 freqControl server --config=configs/Control.process_data.toml &

每次部署项目时需要统一init下项目才能，跟踪调试代码
replace github.com/bsm/sarama-cluster => github.com/CHneger/sarama-cluster v0.0.0-20200108115656-95c44d29f408


