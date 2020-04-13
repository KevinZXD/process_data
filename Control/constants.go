package Control

const (
	FRQ_MSG_QPS              = "msg.qps"
	FRQ_MSG_INVALID          = "msg.invalid"
	FRQ_MSG_IGNORE           = "msg.ignore"
        FRQ_MSG_SUCC             = "msg.succ"
	FRQ_MSG_RDS_SUCCESS      = "msg.process_data.set_ok"
	FRQ_MSG_RDS_FAIL         = "msg.process_data.set_fail"
	FRQ_MSG_WRFILE_SUCCESS   = "msg.file.wrt_ok"
	FRQ_MSG_WRFILE_FAIL      = "msg.file.wrt_fail"
	FRQ_MSG_WRFILE_DIR__FAIL = "msg.file.wrt_dir_fail"
	FRQ_INPUT_CHAN_NODE_NAME = "inchan" // input chan
)
