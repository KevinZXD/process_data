
package Control

import (
	"errors"
	"strconv"
	"time"
    "encoding/json"
	//"github.com/json-iterator/go"
	"process_data/lib/wredis"
)
//var rjson jsoniter.API = jsoniter.ConfigCompatibleWithStandardLibrary
//var rjson = jsoniter.ConfigCompatibleWithStandardLibrary
//水电费

type ResultST struct {
	Uid     string `json:"uid"`
	Mid     string  `json:"mid"`
	Follow     int  `json:"follow"`
	Src_Uid     string `json:"src_uid"`
	Src_Mid     string `json:"src_mid"`
	//Content     string `json:"content"`
	State     int `json:"state"`
	Event     int  `json:"event"`
}

type process_dataLogStruct struct {
    SrcMid             string
	TransmitUid        string
	TransmitMid        string
	TransmitFollowerCount int 
	LogFilePath string
	LogFileName string
}



func Newprocess_dataFreqLogger(
	msg []byte,
	logpath string,
	lognumber int,
) (*process_dataLogStruct, error) {

	var resSt ResultST
	if err := json.Unmarshal(msg, &resSt); err != nil {

		 return nil,err
	}
	//resSt := &ResultST{}
	//if err := rjson.Unmarshal(msg, resSt); err != nil {

	//	return nil,err 
	//}
	if resSt.Event !=2 {
		return nil, errors.New("Event is valid")
	}
    st:=resSt.State
	if  (st>=25 && st<= 34) || (st>=5 && st<= 11) || st==16 || st==17 || st==20 || st==36 || st==41 || st==42 || st==3   {
		return nil, errors.New("State is valid")
	}
    Follow:=resSt.Follow
	if Follow <=150 {
		return nil, errors.New("Follow is below 100")
	}

	if len(resSt.Uid) <=0 {
		return nil, errors.New("uid is valid")
	}

	if len(resSt.Mid) <=0 {
		return nil, errors.New("Mid is valid")
	}

	if len(resSt.Src_Uid) <=0 || len(resSt.Src_Mid) <=0 {
		return nil, errors.New("Src_Uid is valid or Src_Mid is valid")
	}


	uid:=resSt.Uid
    mid:=resSt.Mid
    srcmid:=resSt.Src_Mid
	follow_num:=resSt.Follow



	fileDateDir := time.Now().Format("2006-01-02/15/")
	bucket := wredis.FNV32aHash(mid) % uint64(lognumber)
	loggerStruct := process_dataLogStruct{
        SrcMid:        srcmid,
		TransmitUid:   uid,
		TransmitMid:   mid,
		TransmitFollowerCount:   follow_num,
		LogFilePath: logpath + fileDateDir,
		LogFileName: strconv.FormatUint(bucket, 10) + "_pvlog.txt",
	}
	return &loggerStruct, nil
}
func (l *process_dataLogStruct) Ignore() bool {
	if len(l.TransmitUid) == 0 || len(l.TransmitMid) == 0 {
		return true
	}
	return false
}



func (l *process_dataLogStruct) GetRedisKey() string {
	if len(l.SrcMid) > 0 {
		return l.SrcMid + "_transmit_new"
	}
	return ""



}


