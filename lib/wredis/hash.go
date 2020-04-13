package wredis

import (
	"hash/fnv"
	"math/rand"
	"time"
    "strings"
    "strconv"
)

// 回调函数
type HashCallBack func(key string) uint64

func FNV32Hash(key string) uint64 {
	fnv32hash := fnv.New32()
	fnv32hash.Write([]byte(key))
	return uint64(fnv32hash.Sum32())
}

func FNV32aHash(key string) uint64 {
	fnv32ahash := fnv.New32a()
	fnv32ahash.Write([]byte(key))
	return uint64(fnv32ahash.Sum32())
}

func RANDSUM(key string) uint64 {
	t1 := time.Now().UnixNano() //取到当前时间，精确到纳秒
	//时间戳：当前的时间，距离1970年1月1日0点0分0秒的秒值，纳秒值
	rand.Seed(t1) //把当前时间作为种子传给计算机
	return rand.Uint64()
}

//key: mid_transmit_new 
func REMAINDER(key string) uint64 {
	keyArr := strings.Split(key, "_")
	if len(keyArr) < 3 {
		return 0
	}
       
        mid_last := keyArr[0][len(keyArr[0])-2 :]                                                                     
    keyUint64, _ := strconv.ParseUint(mid_last,10,64)                                                             
    return keyUint64       


}
	var HashHandler = map[string]HashCallBack{
	"FNV32":   FNV32Hash,
	"FNV32a":  FNV32aHash,
	"RANDSUM": RANDSUM, //目前用于主feedhash 写redis，redis key 比较单一，写socket 不均衡
	"REMAINDER": REMAINDER,
}
