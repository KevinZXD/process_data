package graphite

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"process_data/lib/logging"
	ltime "process_data/lib/time"
)

func floatEquals(a, b float64) bool {
	return (a-b) < 0.000001 && (b-a) < 0.000001
}

func NewTestServer(t *testing.T) (map[string]float64, net.Listener, *sync.WaitGroup) {
	res := make(map[string]float64)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not start dummy server:", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				t.Logf("dummy server error:%s", err)
				wg.Done()
				return
			}
			r := bufio.NewReader(conn)
			line, err := r.ReadString('\n')
			t.Logf("line: %d %s", len(line), line)
			for err == nil {
				parts := strings.Split(line, " ")
				i, _ := strconv.ParseFloat(parts[1], 0)
				if testing.Verbose() {
					t.Log("recv", parts[0], i)
				}
				res[parts[0]] = res[parts[0]] + i
				line, err = r.ReadString('\n')
			}
			conn.Close()
			wg.Done()
			return
		}
	}()

	return res, ln, &wg
}

func TestGraphite(t *testing.T) {

	res, l, wg := NewTestServer(t)
	defer l.Close()

	cfg := &Config{
		Address:       l.Addr().String(),
		Prefix:        "alaya.test",
		FlushInterval: ltime.Duration{Duration: time.Second},
	}

	g := NewWithConfig(cfg, logging.DefaultLogger())

	// time, qps
	Add("api1.qps", 10)                                    // 10
	AddQPS("api1", 10)                                     // 20
	AddMetric("api1", "qps", 10)                           // 30
	AddMetrics("api1", []Metric{{Name: "qps", Value: 10}}) // 40

	Add("api2.qps", 10) // 10
	Set("api2.qps", 2)  // 2

	Add("api3.qps", 10) // 10
	SetQPS("api3", 5)   // 5

	Add("api4.qps", 10)         // 10
	SetMetric("api4", "qps", 8) // 8

	Add("api5.qps", 10)                                   // 10
	SetMetrics("api5", []Metric{{Name: "qps", Value: 9}}) // 9

	graphite()

	time.Sleep(1 * time.Second)

	wg.Wait()
	t.Log(res)

	var tests = []struct {
		key    string
		except float64
	}{
		{g.prefix + "api1.qps", 40.0},
		{g.prefix + "api2.qps", 2.0},
		{g.prefix + "api3.qps", 5.0},
		{g.prefix + "api4.qps", 8.0},
		{g.prefix + "api5.qps", 9.0},
	}

	for _, tt := range tests {
		actual, _ := res[tt.key]
		if !floatEquals(tt.except, actual) {
			t.Fatalf("%s except %v actual %v", tt.key, tt.except, actual)
		}
	}
}
