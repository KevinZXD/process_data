package Control

import (
	"process_data/lib/logging"
	"testing"
)

func TestProcess(t *testing.T) {
	var tests = []struct {
		LogMsg string
	}{
		{"ad_5c2205801427|pos56e566f71a201|33|CMCC|2780123000|iphone|3333_2001|108C393010|39.187.201.240|||||2018-12-26 18:59:57|iPhone9%2C2%5F%5Fweibo%5F%5F8.12.3%5F%5Fiphone%5F%5Fos12.0.1"},
		{"ad_5c2205801427|pos56e566f71a201|33|CMIC|2432234234|iphone|"},
	}
	logpath := "/tmp/"
	lognumber := 64
	for _, v := range tests {
		fs, err := NewFreqLogger([]byte(v.LogMsg), logpath, lognumber)
		if err != nil {
			t.Error("fail")
		}
		t.Logf("%+v", fs)
		freqLogger := logging.NewFreqLog(fs.LogFilePath, fs.LogFileName)
		if err := freqLogger.Validate(); err != nil {
			t.Errorf("error :%s", err)
		}
		t.Logf("%+v", freqLogger)
		_, errs := freqLogger.Write([]byte(v.LogMsg))
		if errs != nil {
			t.Errorf("errs: %s", errs)
		}
		t.Log("success")
	}
}

func BenchmarkProcess(b *testing.B) {
	for i := 0; i < b.N; i++ {
		logMsg := "ad_5c2205801427|pos56e566f71a201|33|CMCC|2780123000|iphone|3333_2001|108C393010|39.187.201.240|||||2018-12-26 18:59:57|iPhone9%2C2%5F%5Fweibo%5F%5F8.12.3%5F%5Fiphone%5F%5Fos12.0.1"
		logpath := "/tmp/pvlog/"
		lognumber := 64

		fs, _ := NewFreqLogger([]byte(logMsg), logpath, lognumber)
		freqLogger := logging.NewFreqLog(fs.LogFilePath, fs.LogFileName)
		if err := freqLogger.Validate(); err != nil {
			b.Errorf("err:%s", err)
		}
		_, errs := freqLogger.Write([]byte(logMsg))
		if errs != nil {
			b.Errorf("errs:%s", errs)
		}
	}
}
