package time

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

type TConfig struct {
	Timeout Duration `toml:"timeout" json:"timeout"`
}

// TestJsonParseDuration 从JSON中解析Duration
func TestJsonParseDuration(t *testing.T) {
	t1msJsonStr := `{"timeout":"1ms"}`
	t1ms := time.Millisecond

	c := TConfig{}
	if err := json.Unmarshal([]byte(t1msJsonStr), &c); err != nil {
		t.Error(err)
		return
	}
	t.Logf("\"1ms\" %d  1ms: %d", c.Timeout, t1ms)

	if t1ms != c.Timeout.Duration {
		t.Errorf("\"1ms\" %d != 1ms: %d", c.Timeout.Duration, t1ms)
	}

}

// TestTomlParseDuration 从TOML中解析Duration
func TestTomlParseDuration(t *testing.T) {
	t1msTomlStr := "timeout = \"1ms\""
	t1ms := time.Millisecond

	c := TConfig{}
	if m, err := toml.Decode(t1msTomlStr, &c); err != nil {
		t.Errorf("%s MetaData %v", err, m)
		return
	}
	t.Logf("\"1ms\" %d  1ms: %d", c.Timeout, t1ms)

	if t1ms != c.Timeout.Duration {
		t.Errorf("\"1ms\" %d != 1ms: %d", c.Timeout.Duration, t1ms)
	}

}

// TestParseDuration 从string中解析Duration
func TestParseDuration(t *testing.T) {
	var tests = []struct {
		s      string
		except time.Duration
	}{
		{"1ms", time.Millisecond},   // 1毫秒
		{"1s", time.Second},         // 1秒
		{"25s", 25 * time.Second},   // 25秒
		{"10m", 10 * time.Minute},   // 10分钟
		{"1h", time.Hour},           // 1小时
		{"48h", 2 * 24 * time.Hour}, // 2天
	}
	for i, tt := range tests {
		c := TConfig{}
		if err := c.Timeout.UnmarshalText([]byte(tt.s)); err != nil {
			t.Error(err)
			return
		}
		actual := c.Timeout.Duration
		if actual != tt.except {
			t.Errorf("case %d(\"%s\") actual %d except %d", i, tt.s, actual, tt.except)
		}
	}

}
