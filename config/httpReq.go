package config

import (
	"fmt"
	"time"

	ltime "process_data/lib/time"
)

type HttpReqConfig struct {
	Url        string         `toml:"url" json:"url"`
	Method     string         `toml:"method" json:"method"`
	UrlTimeout ltime.Duration `toml:"timeout" json:"timeout"`
}

type ResponseST struct {
	Result ResultST `json:"result"`
}

type ResultST struct {
	Received int `json:"received"`
	Sent     int `json:"sent"`
}

func (hr *HttpReqConfig) Validate() error {
	if len(hr.Url) == 0 {
		return fmt.Errorf("url is empty")
	}
	if hr.UrlTimeout.Duration == 0 {
		hr.UrlTimeout.Duration = 50 * time.Millisecond
	}

	return nil
}
