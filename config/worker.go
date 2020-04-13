package config

type WorkerConfig struct {
	Routines int `toml:"routines", json:"routines"`
}

func (c *WorkerConfig) Validate() error {
	if c.Routines == 0 {
		c.Routines = 12
	}
	return nil
}
