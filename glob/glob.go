package glob

import "gitlab.paradise-soft.com.tw/backend/yaitoo/cfg"

func LoadConfig() *cfg.Config {
	return cfg.Load("app.conf")
}
