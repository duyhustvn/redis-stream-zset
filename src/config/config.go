package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	RedisConfig RedisConfig  `mapstructure:"redis"`
	Server      ServerConfig `mapstructure:"server"`
}

func LoadConfig() (config *Config, err error) {
	v := viper.New()

	v.SetDefault("server.port", 8081)
	v.SetDefault("server.mode", "stream")

	v.SetDefault("redis.mode", "standalone")
	v.SetDefault("redis.address", "localhost:6379")
	v.SetDefault("redis.password", "changeme")
	v.SetDefault("redis.master_name", "")

	// config read from yaml
	v.AddConfigPath(".") // search at this directory
	v.SetConfigName("config")
	v.SetConfigType("yaml")

	// Config read from env
	v.AutomaticEnv()
	// Change "." in struct to "_" in ENV (server.port -> SERVER_PORT)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err = v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// error is not "file not found"
			return
		}
	}

	// load data to struct
	err = v.Unmarshal(&config)
	return
}
