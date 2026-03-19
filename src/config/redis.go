package config

type RedisConfig struct {
	Address    string `mapstructure:"address"`
	Password   string `mapstructure:"password"`
	Mode       string `mapstructure:"mode"`
	MasterName string `mapstructure:"master_name"`
	EnableGzip bool   `mapstructure:"enable_gzip"`
}

type ServerConfig struct {
	Port int    `mapstructure:"port"`
	Mode string `mapstructure:"mode"`
}
