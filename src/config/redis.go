package config

type RedisConfig struct {
	Address    string `mapstructure:"address"`
	Password   string `mapstructure:"password"`
	Mode       string `mapstructure:"mode"`
	MasterName string `mapstructure:"master_name"`
}

type ServerConfig struct {
	Port int    `mapstructure:"port"`
	Mode string `mapstructure:"mode"`
}
