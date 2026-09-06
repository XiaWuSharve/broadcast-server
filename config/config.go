package config

import (
	"errors"
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

type Config struct {
	Protocol             string   `mapstructure:"protocol" yaml:"protocol"`
	Host                 string   `mapstructure:"host" yaml:"host"`
	Port                 int      `mapstructure:"port" yaml:"port"`
	ReadBufferSize       int      `mapstructure:"read-buffer-size" yaml:"read-buffer-size"`
	WriteBufferSize      int      `mapstructure:"write-buffer-size" yaml:"write-buffer-size"`
	TimeTolerance        int64    `mapstructure:"time-tolerance" yaml:"time-tolerance"`
	RegistryMaxBucketNum uint64   `mapstructure:"registry-max-bucket-num" yaml:"registry-max-bucket-num"`
	NsqdAddress          []string `mapstructure:"nsqd-address" yaml:"nsqd-address"`
	NsqlookupdAddress    string   `mapstructure:"nsqlookupd-address" yaml:"nsqlookupd-address"`
	ReceiverNum          int      `mapstructure:"receiver-num" yaml:"receiver-num"`
	SenderNum            int      `mapstructure:"sender-num" yaml:"sender-num"`
	ProcessorNum         int      `mapstructure:"processor-num" yaml:"processor-num"`
	NodeId               int64    `mapstructure:"node-id" yaml:"node-id"`
	Endpoint             string   `mapstructure:"endpoint" yaml:"endpoint"`
	Instance             string   `mapstructure:"instance" yaml:"instance"`
	AkId                 string   `mapstructure:"ak-id" yaml:"ak-id"`
	AkSecret             string   `mapstructure:"ak-secret" yaml:"ak-secret"`
}

var Cfg = Config{
	Protocol:             "kcp",
	Host:                 "0.0.0.0",
	Port:                 3001,
	ReadBufferSize:       1024,
	WriteBufferSize:      1024,
	TimeTolerance:        300,
	RegistryMaxBucketNum: 128,
	NsqdAddress:          []string{"localhost:4150"},
	NsqlookupdAddress:    "localhost:4161",
	ReceiverNum:          10,
	SenderNum:            10,
	ProcessorNum:         3,
	NodeId:               0,
	Endpoint:             "http://101.37.76.38:8084",
	Instance:             "x02caat39505",
	AkId:                 "abcdefg",
	AkSecret:             "abcdefg",
}

func Init(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file")
	// cmd
	cmd.PersistentFlags().String("protocol", Cfg.Protocol, "protocol for the server (support: kcp, websocket)")
	cmd.PersistentFlags().String("host", Cfg.Host, "host to bind")
	cmd.PersistentFlags().IntP("port", "p", Cfg.Port, "port to listen on")
	cmd.PersistentFlags().Int("read-buffer-size", Cfg.ReadBufferSize, "read buffer size")
	cmd.PersistentFlags().Int("write-buffer-size", Cfg.WriteBufferSize, "write buffer size")
	cmd.PersistentFlags().Int64("time-tolerance", Cfg.TimeTolerance, "time tolerance in seconds")
	cmd.PersistentFlags().Uint64("registry-max-bucket-num", Cfg.RegistryMaxBucketNum, "registry max bucket num")
	cmd.PersistentFlags().StringSlice("nsqd-address", Cfg.NsqdAddress, "nsqd address list")
	cmd.PersistentFlags().String("nsqlookupd-address", Cfg.NsqlookupdAddress, "nsqlookupd address")
	cmd.PersistentFlags().Int("receiver-num", Cfg.ReceiverNum, "receiver num")
	cmd.PersistentFlags().Int("sender-num", Cfg.SenderNum, "sender num")
	cmd.PersistentFlags().Int("processor-num", Cfg.ProcessorNum, "processor num")
	cmd.PersistentFlags().Int64("node-id", Cfg.NodeId, "node id")
	cmd.PersistentFlags().String("endpoint", Cfg.Endpoint, "endpoint")
	cmd.PersistentFlags().String("instance", Cfg.Instance, "instance")
	cmd.PersistentFlags().String("ak-id", Cfg.AkId, "ak id")
	cmd.PersistentFlags().String("ak-secret", Cfg.AkSecret, "ak secret")
}

func Bind(cmd *cobra.Command) {
	// env
	viper.SetEnvPrefix("whisperly")
	viper.AutomaticEnv()
	// config file
	if cfgFile != "" {
		// from cmd
		viper.SetConfigFile(cfgFile)
	} else {
		// from path
		env := viper.GetString("env")
		if env == "" {
			env = "prod"
		}
		viper.SetConfigName("config." + env) // 配置文件名称（没有文件扩展名）
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")                // 把当前目录加入到配置文件的搜索路径中
		viper.AddConfigPath("$HOME/.whisperly") // 配置文件搜索路径，可以设置多个配置文件搜索路径
	}
	if err := viper.ReadInConfig(); err != nil {
		// It's okay if the config file doesn't exist.
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			panic(err)
		}
		slog.Warn("config file not found, using higher level arguments")
	}
	// 绑定 pflag，但仅当 flag 被显式设置时才写入 viper
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		panic(err)
	}

	if err := viper.Unmarshal(&Cfg); err != nil {
		panic(err)
	}
}
