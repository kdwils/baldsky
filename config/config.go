package config

import (
	"errors"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server       ServerConfig
	Database     DatabaseConfig
	Pipelines    []PipelineConfig
	Subscription SubscriptionConfig
}

type ServerConfig struct {
	Port         int    `mapstructure:"port"`
	LogLevel     string `mapstructure:"log_level"`
	Hostname     string `mapstructure:"hostname"`
	ServiceDID   string `mapstructure:"service_did"`
	PublisherDID string `mapstructure:"publisher_did"`
	DIDContext   string `mapstructure:"did_context"`
	ServiceID    string `mapstructure:"service_id"`
}

type DatabaseConfig struct {
	DSN string `mapstructure:"dsn"`
}

type PipelineConfig struct {
	ShortName       string   `mapstructure:"short_name"`
	DisplayName     string   `mapstructure:"display_name"`
	Description     string   `mapstructure:"description"`
	Keywords        []string `mapstructure:"keywords"`
	ExcludeKeywords []string `mapstructure:"exclude_keywords"`
	RequireMedia    bool     `mapstructure:"require_media"`
	BlockDIDs       []string `mapstructure:"block_dids"`
	Enabled         bool     `mapstructure:"enabled"`
	IgnoreBots      bool     `mapstructure:"ignore_bots"`
	CollectionName  string   `mapstructure:"collection_name"`
}

type SubscriptionConfig struct {
	Endpoint       string        `mapstructure:"endpoint"`
	ReconnectDelay time.Duration `mapstructure:"reconnect_delay"`
}

func New(v *viper.Viper) (Config, error) {
	c := Config{}
	if v == nil {
		return c, errors.New("viper not initialized")
	}

	err := v.Unmarshal(&c)
	return c, err
}
