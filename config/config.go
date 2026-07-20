package config

import (
	"errors"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server       ServerConfig
	Auth         AuthConfig
	Database     DatabaseConfig
	Pipelines    []PipelineConfig
	Subscription SubscriptionConfig
}

type ServerConfig struct {
	Port         int     `mapstructure:"port"`
	LogLevel     string  `mapstructure:"log_level"`
	Hostname     string  `mapstructure:"hostname"`
	ServiceDID   string  `mapstructure:"service_did"`
	PublisherDID string  `mapstructure:"publisher_did"`
	DIDContext   string  `mapstructure:"did_context"`
	ServiceID    string  `mapstructure:"service_id"`
	UserAgent    string  `mapstructure:"user_agent"`
	UserAgentURL string  `mapstructure:"user_agent_url"`
	AdminToken   string  `mapstructure:"admin_token"`
	Rate         float64       `mapstructure:"rate"`
	Limit        int           `mapstructure:"limit"`
	RateMaxAge   time.Duration `mapstructure:"rate_max_age"`
}

type AuthConfig struct {
	PDS        string `mapstructure:"pds"`
	Identifier string `mapstructure:"identifier"`
	Password   string `mapstructure:"password"`
}

type DatabaseConfig struct {
	DSN            string        `mapstructure:"dsn"`
	ReconnectDelay time.Duration `mapstructure:"reconnect_delay"`
}

type PipelineConfig struct {
	ShortName       string   `mapstructure:"short_name"`
	DisplayName     string   `mapstructure:"display_name"`
	Description     string   `mapstructure:"description"`
	Keywords        []string `mapstructure:"keywords"`
	ExcludeKeywords []string `mapstructure:"exclude_keywords"`
	ContextKeywords []string `mapstructure:"context_keywords"`
	ContextWords    []string `mapstructure:"context_words"`
	RequireMedia    bool     `mapstructure:"require_media"`
	BlockDIDs       []string `mapstructure:"block_dids"`
	Enabled         bool     `mapstructure:"enabled"`
	IgnoreBots      bool     `mapstructure:"ignore_bots"`
	CollectionName  string   `mapstructure:"collection_name"`
	Languages       []string `mapstructure:"languages"`
	LinkLabel       string   `mapstructure:"link_label"`
	LinkURL         string   `mapstructure:"link_url"`
}

type SubscriptionConfig struct {
	Endpoint       string        `mapstructure:"endpoint"`
	ReconnectDelay time.Duration `mapstructure:"reconnect_delay"`
	Concurrency    int           `mapstructure:"concurrency"`
	QueueSize      int           `mapstructure:"queue_size"`
}

func New(v *viper.Viper) (Config, error) {
	c := Config{}
	if v == nil {
		return c, errors.New("viper not initialized")
	}

	err := v.Unmarshal(&c)
	return c, err
}
