package cmd

import (
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "baldsky",
	Short: "Bluesky bald-themed feed generator",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.baldsky.yaml)")
}

func initConfig() {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Printf("config: could not determine home directory: %v", err)
	}

	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		if home != "" {
			viper.AddConfigPath(home)
		}
		viper.AddConfigPath(".")
		viper.SetConfigName(".baldsky")
		viper.SetConfigType("yaml")
	}

	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.log_level", "info")
	viper.SetDefault("database.dsn", "")
	viper.SetDefault("feed.short_name", "")
	viper.SetDefault("feed.display_name", "")
	viper.SetDefault("feed.keywords", []string{})
	viper.SetDefault("feed.did_context", "https://www.w3.org/ns/did/v1")
	viper.SetDefault("feed.service_id", "#bsky_fg")
	viper.SetDefault("feed.collection_name", "app.bsky.feed.generator")
	viper.SetDefault("subscription.endpoint", "wss://bsky.network")
	viper.SetDefault("subscription.reconnect_delay", "5s")

	viper.SetEnvPrefix("BALDSKY")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Printf("config: %v", err)
	}
}
