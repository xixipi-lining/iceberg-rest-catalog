package server

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/router"
	"github.com/xixipi-lining/iceberg-rest-catalog/logger"
)

var (
	cfgFile   string
	envFile   string
	envPrefix string
)

var rootCmd = &cobra.Command{
	Use:   "iceberg-rest-catalog",
	Short: "Iceberg Rest Catalog",
	Run: func(cmd *cobra.Command, args []string) {
		Run()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (only yaml and json were supported)")
	rootCmd.PersistentFlags().StringVar(&envFile, "env_file", "", "env file (ie. content KEY=VAL )")
	rootCmd.PersistentFlags().StringVar(&envPrefix, "env_prefix", "", "env variables prefix")
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			log.Panicf("read config error %s", err)
		}
	}
	if envFile != "" {
		if err := godotenv.Load(envFile); err != nil {
			panic(fmt.Errorf("error when load env file, %w", err))
		}
	}
	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
}

func initLogger() logger.Logger {
	return logger.NewLogger(&logger.Config{
		Debug:      viper.GetBool("log.debug"),
		FileName:   viper.GetString("log.filename"),
		MaxSize:    viper.GetInt("log.maxsize"),
		MaxBackups: viper.GetInt("log.maxbackups"),
		MaxAge:     viper.GetInt("log.maxage"),
		Compress:   viper.GetBool("log.compress"),
	})
}

func Run() {
	initConfig()
	log := initLogger()

	// 创建Gin引擎
	engine := gin.New()

	// 设置路由
	router.Setup(log)

	// 启动服务器
	port := os.Getenv("PORT")

	log.Infof("Server starting on port %s", port)
	if err := engine.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
