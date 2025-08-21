package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"syscall"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/glue"
	_ "github.com/apache/iceberg-go/catalog/rest"
	_ "github.com/apache/iceberg-go/catalog/sql"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/oklog/run"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/handlers"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/middleware"
	"github.com/xixipi-lining/iceberg-rest-catalog/api/router"
	"github.com/xixipi-lining/iceberg-rest-catalog/logger"
	"gopkg.in/yaml.v3"

	_ "github.com/mattn/go-sqlite3"
)

const (
	cfgFile = ".iceberg-go.yaml"
)

type Config struct {
	DefaultCatalog string                        `yaml:"default-catalog"`
	Catalogs       map[string]iceberg.Properties `yaml:"catalog"`

	ServerConfig handlers.Config `yaml:"server"`

	LogConfig logger.Config `yaml:"log"`
	Port      int           `yaml:"port"`
	Host      string        `yaml:"host"`
}

func loadConfig(configPath string) (*Config, error) {
	var path string
	if len(configPath) > 0 {
		path = configPath
	} else {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		path = filepath.Join(homeDir, cfgFile)
	}

	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := Config{
		DefaultCatalog: "default",
		LogConfig: logger.Config{
			Debug:    true,
			MaxSize:  100,
			Compress: false,
		},
		ServerConfig: handlers.Config{
			Defaults:  map[string]string{},
			Overrides: map[string]string{},
		},
		Port: 8080,
		Host: "127.0.0.1",
	}
	err = yaml.Unmarshal(file, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func fromConfigFiles() (*Config, error) {
	dir := os.Getenv("GOICEBERG_HOME")
	if dir != "" {
		dir = filepath.Join(dir, cfgFile)
	}

	return loadConfig(dir)
}

func main() {
	cfg, err := fromConfigFiles()
	if err != nil {
		panic(err)
	}

	props, ok := cfg.Catalogs[cfg.DefaultCatalog]
	if !ok {
		panic(fmt.Sprintf("catalog %s not found", cfg.DefaultCatalog))
	}

	cat, err := catalog.Load(context.Background(), cfg.DefaultCatalog, props)
	if err != nil {
		panic(err)
	}

	txCat, ok := cat.(catalog.TransactionCatalog)
	if !ok {
		panic("catalog is not a transaction catalog")
	}

	handler := handlers.NewCatalogHandler(cfg.ServerConfig, txCat)

	log := logger.NewLogger(&cfg.LogConfig)

	engine := gin.New()
	engine.Use(middleware.Logger(log))
	engine.Use(cors.Default())
	engine.Use(gin.Recovery())

	router := router.Setup(engine, handler)

	svc := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler: router,
	}

	g := &run.Group{}

	g.Add(func() error {
		return svc.ListenAndServe()
	}, func(err error) {
		if err := svc.Shutdown(context.Background()); err != nil {
			log.Errorf("failed to shutdown: %s", err)
		}
	})

	g.Add(run.SignalHandler(context.Background(), syscall.SIGINT, syscall.SIGTERM))

	if err := g.Run(); err != nil {
		log.Errorf("failed to run: %s", err)
		os.Exit(1)
	}

}
