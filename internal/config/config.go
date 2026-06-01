package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	StorageProvider     string
	FilesystemRoot      string
	ObjectNamespace     string
	HTTPAddress         string
	MaxPageSize         int
	ReadLookbackMinutes int
}

func LoadFromEnv() (Config, error) {
	config := Config{
		StorageProvider:     getEnv("S3MS_STORAGE_PROVIDER", "filesystem"),
		FilesystemRoot:      getEnv("S3MS_FILESYSTEM_ROOT", ".s3-message-data"),
		ObjectNamespace:     os.Getenv("S3MS_OBJECT_NAMESPACE"),
		HTTPAddress:         getEnv("S3MS_HTTP_ADDR", ":8080"),
		MaxPageSize:         getEnvInt("S3MS_MAX_PAGE_SIZE", 100),
		ReadLookbackMinutes: getEnvInt("S3MS_READ_LOOKBACK_MINUTES", 43200),
	}
	if config.StorageProvider != "filesystem" {
		return Config{}, fmt.Errorf("unsupported storage provider %q", config.StorageProvider)
	}
	if config.MaxPageSize <= 0 {
		return Config{}, fmt.Errorf("S3MS_MAX_PAGE_SIZE must be positive")
	}
	if config.ReadLookbackMinutes <= 0 {
		return Config{}, fmt.Errorf("S3MS_READ_LOOKBACK_MINUTES must be positive")
	}
	return config, nil
}

func getEnv(name string, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
