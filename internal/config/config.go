package config

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	Address        string
	DataDir        string
	MetadataPath   string
	Destination    Destination
	Runner         Runner
	RequestTimeout time.Duration
}

type Destination struct {
	Type string
	Path string
}

type Runner struct {
	Type   string
	Binary string
}

func Load() Config {
	dataDir := env("POMPOS_DATA_DIR", "./data")
	destinationPath := env("POMPOS_DESTINATION_PATH", joinDataPath(dataDir, "pompos.duckdb"))
	return Config{
		Address:      env("POMPOS_ADDRESS", ":8080"),
		DataDir:      dataDir,
		MetadataPath: env("POMPOS_METADATA_PATH", joinDataPath(dataDir, "pompos.sqlite")),
		Destination: Destination{
			Type: "duckdb",
			Path: destinationPath,
		},
		Runner: Runner{
			Type:   "ingestr",
			Binary: env("POMPOS_INGESTR_BINARY", "ingestr"),
		},
		RequestTimeout: 10 * time.Second,
	}
}

func joinDataPath(directory, name string) string {
	path := filepath.Join(directory, name)
	if strings.HasPrefix(directory, "."+string(filepath.Separator)) && !strings.HasPrefix(path, "."+string(filepath.Separator)) {
		return "." + string(filepath.Separator) + path
	}
	return path
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
