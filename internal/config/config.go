package config

import (
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	PythonBinary string
	Address      string
	DataDir      string
	MetadataPath string
	Destination  Destination
}

type Destination struct {
	Type string
	Path string
}

func Load() Config {
	dataDir := env("POMPOS_DATA_DIR", "./data")
	destinationPath := env("POMPOS_DESTINATION_PATH", joinDataPath(dataDir, "pompos.duckdb"))
	return Config{
		PythonBinary: env("POMPOS_PYTHON_BINARY", "python3"),
		Address:      env("POMPOS_ADDRESS", "127.0.0.1:8080"),
		DataDir:      dataDir,
		MetadataPath: env("POMPOS_METADATA_PATH", joinDataPath(dataDir, "pompos.sqlite")),
		Destination: Destination{
			Type: "duckdb",
			Path: destinationPath,
		},
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
