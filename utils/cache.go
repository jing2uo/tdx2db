package utils

import (
	"os"
	"path/filepath"
)

func GetCacheDir() (string, error) {
	cacheDir := os.TempDir()

	appDir := filepath.Join(cacheDir, "tdx2db-temp")
	if err := os.MkdirAll(appDir, 0755); err != nil {
		return "", err
	}

	return appDir, nil
}
