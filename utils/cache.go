package utils

import (
	"os"
)

func GetCacheDir() (string, error) {
	appDir, err := os.MkdirTemp("", "tdx2db-temp-")
	if err != nil {
		return "", err
	}

	return appDir, nil
}
