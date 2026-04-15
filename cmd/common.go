package cmd

import (
	"path/filepath"
	"time"

	"github.com/jing2uo/tdx2db/utils"
)

func GetToday() time.Time {
	return time.Now().Truncate(24 * time.Hour)
}

var TempDir, _ = utils.GetCacheDir()
var VipdocDir = filepath.Join(TempDir, "vipdoc")

