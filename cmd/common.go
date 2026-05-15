package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jing2uo/tdx2db/utils"
)

func GetToday() time.Time {
	return time.Now().Truncate(24 * time.Hour)
}

// TempDir / VipdocDir 默认走 $TMPDIR (Linux 通常 /tmp), 通过
// utils.GetCacheDir 在 package init 时创建唯一子目录。
// 当 $TMPDIR 容量不够 (如 tmpfs 已被占用大半) 时, 用 --temp 在调用方
// 切到磁盘大目录, 见 OverrideTempDir。
var TempDir, _ = utils.GetCacheDir()
var VipdocDir = filepath.Join(TempDir, "vipdoc")

// OverrideTempDir 把默认 TempDir 切到 parent 下的新 mkdtemp 目录,
// 同时更新 VipdocDir, 并清掉 package init 创建的原临时目录。
// 失败时不动现有 TempDir, 调用方可以照常退出。
func OverrideTempDir(parent string) error {
	if parent == "" {
		return nil
	}
	// 绝对化: 不然 mkdtemp 出来的是相对路径, 后面 datatool 走
	// exec.Command(toolPath) + cmd.Dir = cacheDir 时,
	// 子进程先 chdir 到相对 Dir, 再 execve 相对 Path,
	// 两段相对路径叠加 → "no such file or directory"。
	abs, err := filepath.Abs(parent)
	if err != nil {
		return fmt.Errorf("abs temp parent %s: %w", parent, err)
	}
	if err := os.MkdirAll(abs, 0755); err != nil {
		return fmt.Errorf("create temp parent %s: %w", abs, err)
	}
	dir, err := os.MkdirTemp(abs, "tdx2db-temp-")
	if err != nil {
		return fmt.Errorf("mkdir temp under %s: %w", abs, err)
	}
	oldTemp := TempDir
	TempDir = dir
	VipdocDir = filepath.Join(TempDir, "vipdoc")
	if oldTemp != "" {
		_ = os.RemoveAll(oldTemp)
	}
	return nil
}

