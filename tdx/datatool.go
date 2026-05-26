package tdx

import (
	"embed"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

//go:embed  embed/*
var embedFS embed.FS
var startDateStr = "19901201"

// DatatoolCreate merges TDX incremental data into per-stock files.
//
// The embedded datatool binary is Linux/amd64 only, so it is used solely on
// Linux/amd64. Everywhere else (Windows, macOS, and non-amd64 Linux such as
// arm64), "day" uses the native Go implementation (NativeDayMerge), and
// "min"/"tick" print a notice and skip, since only "day" has a native port.
func DatatoolCreate(cacheDir, subCommand string, endDate time.Time) error {
	switch subCommand {
	case "day", "min", "tick":
	default:
		return errors.New("unsupported datatool subcommand: " + subCommand)
	}

	useEmbedded := runtime.GOOS == "linux" && runtime.GOARCH == "amd64"
	if !useEmbedded {
		if subCommand == "day" {
			return NativeDayMerge(filepath.Join(cacheDir, "vipdoc"))
		}
		fmt.Printf("⚠️  %s 子命令暂不支持 %s/%s，已跳过\n", subCommand, runtime.GOOS, runtime.GOARCH)
		return nil
	}

	return datatoolExec(cacheDir, subCommand, endDate)
}

func datatoolExec(cacheDir, subCommand string, endDate time.Time) error {
	toolPath, err := extractDatatool(cacheDir)
	if err != nil {
		return fmt.Errorf("failed to extract datatool: %w", err)
	}

	endDateStr := endDate.Format("20060102")

	cmd := exec.Command(toolPath, subCommand, "create", startDateStr, endDateStr)
	cmd.Dir = cacheDir
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to execute datatool command: %w", err)
	}

	return nil
}

func extractDatatool(cacheDir string) (string, error) {
	toolPath, err := extractFileFromEmbed(cacheDir, "embed/datatool")
	if err != nil {
		return "", fmt.Errorf("failed to extract binary: %w", err)
	}

	if _, err := extractFileFromEmbed(cacheDir, "embed/datatool.ini"); err != nil {
		return "", fmt.Errorf("failed to extract config: %w", err)
	}

	cmd := exec.Command(toolPath, "-h")
	cmd.Dir = cacheDir
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to execute datatool: %w", err)
	}

	return toolPath, nil
}

func extractFileFromEmbed(cacheDir string, srcPath string) (string, error) {
	destFileName := filepath.Base(srcPath)
	destPath := filepath.Join(cacheDir, destFileName)

	data, err := embedFS.ReadFile(srcPath)
	if err != nil {
		return "", fmt.Errorf("failed to read embedded file %s: %w", srcPath, err)
	}

	if err := os.WriteFile(destPath, data, 0755); err != nil {
		return "", fmt.Errorf("failed to write file %s: %w", destPath, err)
	}

	if runtime.GOOS != "windows" {
		if err := os.Chmod(destPath, 0755); err != nil {
			return "", fmt.Errorf("failed to set file permissions for %s: %w", destPath, err)
		}
	}

	return destPath, nil
}
