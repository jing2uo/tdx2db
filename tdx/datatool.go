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

//datatool [day,tick,min] create 19901201 20250610

func DatatoolCreate(cacheDir, subCommand string, endDate time.Time) error {
	switch subCommand {
	case "day", "min", "tick":
		//
	default:
		return errors.New("unsupported datatool subcommand: " + subCommand)
	}

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
