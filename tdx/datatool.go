package tdx

import (
	"embed"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

//go:embed  embed/*
var embedFS embed.FS

func DatatoolCreate(cacheDir string, startDate, endDate time.Time) error {
	toolPath, err := extractDatatool(cacheDir)
	if err != nil {
		return fmt.Errorf("failed to extract datatool: %w", err)
	}

	// Format dates as YYYYMMDD
	startDateStr := startDate.Format("20060102")
	endDateStr := endDate.Format("20060102")

	//shell: ./datatool day create 20250601 20250610
	cmd := exec.Command(toolPath, "day", "create", startDateStr, endDateStr)
	cmd.Dir = cacheDir
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to execute datatool command: %w", err)
	}

	return nil
}

func extractDatatool(cacheDir string) (string, error) {
	toolPath, err := extractBinary(cacheDir)
	if err != nil {
		return "", err
	}

	if _, err := extractConfig(cacheDir); err != nil {
		return "", err
	}

	cmd := exec.Command(toolPath, "-h")
	cmd.Dir = cacheDir
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to execute datatool: %w", err)
	}

	return toolPath, nil
}

func extractBinary(cacheDir string) (string, error) {
	toolName := "datatool"
	if runtime.GOOS == "windows" {
		toolName += ".exe"
	}
	srcPath := filepath.Join("embed", toolName)
	destPath := filepath.Join(cacheDir, toolName)

	data, err := embedFS.ReadFile(srcPath)
	if err != nil {
		return "", fmt.Errorf("failed to read embedded binary for %s-%s: %w", runtime.GOOS, runtime.GOARCH, err)
	}

	if err := os.WriteFile(destPath, data, 0755); err != nil {
		return "", fmt.Errorf("failed to write binary file: %w", err)
	}

	if runtime.GOOS != "windows" {
		if err := os.Chmod(destPath, 0755); err != nil {
			return "", fmt.Errorf("failed to set binary permissions: %w", err)
		}
	}

	return destPath, nil
}

func extractConfig(cacheDir string) (string, error) {
	srcPath := "embed/datatool.ini"
	destPath := filepath.Join(cacheDir, "datatool.ini")

	srcFile, err := embedFS.Open(srcPath)
	if err != nil {
		return "", fmt.Errorf("failed to open embedded config: %w", err)
	}
	defer srcFile.Close()

	destFile, err := os.Create(destPath)
	if err != nil {
		return "", fmt.Errorf("failed to create config file: %w", err)
	}
	defer destFile.Close()

	if _, err := io.Copy(destFile, srcFile); err != nil {
		return "", fmt.Errorf("failed to copy config file: %w", err)
	}

	return destPath, nil
}
