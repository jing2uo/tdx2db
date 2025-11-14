package utils

import (
	"fmt"
	"os"
)

func CheckDirectory(path string) error {
	fileInfo, err := os.Stat(path)
	if os.IsNotExist(err) {
		return fmt.Errorf("the directory does not exist: %s", path)
	}
	if err != nil {
		return fmt.Errorf("error checking %s: %w", path, err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("the specified path %s is not a directory", path)
	}
	return nil
}

func CheckFile(path string) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("the file does not exist: %s", path)
		}
		return err
	}
	if !fileInfo.Mode().IsRegular() {
		return fmt.Errorf("the specified path %s is not a file", path)
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsPermission(err) {
			return fmt.Errorf("could not read the file: %s", err)
		}
		return err
	}
	file.Close()
	return nil
}

func CheckOutputDir(path string) error {
	fileInfo, err := os.Stat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("could not create output directory %s: %w", path, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("could not access output directory %s: %w", path, err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("the specified output path is not a directory: %s", path)
	}

	tmpFile, err := os.CreateTemp(path, "test-")
	if err != nil {
		return fmt.Errorf("output directory %s is not writable: %w", path, err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())

	return nil
}
