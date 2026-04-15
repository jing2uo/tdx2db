package utils

import (
	"archive/zip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// UnzipFile 解压 zip。默认跳过内嵌的 .zip 文件（用于 day4/tic4 等包含嵌套归档的场景）。
// 传 includeNestedZip=true 时会一并解压嵌套的 .zip 文件。
func UnzipFile(zipPath, targetPath string, includeNestedZip ...bool) error {
	includeZip := len(includeNestedZip) > 0 && includeNestedZip[0]

	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		if !includeZip && strings.HasSuffix(strings.ToLower(f.Name), ".zip") {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer rc.Close()

		path := filepath.Join(targetPath, f.Name)

		if f.FileInfo().IsDir() {
			// 创建目录
			err = os.MkdirAll(path, f.Mode())
			if err != nil {
				return err
			}
		} else {
			// 确保文件所在目录存在
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				return err
			}

			// 创建目标文件
			outFile, err := os.Create(path)
			if err != nil {
				return err
			}
			defer outFile.Close()

			// 复制文件内容
			_, err = io.Copy(outFile, rc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
