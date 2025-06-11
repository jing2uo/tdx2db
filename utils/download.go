package utils

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
)

type Download struct {
	Url           string
	Target        string
	TotalSections int
}

func DownloadFile(url string, targetPath string) error {
	const totalSections = 5 // Default number of sections

	// Create download struct
	d := &Download{
		Url:           url,
		Target:        targetPath,
		TotalSections: totalSections,
	}

	// Get file size
	r, err := d.getNewRequest("HEAD")
	if err != nil {
		return fmt.Errorf("failed to create HEAD request: %w", err)
	}
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		return fmt.Errorf("failed to execute HEAD request: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode > 299 {
		return fmt.Errorf("server returned error status code: %d", res.StatusCode)
	}
	size, err := strconv.Atoi(res.Header.Get("Content-Length"))
	if err != nil || size <= 0 {
		return fmt.Errorf("invalid content length: %s", res.Header.Get("Content-Length"))
	}

	// Calculate sections
	eachSize := size / d.TotalSections
	sections := make([][2]int, d.TotalSections)
	for i := range sections {
		if i == 0 {
			sections[i][0] = 0
		} else {
			sections[i][0] = sections[i-1][1] + 1
		}
		if i < d.TotalSections-1 {
			sections[i][1] = sections[i][0] + eachSize
		} else {
			sections[i][1] = size - 1 // Adjust end of last section
		}
	}

	// Concurrently download sections
	var wg sync.WaitGroup
	for i, section := range sections {
		wg.Add(1)
		go func(i int, section [2]int) {
			defer wg.Done()
			if err := d.downloadSection(i, section); err != nil {
				fmt.Printf("Failed to download section %d: %v\n", i, err)
			}
		}(i, section)
	}
	wg.Wait()

	// Merge sections
	if err := d.mergeSections(sections); err != nil {
		return fmt.Errorf("failed to merge sections: %w", err)
	}

	return nil
}

func (d *Download) getNewRequest(method string) (*http.Request, error) {
	r, err := http.NewRequest(method, d.Url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s request: %w", method, err)
	}
	r.Header.Set("User-Agent", "Simple Downloader")
	return r, nil
}

func (d *Download) downloadSection(i int, section [2]int) error {
	r, err := d.getNewRequest("GET")
	if err != nil {
		return fmt.Errorf("failed to create GET request for section %d: %w", i, err)
	}
	r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", section[0], section[1]))
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return fmt.Errorf("failed to execute GET request for section %d: %w", i, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("server does not support partial content for section %d: status code %d", i, resp.StatusCode)
	}

	f, err := os.Create(fmt.Sprintf("%s.part%d", d.Target, i))
	if err != nil {
		return fmt.Errorf("failed to create part file for section %d: %w", i, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("failed to write section %d to file: %w", i, err)
	}

	return nil
}

func (d *Download) mergeSections(sections [][2]int) error {
	f, err := os.Create(d.Target)
	if err != nil {
		return fmt.Errorf("failed to create target file %s: %w", d.Target, err)
	}
	defer f.Close()

	for i := 0; i < len(sections); i++ {
		partFile := fmt.Sprintf("%s.part%d", d.Target, i)
		data, err := os.ReadFile(partFile)
		if err != nil {
			return fmt.Errorf("failed to read part file %s: %w", partFile, err)
		}
		if _, err := f.Write(data); err != nil {
			return fmt.Errorf("failed to write part %d to target file: %w", i, err)
		}
		if err := os.Remove(partFile); err != nil {
			return fmt.Errorf("failed to remove part file %s: %w", partFile, err)
		}
	}
	return nil
}
