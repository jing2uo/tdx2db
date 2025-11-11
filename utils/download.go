package utils

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
)

// Download 封装下载任务
type Download struct {
	Url           string
	Target        string
	TotalSections int
}

// DownloadFile 下载文件并返回 HTTP 状态码。
// 若状态码为 404 或 200，error 为 nil。
// 若服务器不支持 Range，则自动降级为单线程下载（静默处理）。
func DownloadFile(url string, targetPath string) (int, error) {
	const totalSections = 5

	d := &Download{
		Url:           url,
		Target:        targetPath,
		TotalSections: totalSections,
	}

	// Step 1: HEAD 获取文件元信息
	r, err := d.getNewRequest("HEAD")
	if err != nil {
		return 0, fmt.Errorf("create HEAD request: %w", err)
	}
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		return 0, fmt.Errorf("execute HEAD request: %w", err)
	}
	defer res.Body.Close()

	statusCode := res.StatusCode

	if statusCode == http.StatusNotFound {
		return 404, nil
	}
	if statusCode != http.StatusOK {
		return statusCode, fmt.Errorf("unexpected status: %d", statusCode)
	}

	// Step 2: 检查是否能获取 Content-Length
	size, err := strconv.Atoi(res.Header.Get("Content-Length"))
	if err != nil || size <= 0 {
		return d.singleThreadDownload()
	}

	// Step 3: 检查服务器是否支持 Range
	testReq, _ := d.getNewRequest("GET")
	testReq.Header.Set("Range", "bytes=0-0")
	testResp, err := http.DefaultClient.Do(testReq)
	if err != nil {
		return 0, fmt.Errorf("range test request: %w", err)
	}
	defer testResp.Body.Close()

	if testResp.StatusCode != http.StatusPartialContent {
		// 不支持 Range -> 自动降级为单线程
		return d.singleThreadDownload()
	}

	// Step 4: 执行并发下载
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
			sections[i][1] = size - 1
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var sectionErr error

	for i, sec := range sections {
		wg.Add(1)
		go func(i int, sec [2]int) {
			defer wg.Done()
			if err := d.downloadSection(i, sec); err != nil {
				mu.Lock()
				if sectionErr == nil {
					sectionErr = err
				}
				mu.Unlock()
			}
		}(i, sec)
	}
	wg.Wait()

	if sectionErr != nil {
		// 自动降级
		return d.singleThreadDownload()
	}

	if err := d.mergeSections(sections); err != nil {
		return statusCode, fmt.Errorf("merge sections: %w", err)
	}

	return statusCode, nil
}

func (d *Download) getNewRequest(method string) (*http.Request, error) {
	r, err := http.NewRequest(method, d.Url, nil)
	if err != nil {
		return nil, fmt.Errorf("create %s request: %w", method, err)
	}
	r.Header.Set("User-Agent", "GenericDownloader/1.0")
	return r, nil
}

func (d *Download) downloadSection(i int, section [2]int) error {
	r, err := d.getNewRequest("GET")
	if err != nil {
		return fmt.Errorf("create section %d request: %w", i, err)
	}
	r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", section[0], section[1]))

	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return fmt.Errorf("execute section %d: %w", i, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected section %d status: %d", i, resp.StatusCode)
	}

	f, err := os.Create(fmt.Sprintf("%s.part%d", d.Target, i))
	if err != nil {
		return fmt.Errorf("create part file %d: %w", i, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return fmt.Errorf("write part %d: %w", i, err)
	}

	return nil
}

func (d *Download) mergeSections(sections [][2]int) error {
	f, err := os.Create(d.Target)
	if err != nil {
		return fmt.Errorf("create target: %w", err)
	}
	defer f.Close()

	for i := 0; i < len(sections); i++ {
		partFile := fmt.Sprintf("%s.part%d", d.Target, i)
		data, err := os.ReadFile(partFile)
		if err != nil {
			return fmt.Errorf("read part file %s: %w", partFile, err)
		}
		if _, err := f.Write(data); err != nil {
			return fmt.Errorf("write part %d: %w", i, err)
		}
		_ = os.Remove(partFile)
	}
	return nil
}

// singleThreadDownload 用于降级时的完整文件下载
func (d *Download) singleThreadDownload() (int, error) {
	resp, err := http.Get(d.Url)
	if err != nil {
		return 0, fmt.Errorf("single-thread GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return 404, nil
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	f, err := os.Create(d.Target)
	if err != nil {
		return resp.StatusCode, fmt.Errorf("create target: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return resp.StatusCode, fmt.Errorf("write target: %w", err)
	}

	return resp.StatusCode, nil
}
