package utils

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

// 下载器默认值: 所有下载共享。
// 默认带浏览器 UA + 自动 Referer (按 URL host) + 重试退避, 应对偶发反爬与瞬时网络抖动。
const (
	defaultMaxAttempts  = 3
	defaultRetryBackoff = 3 * time.Second
	defaultUserAgent    = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

// DownloadOptions 控制单次下载的请求头 / 重试 / 超时。零值字段走默认:
// MaxAttempts<=0 → 3, RetryBackoff<=0 → 3s, Timeout==0 → 不限 (沿用大文件慢下载),
// Headers 始终在默认 UA + 自动 Referer 之上叠加 (同名覆盖)。
type DownloadOptions struct {
	Headers      map[string]string
	MaxAttempts  int
	RetryBackoff time.Duration
	Timeout      time.Duration // 每次尝试的 http.Client 超时; 0 = 不限
}

// Download 封装下载任务
type Download struct {
	Url           string
	Target        string
	TotalSections int
	client        *http.Client
	headers       map[string]string
}

// DownloadFile 下载文件并返回 HTTP 状态码 (向后兼容入口)。
// 走默认 options: 浏览器 UA + 自动 Referer + 3 次重试。
// 若状态码为 404 或 200，error 为 nil。
func DownloadFile(url string, targetPath string) (int, error) {
	return DownloadFileWithOptions(context.Background(), url, targetPath, DownloadOptions{})
}

// DownloadFileWithOptions 带请求头 / 重试 / 超时的下载。整体下载 (HEAD + 分段或单线程)
// 作为一次尝试, 失败按 RetryBackoff 递增退避重试, 直到成功或耗尽 MaxAttempts; 全程响应
// ctx 取消。404 视为确定性结果, 立即返回不重试。
func DownloadFileWithOptions(ctx context.Context, downloadURL, targetPath string, opt DownloadOptions) (int, error) {
	maxAttempts := opt.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	backoff := opt.RetryBackoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}

	d := &Download{
		Url:           downloadURL,
		Target:        targetPath,
		TotalSections: 5,
		client:        &http.Client{Timeout: opt.Timeout},
		headers:       buildHeaders(downloadURL, opt.Headers),
	}

	var lastStatus int
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return lastStatus, err
		}
		if attempt > 1 {
			delay := time.Duration(attempt-1) * backoff
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return lastStatus, ctx.Err()
			case <-timer.C:
			}
		}

		status, err := d.run(ctx)
		if err == nil {
			return status, nil
		}
		lastStatus, lastErr = status, err
	}
	return lastStatus, fmt.Errorf("download %s failed after %d attempts: %w", downloadURL, maxAttempts, lastErr)
}

// buildHeaders 在默认 UA + 自动 Referer (scheme://host/) 之上叠加调用方 headers (同名覆盖)。
func buildHeaders(downloadURL string, override map[string]string) map[string]string {
	h := map[string]string{"User-Agent": defaultUserAgent}
	if u, err := url.Parse(downloadURL); err == nil && u.Scheme != "" && u.Host != "" {
		h["Referer"] = u.Scheme + "://" + u.Host + "/"
	}
	for k, v := range override {
		h[k] = v
	}
	return h
}

// run 执行一次完整下载尝试, 返回 HTTP 状态码。404 / 200 时 error 为 nil。
func (d *Download) run(ctx context.Context) (int, error) {
	// Step 1: HEAD 获取文件元信息
	r, err := d.newRequest(ctx, "HEAD")
	if err != nil {
		return 0, fmt.Errorf("create HEAD request: %w", err)
	}
	res, err := d.client.Do(r)
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
		return d.singleThreadDownload(ctx)
	}

	// Step 3: 检查服务器是否支持 Range
	testReq, _ := d.newRequest(ctx, "GET")
	testReq.Header.Set("Range", "bytes=0-0")
	testResp, err := d.client.Do(testReq)
	if err != nil {
		return 0, fmt.Errorf("range test request: %w", err)
	}
	defer testResp.Body.Close()

	if testResp.StatusCode != http.StatusPartialContent {
		// 不支持 Range -> 自动降级为单线程 (Go 客户端会透明 gzip 解压)
		return d.singleThreadDownload(ctx)
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
			if err := d.downloadSection(ctx, i, sec); err != nil {
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
		return d.singleThreadDownload(ctx)
	}

	if err := d.mergeSections(sections); err != nil {
		return statusCode, fmt.Errorf("merge sections: %w", err)
	}

	return statusCode, nil
}

func (d *Download) newRequest(ctx context.Context, method string) (*http.Request, error) {
	r, err := http.NewRequestWithContext(ctx, method, d.Url, nil)
	if err != nil {
		return nil, fmt.Errorf("create %s request: %w", method, err)
	}
	for k, v := range d.headers {
		r.Header.Set(k, v)
	}
	return r, nil
}

func (d *Download) downloadSection(ctx context.Context, i int, section [2]int) error {
	r, err := d.newRequest(ctx, "GET")
	if err != nil {
		return fmt.Errorf("create section %d request: %w", i, err)
	}
	r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", section[0], section[1]))

	resp, err := d.client.Do(r)
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

// singleThreadDownload 用于降级时的完整文件下载 (Go 客户端透明处理 gzip Content-Encoding)。
func (d *Download) singleThreadDownload(ctx context.Context) (int, error) {
	req, err := d.newRequest(ctx, "GET")
	if err != nil {
		return 0, fmt.Errorf("create single-thread request: %w", err)
	}
	resp, err := d.client.Do(req)
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
