package utils

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestDownloadFileRetryAndHeaders 验证: 整体下载失败后按退避重试; 默认补浏览器 UA +
// 自动 Referer (scheme://host/); 调用方 Headers 覆盖自动值。
func TestDownloadFileRetryAndHeaders(t *testing.T) {
	var (
		mu      sync.Mutex
		heads   int
		lastUA  string
		lastRef string
		gotBody = "stock-list-bytes"
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodHead {
			heads++
			if heads == 1 {
				w.WriteHeader(http.StatusBadGateway) // 第一次整体尝试失败
				return
			}
			w.WriteHeader(http.StatusOK) // 不带 Content-Length -> 走单线程 GET
			return
		}
		lastUA = r.Header.Get("User-Agent")
		lastRef = r.Header.Get("Referer")
		_, _ = w.Write([]byte(gotBody))
	}))
	defer srv.Close()

	target := filepath.Join(t.TempDir(), "out.bin")
	status, err := DownloadFileWithOptions(context.Background(), srv.URL+"/list.xlsx", target, DownloadOptions{
		Headers:      map[string]string{"Referer": "http://override.test/"},
		MaxAttempts:  3,
		RetryBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	if heads != 2 {
		t.Fatalf("expected 2 HEAD attempts (1 fail + 1 ok), got %d", heads)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != gotBody {
		t.Fatalf("content = %q err=%v", data, err)
	}
	if lastUA == "" {
		t.Fatal("expected default User-Agent to be set")
	}
	if lastRef != "http://override.test/" {
		t.Fatalf("Referer override not applied: %q", lastRef)
	}
}

// TestDownloadFileAutoReferer 验证未指定 Referer 时自动补 scheme://host/。
func TestDownloadFileAutoReferer(t *testing.T) {
	var gotRef string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		gotRef = r.Header.Get("Referer")
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	target := filepath.Join(t.TempDir(), "out.bin")
	if _, err := DownloadFileWithOptions(context.Background(), srv.URL+"/a.xlsx", target, DownloadOptions{MaxAttempts: 1}); err != nil {
		t.Fatalf("download: %v", err)
	}
	if gotRef != srv.URL+"/" {
		t.Fatalf("auto Referer = %q, want %q", gotRef, srv.URL+"/")
	}
}
