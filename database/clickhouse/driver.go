package clickhouse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jmoiron/sqlx"
)

type ClickHouseDriver struct {
	dsn      string
	db       *sqlx.DB
	database string

	// HTTP
	httpImportUrl string
	authUser      string
	authPass      string

	viewImpls map[model.ViewID]func() error
}

func NewClickHouseDriver(u *url.URL) (*ClickHouseDriver, error) {
	q := u.Query()

	// HTTP 端口 (默认 8123)
	httpPort := q.Get("http_port")
	if httpPort == "" {
		httpPort = "8123"
	}
	q.Del("http_port")

	// 2. Host 必填
	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("clickhouse host is required")
	}

	// 3. TCP 端口 (默认 9000)
	tcpPort := u.Port()
	if tcpPort == "" {
		tcpPort = "9000"
	}

	// 4. 处理 Database (默认 "default")
	database := strings.TrimPrefix(u.Path, "/")
	if database == "" {
		database = "default"
	}
	u.Path = "/" + database

	// 5. 处理 User (默认 "default")
	user := u.User.Username()
	if user == "" {
		user = "default"
	}

	// 6. 处理 Password
	pass, passSet := u.User.Password()

	// 生成 HTTP 导入用的 Base URL
	httpImportUrl := fmt.Sprintf("http://%s:%s", host, httpPort)

	// 更新 URL 对象以生成最终 DSN
	u.Host = fmt.Sprintf("%s:%s", host, tcpPort)

	// 根据是否显式设置了密码（包括空密码）来重建 UserInfo
	if passSet {
		u.User = url.UserPassword(user, pass)
	} else {
		u.User = url.User(user)
	}

	u.RawQuery = q.Encode()

	finalDSN := u.String()

	return &ClickHouseDriver{
		dsn:           finalDSN,
		httpImportUrl: httpImportUrl,
		authUser:      user,
		authPass:      pass,
		database:      database,
		viewImpls:     make(map[model.ViewID]func() error),
	}, nil
}

func (d *ClickHouseDriver) pingHTTP() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", d.httpImportUrl, nil)
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}

	q := req.URL.Query()
	q.Add("query", "SELECT 1")
	req.URL.RawQuery = q.Encode()

	if d.authUser != "" {
		req.SetBasicAuth(d.authUser, d.authPass)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("http unreachable (%s): %w", d.httpImportUrl, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("http check failed (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

func (d *ClickHouseDriver) extractPort(u string) string {
	parsed, _ := url.Parse(u)
	return parsed.Port()
}

func (d *ClickHouseDriver) Connect() error {
	db, err := sqlx.Open("clickhouse", d.dsn)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(0)

	if err := db.Ping(); err != nil {
		return fmt.Errorf("clickhouse ping failed: %w", err)
	}

	if err := d.pingHTTP(); err != nil {
		_ = db.Close()
		return fmt.Errorf("tcp connected but http check (port %s) failed: %w",
			d.extractPort(d.httpImportUrl), err)
	}

	d.db = db
	return nil
}

func (d *ClickHouseDriver) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *ClickHouseDriver) InitSchema() error {
	tables := model.AllTables()

	for _, t := range tables {
		if err := d.createTableInternal(t); err != nil {
			return fmt.Errorf("failed to create table %s: %w", t.TableName, err)
		}
	}

	d.registerViews()
	for _, viewID := range model.AllViews() {
		implFunc, exists := d.viewImpls[viewID]
		if !exists {
			return fmt.Errorf("[ClickHouse] Missing implementation for view: %s", viewID)
		}
		if err := implFunc(); err != nil {
			return fmt.Errorf("failed to create view %s: %w", viewID, err)
		}
	}
	return nil
}
