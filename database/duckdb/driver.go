package duckdb

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

type DuckDBDriver struct {
	dsn       string
	db        *sqlx.DB
	viewImpls map[model.ViewID]func() error
}

func NewDuckDBDriver(u *url.URL) (*DuckDBDriver, error) {

	var dbPath string

	if u.Host == "." || u.Host == ".." {
		// 处理 ./ 或 ../
		dbPath = u.Host + u.Path
	} else if u.Host != "" {
		// 处理 duckdb://filename.db
		dbPath = u.Host + u.Path
	} else {
		// 处理 duckdb:///absolute/path.db
		dbPath = u.Path
	}

	dbPath = strings.TrimSpace(dbPath)

	// 禁止内存模式
	if dbPath == "" || dbPath == ":memory:" {
		return nil, fmt.Errorf("duckdb driver: memory mode is not allowed, please provide a file path (e.g. duckdb://data.db)")
	}
	// home 目录展开
	if strings.HasPrefix(dbPath, "~/") || strings.HasPrefix(dbPath, "~\\") || dbPath == "~" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get user home directory: %w", err)
		}

		if dbPath == "~" {
			dbPath = homeDir
		} else {
			dbPath = filepath.Join(homeDir, dbPath[1:])
		}
	}

	// 3. 准备目录
	dir := filepath.Dir(dbPath)
	if dir != "." && dir != "/" {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create directory for duckdb file: %w", err)
			}
		}
	}

	if u.RawQuery != "" {
		dbPath = fmt.Sprintf("%s?%s", dbPath, u.RawQuery)
	}

	return &DuckDBDriver{
		dsn:       dbPath,
		viewImpls: make(map[model.ViewID]func() error),
	}, nil
}

func (d *DuckDBDriver) Connect() error {
	db, err := sqlx.Open("duckdb", d.dsn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	db.Mapper = reflectx.NewMapperFunc("col", strings.ToLower)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return fmt.Errorf("duckdb ping failed (check file permissions): %w", err)
	}

	d.db = db
	return nil
}

func (d *DuckDBDriver) Close() error {
	return d.db.Close()
}

func (d *DuckDBDriver) InitSchema() error {
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
			return fmt.Errorf("[DuckDB] Missing implementation for required view: %s", viewID)
		}

		if err := implFunc(); err != nil {
			return fmt.Errorf("failed to create view %s: %w", viewID, err)
		}
	}

	return nil
}
