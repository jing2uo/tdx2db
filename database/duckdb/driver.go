package duckdb

import (
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jmoiron/sqlx"
)

type DuckDBDriver struct {
	dsn       string
	db        *sqlx.DB
	viewImpls map[model.ViewID]func() error
}

func NewDriver(cfg model.DBConfig) *DuckDBDriver {
	return &DuckDBDriver{dsn: cfg.DSN, viewImpls: make(map[model.ViewID]func() error)}
}

func (d *DuckDBDriver) Connect() error {
	db, err := sqlx.Open("duckdb", d.dsn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(0)

	d.db = db
	return nil
}

func (d *DuckDBDriver) Close() error {
	return d.db.Close()
}

func (d *DuckDBDriver) InitSchema() error {
	// 1. 定义需要创建的表 (通过 Model 获取 Meta)
	tables := model.AllTables()

	// 2. 循环建表
	for _, t := range tables {
		// 注意：t 现在是 *model.TableMeta
		if err := d.createTableInternal(t); err != nil {
			return fmt.Errorf("failed to create table %s: %w", t.TableName, err)
		}
	}
	// 3. 创建视图
	d.registerViews()
	for _, viewID := range model.AllViews() {

		// 检查：我(DuckDB)是否实现了这个View？
		implFunc, exists := d.viewImpls[viewID]
		if !exists {
			// 核心逻辑：一旦发现漏网之鱼，直接报错！
			return fmt.Errorf("[DuckDB] Missing implementation for required view: %s", viewID)
		}

		// 执行实现逻辑
		if err := implFunc(); err != nil {
			return fmt.Errorf("failed to create view %s: %w", viewID, err)
		}
	}

	return nil
}
