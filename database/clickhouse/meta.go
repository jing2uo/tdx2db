package clickhouse

import (
	"database/sql"
	"fmt"

	"github.com/jing2uo/tdx2db/model"
)

// _meta 表：单实例元数据（schema 版本等）。
// 未来计划扩展：每次 init / cron 写一条 run 记录用于审计与数据校验。
const chMetaTable = "_meta"

func (d *ClickHouseDriver) ReadSchemaVersion() (string, error) {
	var value string
	err := d.db.Get(&value,
		fmt.Sprintf("SELECT value FROM %s WHERE key = 'schema_version'", chMetaTable))
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to read schema version: %w", err)
	}
	return value, nil
}

func (d *ClickHouseDriver) WriteSchemaVersion() error {
	_, err := d.db.Exec(
		fmt.Sprintf("INSERT INTO %s (key, value) VALUES ('schema_version', ?)", chMetaTable),
		fmt.Sprintf("%d.%d", model.SchemaMajor, model.SchemaMinor),
	)
	if err != nil {
		return fmt.Errorf("failed to write schema version: %w", err)
	}
	return nil
}
