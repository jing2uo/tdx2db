package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
)

const schemaVersionDocURL = "https://github.com/jing2uo/tdx2db/releases"

func schemaVersionIncompatible(dbMajor, codeMajor int) error {
	return fmt.Errorf(
		"\n数据库 schema 版本不兼容 (当前库: v%d.x, 需要: v%d.x)\n请阅读 %s 了解迁移方式",
		dbMajor, codeMajor, schemaVersionDocURL,
	)
}

func schemaVersionMissing(codeMajor int) error {
	return fmt.Errorf(
		"\n数据库 schema 版本缺失 (需要: v%d.x)\n请阅读 %s 了解迁移方式",
		codeMajor, schemaVersionDocURL,
	)
}

func writeSchemaVersion(db database.DataRepository) error {
	ver, err := db.ReadSchemaVersion()
	if err != nil {
		return err
	}
	if ver == "" {
		return db.WriteSchemaVersion()
	}
	dbMajor, err := strconv.Atoi(strings.SplitN(ver, ".", 2)[0])
	if err != nil {
		return fmt.Errorf("invalid schema version format: %q", ver)
	}
	if dbMajor != model.SchemaMajor {
		return schemaVersionIncompatible(dbMajor, model.SchemaMajor)
	}
	return nil
}

func checkSchemaVersion(db database.DataRepository) error {
	ver, err := db.ReadSchemaVersion()
	if err != nil {
		return err
	}
	if ver == "" {
		return schemaVersionMissing(model.SchemaMajor)
	}
	dbMajor, err := strconv.Atoi(strings.SplitN(ver, ".", 2)[0])
	if err != nil {
		return fmt.Errorf("invalid schema version format: %q", ver)
	}
	if dbMajor != model.SchemaMajor {
		return schemaVersionIncompatible(dbMajor, model.SchemaMajor)
	}
	return nil
}
