package database

import (
	"database/sql"
	"fmt"
)

// CreateQfqView 创建或更新用于前复权行情数据的视图
func CreateQfqView(db *sql.DB) error {
	query := `
	CREATE OR REPLACE VIEW stocks_qfq AS
	SELECT
		s.symbol,
		s.date,
		s.volume,
		s.amount,
		ROUND(s.open * f.factor, 2) AS open,
		ROUND(s.high * f.factor, 2) AS high,
		ROUND(s.low * f.factor, 2) AS low,
		ROUND(s.close * f.factor, 2) AS close,
	FROM stocks s
	JOIN factor f ON s.symbol = f.symbol AND s.date = f.date
	`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view stocks_qfq: %w", err)
	}
	return nil
}
