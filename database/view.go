package database

import (
	"database/sql"
	"fmt"
)

var QfqViewName = "v_qfq_stocks"

func CreateQfqView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		s.symbol,
		s.date,
		s.volume,
		s.amount,
		ROUND(s.open * f.factor, 2) AS open,
		ROUND(s.high * f.factor, 2) AS high,
		ROUND(s.low * f.factor, 2) AS low,
		ROUND(s.close * f.factor, 2) AS close,
	FROM %s s
	JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
	`, QfqViewName, StocksSchema.Name, FactorSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", QfqViewName, err)
	}
	return nil
}
