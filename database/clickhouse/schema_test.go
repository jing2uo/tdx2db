package clickhouse

import (
	"testing"

	"github.com/jing2uo/tdx2db/model"
)

func TestMapTypeNullableDate(t *testing.T) {
	got := (&ClickHouseDriver{}).mapType(model.Column{
		Name:     "delist_date",
		Type:     model.TypeDate,
		Nullable: true,
	})
	if got != "Nullable(Date32)" {
		t.Fatalf("expected Nullable(Date32), got %q", got)
	}
}

func TestMapTypeKeepsSymbolLowCardinality(t *testing.T) {
	got := (&ClickHouseDriver{}).mapType(model.Column{
		Name: "symbol",
		Type: model.TypeString,
	})
	if got != "LowCardinality(String)" {
		t.Fatalf("expected LowCardinality(String), got %q", got)
	}
}
