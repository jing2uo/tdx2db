package model

import (
	"testing"
	"time"
)

func TestSchemaFromStructNullableTag(t *testing.T) {
	type row struct {
		Date time.Time `col:"date" type:"date" nullable:"true"`
	}

	meta := SchemaFromStruct("test_nullable_schema", row{}, []string{"date"})
	if len(meta.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(meta.Columns))
	}
	if !meta.Columns[0].Nullable {
		t.Fatalf("expected nullable column")
	}
}
