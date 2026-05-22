package tdx

import (
	"testing"

	"github.com/jing2uo/tdx2db/model"
)

func TestIsOnlineSymbolNameClassKeepsBShare(t *testing.T) {
	tests := []struct {
		name  string
		class string
		want  bool
	}{
		{name: "stock", class: model.ClassStock, want: true},
		{name: "bstock", class: model.ClassBStock, want: true},
		{name: "etf", class: model.ClassETF, want: true},
		{name: "index", class: model.ClassIndex, want: true},
		{name: "block", class: model.ClassBlock, want: false},
		{name: "unknown", class: model.ClassUnknown, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isOnlineSymbolNameClass(tt.class); got != tt.want {
				t.Fatalf("isOnlineSymbolNameClass(%q) = %v, want %v", tt.class, got, tt.want)
			}
		})
	}
}
