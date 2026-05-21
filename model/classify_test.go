package model

import "testing"

func TestClassifyCodeSeparatesBShare(t *testing.T) {
	tests := []struct {
		symbol string
		want   string
	}{
		{symbol: "sh600000", want: ClassStock},
		{symbol: "sz000001", want: ClassStock},
		{symbol: "sh900936", want: ClassBStock},
		{symbol: "sz200541", want: ClassBStock},
		{symbol: "sh510300", want: ClassETF},
	}

	for _, tt := range tests {
		t.Run(tt.symbol, func(t *testing.T) {
			if got := ClassifyCode(tt.symbol); got != tt.want {
				t.Fatalf("ClassifyCode(%q) = %q, want %q", tt.symbol, got, tt.want)
			}
		})
	}
}

func TestPriceScaleKeepsBSharePrecision(t *testing.T) {
	tests := []struct {
		symbol string
		want   float64
	}{
		{symbol: "sh600000", want: 100},
		{symbol: "sh900936", want: 1000},
		{symbol: "sz200541", want: 1000},
		{symbol: "sh510300", want: 1000},
	}

	for _, tt := range tests {
		t.Run(tt.symbol, func(t *testing.T) {
			if got := PriceScale(tt.symbol); got != tt.want {
				t.Fatalf("PriceScale(%q) = %v, want %v", tt.symbol, got, tt.want)
			}
		})
	}
}

func TestSymbolFromCodeKeepsBShareMapping(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{code: "600000", want: "sh600000"},
		{code: "900936", want: "sh900936"},
		{code: "200541", want: "sz200541"},
		{code: "510300", want: "sh510300"},
	}

	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			got, ok := SymbolFromCode(tt.code)
			if !ok {
				t.Fatalf("SymbolFromCode(%q) did not resolve", tt.code)
			}
			if got != tt.want {
				t.Fatalf("SymbolFromCode(%q) = %q, want %q", tt.code, got, tt.want)
			}
		})
	}
}
