package calc

import (
	"math"
	"testing"
	"time"

	"github.com/jing2uo/tdx2db/model"
)

func date(y, m, d int) time.Time {
	return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
}

// TestXdxrFactorUpdate 验证除权日 prevClose != PreClose 时因子正确更新
// 场景：送股导致 preclose 被调整，factor 应乘以 prevClose/preclose
func TestXdxrFactorUpdate(t *testing.T) {
	// basic.PreClose 已由 calc/basic.go 正确计算（含非交易日映射）
	// calculateFullHfq 只需检测 prevClose != PreClose
	basics := []model.BasicDaily{
		{Symbol: "sz000001", Date: date(2007, 5, 31), Close: 28.69, PreClose: 27.32},
		// 2007-06-18 除权（非交易日），basic.go 已映射到 2007-06-20 并调整 PreClose
		{Symbol: "sz000001", Date: date(2007, 6, 20), Close: 31.19, PreClose: 26.081818181818186},
		{Symbol: "sz000001", Date: date(2007, 6, 21), Close: 34.31, PreClose: 31.19},
	}

	factors := calculateFullHfq(basics)

	if len(factors) != 3 {
		t.Fatalf("expected 3 factors, got %d", len(factors))
	}

	// 第一天：hfq = 1.0
	if factors[0].HfqFactor != 1.0 {
		t.Errorf("day 0: expected hfq=1.0, got %f", factors[0].HfqFactor)
	}

	// 2007-06-20：prevClose=28.69, preclose=26.08..., ratio≈1.1
	expectedHfq := 28.69 / 26.081818181818186
	if math.Abs(factors[1].HfqFactor-expectedHfq) > 1e-9 {
		t.Errorf("day 1 (2007-06-20): expected hfq≈%f, got %f", expectedHfq, factors[1].HfqFactor)
	}

	// 2007-06-21：无除权，hfq 不变
	if math.Abs(factors[2].HfqFactor-expectedHfq) > 1e-9 {
		t.Errorf("day 2 (2007-06-21): expected hfq≈%f, got %f", expectedHfq, factors[2].HfqFactor)
	}
}

// TestNoXdxrFactorUnchanged 验证无除权时因子保持不变
func TestNoXdxrFactorUnchanged(t *testing.T) {
	basics := []model.BasicDaily{
		{Symbol: "sz000001", Date: date(2020, 7, 20), Close: 15.0, PreClose: 14.5},
		{Symbol: "sz000001", Date: date(2020, 7, 21), Close: 16.0, PreClose: 15.0},
		{Symbol: "sz000001", Date: date(2020, 7, 22), Close: 15.5, PreClose: 16.0},
	}

	factors := calculateFullHfq(basics)

	for i, f := range factors {
		if f.HfqFactor != 1.0 {
			t.Errorf("day %d: expected hfq=1.0, got %f", i, f.HfqFactor)
		}
	}
}
