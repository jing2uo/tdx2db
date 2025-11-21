package main

import (
	"fmt"
	"strings"
)

// 测试minline参数逻辑
func testMinlineLogic() {
	testCases := []struct {
		minline      string
		shouldUpdate bool
		description  string
	}{
		{"5", true, "单独5分钟"},
		{"1", false, "单独1分钟"},
		{"1,5", true, "1分钟和5分钟"},
		{"5,1", true, "5分钟和1分钟"},
		{"", false, "空参数"},
		{"15", true, "包含1和5的组合"},
	}

	fmt.Println("测试minline参数逻辑:")
	for _, tc := range testCases {
		result := tc.minline != "" && strings.Contains(tc.minline, "5")
		status := "✅"
		if result != tc.shouldUpdate {
			status = "❌"
		}
		fmt.Printf("%s %s: minline='%s', 预期=%v, 实际=%v\n",
			status, tc.description, tc.minline, tc.shouldUpdate, result)
	}
}

// 测试视图名称
func testViewNames() {
	qfqViewName := "v_qfq_stocks"
	hfqViewName := "v_hfq_stocks"
	qfq5MinViewName := "v_qfq_stocks_5min"
	hfq5MinViewName := "v_hfq_stocks_5min"

	fmt.Println("\n测试视图名称:")
	fmt.Printf("前复权日线视图: %s\n", qfqViewName)
	fmt.Printf("后复权日线视图: %s\n", hfqViewName)
	fmt.Printf("前复权5分钟视图: %s\n", qfq5MinViewName)
	fmt.Printf("后复权5分钟视图: %s\n", hfq5MinViewName)
}

func main() {
	testMinlineLogic()
	testViewNames()
	fmt.Println("\n✅ 逻辑测试完成")
}