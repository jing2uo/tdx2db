package workflow

import "time"

type TradingCalendar struct {
	holidays map[string]struct{}
}

func NewTradingCalendar(holidays []time.Time) *TradingCalendar {
	set := make(map[string]struct{}, len(holidays))
	for _, d := range holidays {
		set[d.Format("2006-01-02")] = struct{}{}
	}
	return &TradingCalendar{holidays: set}
}

func (c *TradingCalendar) IsHoliday(d time.Time) bool {
	_, ok := c.holidays[d.Format("2006-01-02")]
	return ok
}

func (c *TradingCalendar) IsWeekend(d time.Time) bool {
	w := d.Weekday()
	return w == time.Saturday || w == time.Sunday
}

func (c *TradingCalendar) IsTradingDay(d time.Time) bool {
	return !c.IsWeekend(d) && !c.IsHoliday(d)
}

// LastTradingDayOnOrBefore 返回 ≤ d 的最近交易日。
func (c *TradingCalendar) LastTradingDayOnOrBefore(d time.Time) time.Time {
	cur := d
	for i := 0; i < 30; i++ {
		if c.IsTradingDay(cur) {
			return cur
		}
		cur = cur.AddDate(0, 0, -1)
	}
	return cur
}
