package workflow

import (
	"fmt"
	"time"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
)

// WorkPlan 在任务图启动前汇总交易日历与各表最新日期，决定哪些任务真正需要跑。
// 任务框架里的 SkipIf 只读取本结构，不再各自重复查询。
type WorkPlan struct {
	Today          time.Time
	LastTradingDay time.Time
	Calendar       *TradingCalendar

	NeedDaily    bool
	NeedGbbq     bool
	NeedBasic    bool
	NeedFactor   bool
	NeedHolidays bool

	Reason string // 用于日志
}

// AnyNeeded 是否有任何任务需要执行。
func (p *WorkPlan) AnyNeeded() bool {
	return p.NeedDaily || p.NeedGbbq || p.NeedBasic || p.NeedFactor || p.NeedHolidays
}

// BuildWorkPlan 读取交易日历与各表最新日期，推导本次 cron 要做什么。
func BuildWorkPlan(db database.DataRepository, today time.Time) (*WorkPlan, error) {
	holidays, err := db.GetHolidays()
	if err != nil {
		return nil, fmt.Errorf("failed to load holidays: %w", err)
	}

	plan := &WorkPlan{Today: today}

	// raw_holidays 为空：属于首次运行 / 旧库，放行所有任务让其自行写入节假日。
	if len(holidays) == 0 {
		plan.NeedDaily = true
		plan.NeedGbbq = true
		plan.NeedBasic = true
		plan.NeedFactor = true
		plan.NeedHolidays = true
		plan.Reason = "🌱 raw_holidays 为空，走完整流程"
		return plan, nil
	}

	cal := NewTradingCalendar(holidays)
	plan.Calendar = cal
	plan.LastTradingDay = cal.LastTradingDayOnOrBefore(today)

	dailyLatest, err := db.GetLatestDate(model.TableKlineDaily.TableName, "date")
	if err != nil {
		return nil, fmt.Errorf("failed to get latest daily date: %w", err)
	}
	basicLatest, err := db.GetLatestDate(model.TableBasicDaily.TableName, "date")
	if err != nil {
		return nil, fmt.Errorf("failed to get latest basic date: %w", err)
	}
	factorLatest, err := db.GetLatestDate(model.TableAdjustFactor.TableName, "date")
	if err != nil {
		return nil, fmt.Errorf("failed to get latest factor date: %w", err)
	}

	// 空库：交给 init 流程；此处不标任何 Need，调用方自行决定。
	if dailyLatest.IsZero() {
		plan.Reason = "🛑 数据库无日线数据，请先运行 init"
		return plan, nil
	}

	plan.NeedDaily = dailyLatest.Before(plan.LastTradingDay)
	// gbbq 与日线同频：日线没新数据时 gbbq 也无须更新。
	plan.NeedGbbq = plan.NeedDaily
	// basic/factor 要追赶 daily；如果 daily 将更新，那之后也必须重算。
	plan.NeedBasic = plan.NeedDaily || basicLatest.Before(dailyLatest)
	plan.NeedFactor = plan.NeedDaily || factorLatest.Before(basicLatest)
	// holidays 来自 gbbq.zip，与 gbbq 同频刷新即可。
	plan.NeedHolidays = plan.NeedGbbq

	plan.Reason = describePlan(plan, dailyLatest)
	return plan, nil
}

func describePlan(plan *WorkPlan, dailyLatest time.Time) string {
	dayStr := plan.Today.Format("2006-01-02")
	cal := plan.Calendar

	if plan.NeedDaily {
		return fmt.Sprintf("📅 数据库日线最新 %s，落后于最近交易日 %s，执行更新",
			dailyLatest.Format("2006-01-02"), plan.LastTradingDay.Format("2006-01-02"))
	}

	if plan.NeedBasic || plan.NeedFactor {
		return fmt.Sprintf("📅 日线已是最新 (%s)，补算 basic/factor", dailyLatest.Format("2006-01-02"))
	}

	switch {
	case cal.IsHoliday(plan.Today):
		return fmt.Sprintf("🎉 今日 %s 为节假日，A 股休市，全部任务跳过", dayStr)
	case cal.IsWeekend(plan.Today):
		return fmt.Sprintf("🌴 今日 %s 为周末，A 股休市，全部任务跳过", dayStr)
	default:
		return fmt.Sprintf("✅ 数据库已是最新 (%s)，全部任务跳过", dailyLatest.Format("2006-01-02"))
	}
}
