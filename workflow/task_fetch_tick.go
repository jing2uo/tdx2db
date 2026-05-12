package workflow

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
)

// ExtraTickValidDates 是 fetch_tick 写入 args.Extra 的 key，
// 值类型为 []time.Time，列出本轮成功下载到的分时日期。
const ExtraTickValidDates = "tick_valid_dates"

var TaskFetchTick *Task

func init() {
	TaskFetchTick = &Task{
		Name:      "fetch_tick",
		DependsOn: []string{},
		SkipIf: func(ctx context.Context, db database.DataRepository, args *TaskArgs) bool {
			return !args.Min
		},
		Executor: executeFetchTick,
	}
	registerTask(TaskFetchTick, "update")
}

func executeFetchTick(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	latest, err := db.GetLatestDate(model.TableKline1Min.TableName, "datetime")
	if err != nil {
		return nil, fmt.Errorf("query 1min latest: %w", err)
	}
	if latest.IsZero() {
		fmt.Println("🛑 数据库中没有分时数据，历史请自行导入")
		latest = args.Today.AddDate(0, 0, -1)
	} else {
		fmt.Printf("📅 分时数据最新日期为 %s\n", latest.Format("2006-01-02"))
	}

	src := pullSource{
		targetDir:   filepath.Join(args.VipdocDir, "newdatetick"),
		urlTemplate: "https://www.tdx.com.cn/products/data/data/g4tic/%s.zip",
		fileSuffix:  "tic",
		label:       "分时",
	}
	validDates, err := pullDateRange(ctx, latest, src, args)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tick data: %w", err)
	}

	if len(validDates) >= 30 {
		return nil, fmt.Errorf("分时数据超过30天未更新，请手动补齐后继续")
	}

	if args.Extra == nil {
		args.Extra = map[string]interface{}{}
	}
	args.Extra[ExtraTickValidDates] = validDates

	if len(validDates) == 0 {
		return &TaskResult{State: StateSkipped, Message: "no new tick data"}, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	endDate := validDates[len(validDates)-1]
	fmt.Printf("🐌 开始转档分笔数据\n")
	if err := tdx.DatatoolCreate(args.TempDir, "tick", endDate); err != nil {
		return nil, fmt.Errorf("failed to run DatatoolTickCreate: %w", err)
	}

	return &TaskResult{State: StateCompleted, Rows: len(validDates), Message: "tick fetched"}, nil
}
