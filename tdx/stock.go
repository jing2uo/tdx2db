package tdx

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/jing2uo/tdx2db/model"
)

type stockListItem struct {
	Market uint16
	Code   string
	Name   string
}

func FetchOnlineSymbolNames(ctx context.Context) ([]model.SymbolName, error) {
	client := NewOnlineClient()
	if err := client.ConnectStandard(); err != nil {
		return nil, err
	}
	defer client.Close()
	if err := client.LoginStandard(); err != nil {
		return nil, err
	}

	markets := []uint16{0, 1, 2} // SZ, SH, BJ
	var all []stockListItem

	for _, m := range markets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		items, err := client.getStockList(m)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch stock list market=%d: %w", m, err)
		}
		all = append(all, items...)
	}

	names := make([]model.SymbolName, 0, len(all))
	for _, s := range all {
		symbol, ok := marketCodeToSymbol(s.Market, s.Code)
		if !ok {
			continue
		}
		class := model.ClassifyCode(symbol)
		if !isOnlineSymbolNameClass(class) {
			continue
		}
		names = append(names, model.SymbolName{
			Symbol: symbol,
			Name:   s.Name,
			Class:  class,
		})
	}
	sort.Slice(names, func(i, j int) bool { return names[i].Symbol < names[j].Symbol })

	return names, nil
}

func isOnlineSymbolNameClass(class string) bool {
	switch class {
	case model.ClassStock, model.ClassBStock, model.ClassETF, model.ClassIndex:
		return true
	}
	return false
}

func (c *OnlineClient) LoginStandard() error {
	_, err := c.CallWithHead(0x0c, 0x0d, []byte{1})
	if err != nil {
		return fmt.Errorf("failed to login TDX standard server: %w", err)
	}
	return nil
}

func (c *OnlineClient) getStockList(market uint16) ([]stockListItem, error) {
	const pageSize = 1600
	var result []stockListItem
	for start := 0; ; start += pageSize {
		items, err := c.getStockListPage(market, uint32(start), uint32(pageSize))
		if err != nil {
			return nil, err
		}
		result = append(result, items...)
		if len(items) < pageSize {
			break
		}
	}
	return result, nil
}

func (c *OnlineClient) getStockListPage(market uint16, start, count uint32) ([]stockListItem, error) {
	// 0x44d: stock list, body = market(u16) + start(u32) + count(u32) + padding(u32)
	body := make([]byte, 14)
	binary.LittleEndian.PutUint16(body[0:2], market)
	binary.LittleEndian.PutUint32(body[2:6], start)
	binary.LittleEndian.PutUint32(body[6:10], count)

	data, err := c.CallWithHead(0x0c, 0x44d, body)
	if err != nil {
		return nil, err
	}
	if len(data) < 2 {
		return nil, fmt.Errorf("stock list response too short: %d", len(data))
	}
	rowCount := int(binary.LittleEndian.Uint16(data[0:2]))
	items := make([]stockListItem, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		off := 2 + i*37
		if off+37 > len(data) {
			break
		}
		code := gbkClean(data[off : off+6])
		// off+6: vol (2 bytes, skip)
		name := gbkClean(data[off+8 : off+24])
		// off+24: pre_close (4 bytes float, skip)
		market := market
		if code == "" {
			continue
		}
		items = append(items, stockListItem{
			Market: market,
			Code:   code,
			Name:   name,
		})
	}
	return items, nil
}
