package tdx

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/jing2uo/tdx2db/model"
	"golang.org/x/text/encoding/simplifiedchinese"
)

type BlockType uint16

const (
	BlockTypeHY       BlockType = 0
	BlockTypeHY2      BlockType = 1
	BlockTypeGN       BlockType = 3
	BlockTypeFG       BlockType = 4
	BlockTypeDQ       BlockType = 5
	BlockTypeYJLevel1 BlockType = 7
	BlockTypeYJLevel2 BlockType = 8
	BlockTypeYJLevel3 BlockType = 9
)

type BlockListItem struct {
	Market uint16
	Code   string
	Name   string
	Query  BlockType
}

type BlockMemberItem struct {
	Market uint16
	Code   string
	Name   string
}

func FetchOnlineBlocks(ctx context.Context) ([]model.BlockInfo, []model.BlockMember, error) {
	client := NewOnlineClient()
	if err := client.Connect(); err != nil {
		return nil, nil, err
	}
	defer client.Close()

	queryTypes := []BlockType{BlockTypeGN, BlockTypeFG, BlockTypeDQ, BlockTypeYJLevel1, BlockTypeYJLevel2, BlockTypeYJLevel3}
	infoByCode := make(map[string]model.BlockInfo)
	memberSet := make(map[string]model.BlockMember)

	for _, qt := range queryTypes {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		blocks, err := client.GetBlockList(qt, 2000)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch block list %s: %w", qt.blockType(), err)
		}
		for _, blockItem := range blocks {
			block := blockItem.toBlockInfo()
			if _, exists := infoByCode[block.BlockCode]; !exists {
				infoByCode[block.BlockCode] = block
			}

			members, err := client.GetBlockMembers(blockItem.Code, 5000)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to fetch members for %s %s: %w", blockItem.Code, blockItem.Name, err)
			}
			for _, m := range members {
				symbol, ok := marketCodeToSymbol(m.Market, m.Code)
				if !ok {
					continue
				}
				member := model.BlockMember{StockSymbol: symbol, BlockCode: block.BlockCode}
				memberSet[member.StockSymbol+"\x00"+member.BlockCode] = member
			}
		}
	}

	infos := make([]model.BlockInfo, 0, len(infoByCode))
	for _, v := range infoByCode {
		infos = append(infos, v)
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].BlockType == infos[j].BlockType {
			return infos[i].BlockSymbol < infos[j].BlockSymbol
		}
		return infos[i].BlockType < infos[j].BlockType
	})

	members := make([]model.BlockMember, 0, len(memberSet))
	for _, v := range memberSet {
		members = append(members, v)
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].BlockCode == members[j].BlockCode {
			return members[i].StockSymbol < members[j].StockSymbol
		}
		return members[i].BlockCode < members[j].BlockCode
	})

	return infos, members, nil
}

func (c *OnlineClient) GetBlockList(blockType BlockType, count int) ([]BlockListItem, error) {
	const pageSize = 150
	var result []BlockListItem
	for start := 0; start < count; start += pageSize {
		current := pageSize
		if count-start < current {
			current = count - start
		}
		items, err := c.getBlockListPage(blockType, uint16(start), uint16(current))
		if err != nil {
			return nil, err
		}
		result = append(items, result...)
		if len(items) < current {
			break
		}
	}
	return result, nil
}

func (c *OnlineClient) getBlockListPage(blockType BlockType, start, pageSize uint16) ([]BlockListItem, error) {
	body := make([]byte, 18)
	binary.LittleEndian.PutUint16(body[0:2], pageSize)
	binary.LittleEndian.PutUint16(body[2:4], uint16(blockType))
	body[4] = 0
	body[5] = 1
	binary.LittleEndian.PutUint16(body[6:8], start)
	binary.LittleEndian.PutUint16(body[8:10], 1)

	data, err := c.Call(0x1231, body)
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("block list response too short: %d", len(data))
	}
	countAll := int(binary.LittleEndian.Uint16(data[0:2]))
	rowCount := countAll / 2
	items := make([]BlockListItem, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		off := 4 + i*160
		if off+160 > len(data) {
			break
		}
		market := binary.LittleEndian.Uint16(data[off : off+2])
		items = append(items, BlockListItem{
			Market: market,
			Code:   gbkClean(data[off+2 : off+8]),
			Name:   gbkClean(data[off+24 : off+68]),
			Query:  blockType,
		})
	}
	return items, nil
}

func (c *OnlineClient) GetBlockMembers(blockCode string, count int) ([]BlockMemberItem, error) {
	const pageSize = 80
	var result []BlockMemberItem
	for start := 0; start < count; start += pageSize {
		current := pageSize
		if count-start < current {
			current = count - start
		}
		items, err := c.getBlockMembersPage(blockCode, uint16(start), uint8(current))
		if err != nil {
			return nil, err
		}
		result = append(items, result...)
		if len(items) < current {
			break
		}
	}
	return result, nil
}

func (c *OnlineClient) getBlockMembersPage(blockCode string, start uint16, pageSize uint8) ([]BlockMemberItem, error) {
	body := make([]byte, 43)
	binary.LittleEndian.PutUint32(body[0:4], uint32(exchangeBlockCode(blockCode)))
	binary.LittleEndian.PutUint16(body[13:15], 0) // sort by code
	binary.LittleEndian.PutUint32(body[15:19], uint32(start))
	body[19] = pageSize
	body[20] = 0
	body[21] = 0 // no sort order
	body[22] = 0
	// body[23:43] bitmap remains all zero: request market/code/name only.

	data, err := c.Call(0x122c, body)
	if err != nil {
		return nil, err
	}
	if len(data) < 26 {
		return nil, fmt.Errorf("block member response too short: %d", len(data))
	}
	rowCount := int(binary.LittleEndian.Uint16(data[24:26]))
	items := make([]BlockMemberItem, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		off := 26 + i*68
		if off+68 > len(data) {
			break
		}
		items = append(items, BlockMemberItem{
			Market: binary.LittleEndian.Uint16(data[off : off+2]),
			Code:   gbkClean(data[off+2 : off+24]),
			Name:   gbkClean(data[off+24 : off+68]),
		})
	}
	return items, nil
}

func (b BlockListItem) toBlockInfo() model.BlockInfo {
	blockType, level := b.Query.blockType(), b.Query.level()
	blockSymbol, _ := marketCodeToSymbol(b.Market, b.Code)
	if blockSymbol == "" {
		blockSymbol = "sh" + b.Code
	}
	return model.BlockInfo{
		BlockType:   blockType,
		BlockName:   b.Name,
		BlockSymbol: blockSymbol,
		BlockCode:   blockSymbol,
		ParentCode:  "",
		BlockLevel:  level,
	}
}

func (b BlockType) blockType() string {
	switch b {
	case BlockTypeGN:
		return "concept"
	case BlockTypeFG:
		return "style"
	case BlockTypeDQ:
		return "region"
	case BlockTypeYJLevel1, BlockTypeYJLevel2, BlockTypeYJLevel3:
		return "tdx_research"
	case BlockTypeHY, BlockTypeHY2:
		return "tdx_research"
	default:
		return "unknown"
	}
}

func (b BlockType) level() int {
	switch b {
	case BlockTypeYJLevel1:
		return 1
	case BlockTypeYJLevel2:
		return 2
	case BlockTypeYJLevel3:
		return 3
	default:
		return 1
	}
}

func marketCodeToSymbol(market uint16, code string) (string, bool) {
	switch market {
	case 0:
		return "sz" + code, true
	case 1:
		return "sh" + code, true
	case 2:
		return "bj" + code, true
	default:
		return "", false
	}
}

func exchangeBlockCode(blockSymbol string) int {
	s := strings.TrimSpace(blockSymbol)
	switch {
	case strings.HasPrefix(s, "US"):
		return 30000 + atoiSafe(strings.TrimPrefix(s, "US"))
	case strings.HasPrefix(s, "HK"):
		return 20000 + atoiSafe(strings.TrimPrefix(s, "HK"))
	case strings.HasPrefix(s, "sh") || strings.HasPrefix(s, "sz") || strings.HasPrefix(s, "bj"):
		return exchangeBlockCode(s[2:])
	case strings.HasPrefix(s, "000"):
		return 31000 + atoiSafe(s)
	case strings.HasPrefix(s, "399") && len(s) == 6:
		return atoiSafe(s) - 399000 + 30000
	case strings.HasPrefix(s, "899") && len(s) == 6:
		return atoiSafe(s) - 899000 + 32000
	case strings.HasPrefix(s, "88") && len(s) == 6:
		return atoiSafe(s) - 880000 + 20000
	default:
		return atoiSafe(s)
	}
}

func atoiSafe(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}

func gbkClean(raw []byte) string {
	if i := strings.IndexByte(string(raw), 0); i >= 0 {
		raw = raw[:i]
	}
	out, err := simplifiedchinese.GBK.NewDecoder().Bytes(raw)
	if err != nil {
		return strings.TrimSpace(string(raw))
	}
	return strings.TrimSpace(string(out))
}
