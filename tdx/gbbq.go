package tdx

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
)

func DecodeGbbqFile(gbbqFile string) ([]model.GbbqData, error) {
	hexStr := strings.ReplaceAll(HexKeys, " ", "")
	keys, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex keys: %w", err)
	}

	content, err := os.ReadFile(gbbqFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read GBBQ file: %w", err)
	}

	if len(content) < 4 {
		return nil, nil
	}

	// 读取记录数量
	count := int(binary.LittleEndian.Uint32(content[0:4]))

	gbbqResult := make([]model.GbbqData, 0, count)

	pos := 4
	var clearData [29]byte
	totalLen := len(content)

	for i := 0; i < count; i++ {
		// 检查边界
		if pos+29 > totalLen {
			break
		}

		// --- 解密阶段 ---
		decryptBlockToBuf(keys, content[pos:pos+8], clearData[0:8])
		pos += 8
		decryptBlockToBuf(keys, content[pos:pos+8], clearData[8:16])
		pos += 8
		decryptBlockToBuf(keys, content[pos:pos+8], clearData[16:24])
		pos += 8
		copy(clearData[24:29], content[pos:pos+5])
		pos += 5

		// --- 解析阶段 ---

		// 1. Code 处理
		codeBytes := clearData[1:8]
		strLen := 0
		for k := 0; k < len(codeBytes); k++ {
			if codeBytes[k] == 0 {
				break
			}
			strLen++
		}
		code := string(codeBytes[:strLen])

		// 生成 Symbol 并过滤
		symbol, ok := generateSymbol(code)
		if !ok {
			continue // 没匹配到规则，跳过
		}

		// 2. Date
		dateInt := binary.LittleEndian.Uint32(clearData[8:12])
		dateTime, err := fastParseDate(dateInt) // 假设此函数已定义
		if err != nil {
			continue
		}

		// 3. Floats
		c1 := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[13:17])))
		c2 := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[17:21])))
		c3 := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[21:25])))
		c4 := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[25:29])))

		// 4. Category 分类处理
		category := int(clearData[12])

		gbbqResult = append(gbbqResult, model.GbbqData{
			Category: category,
			Symbol:   symbol,
			Date:     dateTime,
			C1:       c1,
			C2:       c2,
			C3:       c3,
			C4:       c4,
		})
	}

	return gbbqResult, nil
}

// fastParseDate 将 YYYYMMDD 整数转为 time.Time，比字符串解析快 100 倍
func fastParseDate(date uint32) (time.Time, error) {
	if date == 0 {
		return time.Time{}, fmt.Errorf("zero date")
	}
	y := int(date / 10000)
	m := int((date % 10000) / 100)
	d := int(date % 100)
	if m < 1 || m > 12 || d < 1 || d > 31 {
		return time.Time{}, fmt.Errorf("invalid date: %d", date)
	}
	return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.Local), nil
}

func generateSymbol(code string) (string, bool) {
	switch {
	case strings.HasPrefix(code, "00") || strings.HasPrefix(code, "30"):
		return "sz" + code, true
	case strings.HasPrefix(code, "60") || strings.HasPrefix(code, "68"):
		return "sh" + code, true
	case strings.HasPrefix(code, "92") || strings.HasPrefix(code, "87") ||
		strings.HasPrefix(code, "83") || strings.HasPrefix(code, "43"):
		return "bj" + code, true
	default:
		return "", false
	}
}

// decryptBlockToBuf 优化后的解密函数
func decryptBlockToBuf(keys, encrypted, dst []byte) {
	// 安全检查，避免 panic
	if len(encrypted) < 8 || len(dst) < 8 {
		return
	}

	eax := binary.LittleEndian.Uint32(keys[0x44:0x48])
	A := binary.LittleEndian.Uint32(encrypted[0:4])
	B := binary.LittleEndian.Uint32(encrypted[4:8])

	num := eax ^ A
	numold := B

	for j := 0x40; j >= 4; j -= 4 {
		ebx := (num & 0xFF0000) >> 16
		offset := int(ebx)*4 + 0x448
		eax = binary.LittleEndian.Uint32(keys[offset : offset+4])

		ebx = num >> 24
		offset = int(ebx)*4 + 0x48
		eax_add := binary.LittleEndian.Uint32(keys[offset : offset+4])
		eax += eax_add

		ebx = (num & 0xFF00) >> 8
		offset = int(ebx)*4 + 0x848
		eax_xor := binary.LittleEndian.Uint32(keys[offset : offset+4])
		eax ^= eax_xor

		ebx = num & 0xFF
		offset = int(ebx)*4 + 0xC48
		eax_add = binary.LittleEndian.Uint32(keys[offset : offset+4])
		eax += eax_add

		eax_xor = binary.LittleEndian.Uint32(keys[j : j+4])
		eax ^= eax_xor

		temp := num
		num = numold ^ eax
		numold = temp
	}

	numold_op := binary.LittleEndian.Uint32(keys[0:4])
	numold ^= numold_op

	binary.LittleEndian.PutUint32(dst[0:4], numold)
	binary.LittleEndian.PutUint32(dst[4:8], num)
}
