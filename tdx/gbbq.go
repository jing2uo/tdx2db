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
	"github.com/parquet-go/parquet-go"
)

// ConvertGbbqFileToParquet 转换 GBBQ 文件为 Parquet
func ConvertGbbqFileToParquet(gbbqFile, parquetPath string) (string, error) {
	// 1. 解析数据
	data, err := processGbbqFileOptimized(gbbqFile)
	if err != nil {
		return "", fmt.Errorf("failed to process GBBQ file: %w", err)
	}
	if len(data) == 0 {
		return "", nil
	}

	// 2. 创建 Parquet 文件
	f, err := os.Create(parquetPath)
	if err != nil {
		return "", fmt.Errorf("failed to create parquet file: %w", err)
	}
	defer f.Close()

	// 3. 配置 Writer
	writerConfig := []parquet.WriterOption{
		parquet.Compression(&parquet.Snappy),     // 均衡的压缩算法
		parquet.WriteBufferSize(4 * 1024 * 1024), // 4MB Buffer 足够 GBBQ 这种小文件
	}

	// 4. 写入数据
	pw := parquet.NewGenericWriter[model.GbbqData](f, writerConfig...)
	defer pw.Close()

	if _, err := pw.Write(data); err != nil {
		return "", fmt.Errorf("failed to write parquet rows: %w", err)
	}

	return parquetPath, nil
}

// processGbbqFileOptimized 优化的解析逻辑
func processGbbqFileOptimized(gbbqFile string) ([]model.GbbqData, error) {
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
		return nil, nil // 空文件或格式错误
	}

	// 读取记录数量
	count := int(binary.LittleEndian.Uint32(content[0:4]))
	result := make([]model.GbbqData, 0, count)

	pos := 4
	// 预分配一个固定大小的 buffer 用于存放解密后的单条记录 (29 bytes)
	// 结构: [8 byte decrypted] + [8 byte decrypted] + [8 byte decrypted] + [5 byte raw]
	var clearData [29]byte

	// 预计算 keys 的部分偏移量，虽然 Go 编译器也会优化，但这里保持纯净
	// 解密逻辑比较复杂，保持原样，但优化内存分配

	totalLen := len(content)

	for i := 0; i < count; i++ {
		// 检查边界：3个加密块(8*3) + 1个原始块(5) = 29 bytes
		if pos+29 > totalLen {
			break // 或者报错
		}

		// --- 解密阶段 (Zero Allocation) ---
		decryptBlockToBuf(keys, content[pos:pos+8], clearData[0:8])
		pos += 8
		decryptBlockToBuf(keys, content[pos:pos+8], clearData[8:16])
		pos += 8
		decryptBlockToBuf(keys, content[pos:pos+8], clearData[16:24])
		pos += 8
		copy(clearData[24:29], content[pos:pos+5])
		pos += 5

		// --- 解析阶段 ---

		// 1. Category (Byte 12 in slice -> index 12)
		category := int(clearData[12])

		// 2. Code (Bytes 1-7 -> index 1:8)
		// trimming null bytes
		codeBytes := clearData[1:8]
		// 查找第一个 \x00 的位置，手动 slice 避免 bytes.TrimRight 的开销 (微优化)
		strLen := 0
		for k := 0; k < len(codeBytes); k++ {
			if codeBytes[k] == 0 {
				break
			}
			strLen++
		}
		code := string(codeBytes[:strLen])

		// 3. Date (Bytes 8-11 -> index 8:12)
		// 优化：使用数学运算替代 time.Parse("20060102")
		dateInt := binary.LittleEndian.Uint32(clearData[8:12]) // e.g. 20230501
		dateTime, err := fastParseDate(dateInt)
		if err != nil {
			// 记录日志或忽略
			continue
		}

		// 4. Floats (4个 float32)
		// 直接转换 bits，避免中间内存分配
		c1 := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[13:17])))
		c2 := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[17:21])))
		c3 := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[21:25])))
		c4 := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[25:29])))

		result = append(result, model.GbbqData{
			Category: category,
			Code:     code,
			Date:     dateTime,
			C1:       c1,
			C2:       c2,
			C3:       c3,
			C4:       c4,
		})
	}

	return result, nil
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
