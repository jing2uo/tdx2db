package tdx

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

const gbbqURL = "http://www.tdx.com.cn/products/data/data/dbf/gbbq.zip"

func GetLatestGbbqCsv(cacheDir, csvPath string) (string, error) {
	gbbqFile, err := getLatestGbbqFile(cacheDir)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve GBBQ file: %w", err)
	}

	data, err := processGbbqFile(gbbqFile)
	if err != nil {
		return "", fmt.Errorf("failed to process GBBQ file: %w", err)
	}

	file, err := os.Create(csvPath)
	if err != nil {
		return "", fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"category", "date", "code", "fenhong", "peigujia", "songzhuangu", "peigu"}); err != nil {
		return "", fmt.Errorf("failed to write CSV header: %w", err)
	}

	for _, stock := range data {
		row := []string{
			fmt.Sprintf("%d", stock.Category),
			stock.Date.Format("2006-01-02"),
			stock.Code,
			fmt.Sprintf("%f", stock.Fenhong),
			fmt.Sprintf("%f", stock.Peigujia),
			fmt.Sprintf("%f", stock.Songzhuangu),
			fmt.Sprintf("%f", stock.Peigu),
		}
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write CSV row for stock %s: %w", stock.Code, err)
		}
	}

	return csvPath, nil
}

// getGbbqFile downloads and unzips the GBBQ file, returning its path.
func getLatestGbbqFile(cacheDir string) (string, error) {
	zipPath := filepath.Join(cacheDir, "gbbq.zip")
	if err := utils.DownloadFile(gbbqURL, zipPath); err != nil {
		return "", fmt.Errorf("failed to download GBBQ zip file: %w", err)
	}

	unzipPath := filepath.Join(cacheDir, "gbbq-temp")
	if err := utils.UnzipFile(zipPath, unzipPath); err != nil {
		return "", fmt.Errorf("failed to unzip GBBQ file: %w", err)
	}

	return filepath.Join(unzipPath, "gbbq"), nil
}

func processGbbqFile(gbbqFile string) ([]model.GbbqData, error) {
	hexStr := strings.ReplaceAll(HexKeys, " ", "")
	keys, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex keys: %w", err)
	}

	content, err := os.ReadFile(gbbqFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read GBBQ file: %w", err)
	}

	count := binary.LittleEndian.Uint32(content[0:4])
	pos := 4
	var result []model.GbbqData

	for i := 0; i < int(count); i++ {
		clearData := make([]byte, 0, 29)
		for j := 0; j < 3; j++ {
			if pos+8 > len(content) {
				return nil, fmt.Errorf("invalid data length at position %d", pos)
			}
			encrypted := content[pos : pos+8]
			decrypted := decryptBlock(keys, encrypted)
			clearData = append(clearData, decrypted...)
			pos += 8
		}

		if pos+5 > len(content) {
			return nil, fmt.Errorf("invalid data length at position %d", pos)
		}
		clearData = append(clearData, content[pos:pos+5]...)
		pos += 5

		if len(clearData) < 29 {
			return nil, fmt.Errorf("incomplete data block at record %d", i)
		}

		category := clearData[12]

		codeBytes := clearData[1:8]
		code := string(bytes.TrimRight(codeBytes, "\x00"))
		date := binary.LittleEndian.Uint32(clearData[8:12])
		dateStr := fmt.Sprintf("%08d", date)
		dateTime, err := time.Parse("20060102", dateStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse date for record %d: %w", i, err)
		}

		fenhong := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[13:17])))
		peigujia := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[17:21])))
		songzhuangu := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[21:25])))
		peigu := float64(math.Float32frombits(binary.LittleEndian.Uint32(clearData[25:29])))

		g := model.GbbqData{
			Category:    int(category),
			Code:        code,
			Date:        dateTime,
			Fenhong:     fenhong,
			Peigujia:    peigujia,
			Songzhuangu: songzhuangu,
			Peigu:       peigu,
		}
		result = append(result, g)
	}

	return result, nil
}

func decryptBlock(keys, encrypted []byte) []byte {
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
	decrypted := make([]byte, 8)
	binary.LittleEndian.PutUint32(decrypted[0:4], numold)
	binary.LittleEndian.PutUint32(decrypted[4:8], num)
	return decrypted
}
