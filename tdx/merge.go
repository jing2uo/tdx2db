package tdx

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	codRecordSize = 150
	md1BlockSize  = 512
)

// codEntry represents a single record parsed from a TDX .cod file.
// The .cod file maps stock codes to sequence numbers used to locate
// corresponding data blocks in the paired .md1 file.
type codEntry struct {
	StockCode string // [0:6] ASCII stock code (e.g. "600519")
	SeqNum    uint16 // [32:34] sequence number → md1 offset = SeqNum * 512
}

// md1OHLCV holds OHLCV data extracted from one 512-byte block in a .md1 file.
type md1OHLCV struct {
	Open   float64 // [12:20] double
	High   float64 // [20:28] double
	Low    float64 // [28:36] double
	Close  float64 // [36:44] double
	Amount float64 // [72:80] double (turnover in yuan)
	Volume uint32  // [56:60] uint32 (in shares)
}

// NativeDayMerge reads all .md1/.cod file pairs from vipdocDir/refmhq/ and
// merges the parsed records into per-stock .day files under vipdocDir/{exchange}/lday/.
//
// TDX distributes daily incremental data as .md1 (market data, 512 bytes/block)
// and .cod (code mapping, 150 bytes/record) file pairs. Each .cod record maps
// a stock code to a sequence number that locates its data block in the .md1 file
// at offset = seqNum * 512.
//
// This function replaces the external datatool binary for the "day" subcommand,
// enabling native cross-platform support (including macOS arm64).
func NativeDayMerge(vipdocDir string) error {
	refmhqDir := filepath.Join(vipdocDir, "refmhq")

	md1Files, err := filepath.Glob(filepath.Join(refmhqDir, "*.md1"))
	if err != nil {
		return fmt.Errorf("failed to glob md1 files: %w", err)
	}

	if len(md1Files) == 0 {
		return nil
	}

	sort.Strings(md1Files)

	for _, md1File := range md1Files {
		baseName := filepath.Base(md1File)
		codFile := filepath.Join(refmhqDir, strings.TrimSuffix(baseName, ".md1")+".cod")

		if _, err := os.Stat(codFile); os.IsNotExist(err) {
			continue
		}

		exchange, dateVal, err := parseIncrFilename(baseName)
		if err != nil {
			continue
		}

		if err := mergeSingleDay(vipdocDir, exchange, dateVal, codFile, md1File); err != nil {
			return fmt.Errorf("merge %s failed: %w", baseName, err)
		}
	}

	return nil
}

// parseIncrFilename extracts exchange prefix and date from filenames like "sh260318.md1".
// Returns exchange ("sh"|"sz"|"bj") and date as YYYYMMDD uint32 (e.g. 20260318).
func parseIncrFilename(name string) (exchange string, dateVal uint32, err error) {
	base := strings.SplitN(name, ".", 2)[0]
	if len(base) < 8 {
		return "", 0, fmt.Errorf("filename too short: %s", name)
	}

	exchange = base[:2]
	dateStr := base[2:]
	if len(dateStr) != 6 {
		return "", 0, fmt.Errorf("invalid date part in filename: %s", dateStr)
	}

	yy := int(dateStr[0]-'0')*10 + int(dateStr[1]-'0')
	mm := int(dateStr[2]-'0')*10 + int(dateStr[3]-'0')
	dd := int(dateStr[4]-'0')*10 + int(dateStr[5]-'0')

	year := 2000 + yy
	if year > 2080 {
		year -= 100
	}

	if mm < 1 || mm > 12 || dd < 1 || dd > 31 {
		return "", 0, fmt.Errorf("invalid date: %d-%02d-%02d", year, mm, dd)
	}

	dateVal = uint32(year*10000 + mm*100 + dd)
	return exchange, dateVal, nil
}

// parseCodEntries reads a .cod file and returns all stock code → sequence number mappings.
//
// .cod file format (150 bytes per record):
//
//	[0:6]   char[6]  stock code (ASCII, e.g. "600519")
//	[32:34] uint16   sequence number (LE) → md1 block offset = seq * 512
func parseCodEntries(codFile string) ([]codEntry, error) {
	data, err := os.ReadFile(codFile)
	if err != nil {
		return nil, fmt.Errorf("read cod file: %w", err)
	}

	if len(data)%codRecordSize != 0 {
		return nil, fmt.Errorf("invalid cod file size %d (not divisible by %d)", len(data), codRecordSize)
	}

	count := len(data) / codRecordSize
	entries := make([]codEntry, 0, count)

	for i := 0; i < count; i++ {
		offset := i * codRecordSize
		rec := data[offset : offset+codRecordSize]

		code := strings.TrimRight(string(rec[0:6]), "\x00 ")
		if code == "" {
			continue
		}

		seqNum := binary.LittleEndian.Uint16(rec[32:34])

		entries = append(entries, codEntry{
			StockCode: code,
			SeqNum:    seqNum,
		})
	}

	return entries, nil
}

// readMd1Block extracts OHLCV data from the 512-byte block at the given sequence index.
//
// .md1 file format (512 bytes per block):
//
//	[12:20] float64  open price
//	[20:28] float64  high price
//	[28:36] float64  low price
//	[36:44] float64  close price
//	[56:60] uint32   volume (in shares)
//	[72:80] float64  amount (turnover in yuan)
func readMd1Block(md1Data []byte, seqNum uint16) (md1OHLCV, error) {
	offset := int(seqNum) * md1BlockSize
	if offset+md1BlockSize > len(md1Data) {
		return md1OHLCV{}, fmt.Errorf("md1 offset %d out of range (size %d)", offset, len(md1Data))
	}

	blk := md1Data[offset : offset+md1BlockSize]

	return md1OHLCV{
		Open:   math.Float64frombits(binary.LittleEndian.Uint64(blk[12:20])),
		High:   math.Float64frombits(binary.LittleEndian.Uint64(blk[20:28])),
		Low:    math.Float64frombits(binary.LittleEndian.Uint64(blk[28:36])),
		Close:  math.Float64frombits(binary.LittleEndian.Uint64(blk[36:44])),
		Volume: binary.LittleEndian.Uint32(blk[56:60]),
		Amount: math.Float64frombits(binary.LittleEndian.Uint64(blk[72:80])),
	}, nil
}

// makeDayRecord encodes one 32-byte .day record from OHLCV data and a date.
//
// .day file format (32 bytes per record, little-endian):
//
//	[0:4]   uint32   date (YYYYMMDD)
//	[4:8]   uint32   open (price * 100)
//	[8:12]  uint32   high (price * 100)
//	[12:16] uint32   low (price * 100)
//	[16:20] uint32   close (price * 100)
//	[20:24] float32  amount (IEEE 754)
//	[24:28] uint32   volume
//	[28:32] uint32   reserved
func makeDayRecord(date uint32, rec md1OHLCV) []byte {
	buf := make([]byte, recordSize)

	binary.LittleEndian.PutUint32(buf[0:4], date)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(math.Round(rec.Open*100)))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(math.Round(rec.High*100)))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(math.Round(rec.Low*100)))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(math.Round(rec.Close*100)))
	binary.LittleEndian.PutUint32(buf[20:24], math.Float32bits(float32(rec.Amount)))
	binary.LittleEndian.PutUint32(buf[24:28], rec.Volume)

	return buf
}

// mergeSingleDay processes one .cod+.md1 pair for a single exchange and date,
// appending new records to the corresponding per-stock .day files.
func mergeSingleDay(vipdocDir, exchange string, date uint32, codFile, md1File string) error {
	entries, err := parseCodEntries(codFile)
	if err != nil {
		return err
	}

	md1Data, err := os.ReadFile(md1File)
	if err != nil {
		return fmt.Errorf("read md1: %w", err)
	}

	ldayDir := filepath.Join(vipdocDir, exchange, "lday")
	if err := os.MkdirAll(ldayDir, 0755); err != nil {
		return fmt.Errorf("create lday dir: %w", err)
	}

	for _, ent := range entries {
		ohlcv, err := readMd1Block(md1Data, ent.SeqNum)
		if err != nil {
			continue
		}

		if ohlcv.Volume == 0 && ohlcv.Amount == 0 {
			continue
		}

		if ohlcv.Open <= 0 || ohlcv.Close <= 0 {
			continue
		}

		dayFileName := fmt.Sprintf("%s%s.day", exchange, ent.StockCode)
		dayFilePath := filepath.Join(ldayDir, dayFileName)

		dayRec := makeDayRecord(date, ohlcv)

		if err := appendDayRecord(dayFilePath, date, dayRec); err != nil {
			continue
		}
	}

	return nil
}

// appendDayRecord appends a 32-byte record to a .day file.
// It deduplicates by checking whether the file's last record already
// has the same or a newer date.
func appendDayRecord(path string, date uint32, record []byte) error {
	fi, err := os.Stat(path)
	if err == nil && fi.Size() >= recordSize {
		f, err := os.Open(path)
		if err != nil {
			return err
		}

		lastRec := make([]byte, recordSize)
		_, readErr := f.ReadAt(lastRec, fi.Size()-recordSize)
		f.Close()
		if readErr != nil {
			return readErr
		}

		lastDate := binary.LittleEndian.Uint32(lastRec[0:4])
		if lastDate >= date {
			return nil
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(record)
	return err
}
