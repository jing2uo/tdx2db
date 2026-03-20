package tdx

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestParseIncrFilename(t *testing.T) {
	tests := []struct {
		name     string
		wantExch string
		wantDate uint32
		wantErr  bool
	}{
		{"sh260318.md1", "sh", 20260318, false},
		{"sz260318.cod", "sz", 20260318, false},
		{"bj260318.md1", "bj", 20260318, false},
		{"sh990101.md1", "sh", 19990101, false},
		{"sh250610.md1", "sh", 20250610, false},
		{"ab.md1", "", 0, true},       // too short
		{"sh13.md1", "", 0, true},     // too short date part
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exch, date, err := parseIncrFilename(tt.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseIncrFilename(%q) error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if exch != tt.wantExch || date != tt.wantDate {
					t.Errorf("parseIncrFilename(%q) = (%q, %d), want (%q, %d)",
						tt.name, exch, date, tt.wantExch, tt.wantDate)
				}
			}
		})
	}
}

func TestMakeDayRecord(t *testing.T) {
	rec := md1OHLCV{
		Open:   1489.00,
		High:   1496.50,
		Low:    1463.15,
		Close:  1468.80,
		Amount: 5239992522.0,
		Volume: 3555100,
	}

	buf := makeDayRecord(20260318, rec)

	if len(buf) != recordSize {
		t.Fatalf("expected %d bytes, got %d", recordSize, len(buf))
	}

	// Verify date
	date := binary.LittleEndian.Uint32(buf[0:4])
	if date != 20260318 {
		t.Errorf("date = %d, want 20260318", date)
	}

	// Verify OHLC (price * 100)
	openRaw := binary.LittleEndian.Uint32(buf[4:8])
	if openRaw != 148900 {
		t.Errorf("open = %d, want 148900", openRaw)
	}

	highRaw := binary.LittleEndian.Uint32(buf[8:12])
	if highRaw != 149650 {
		t.Errorf("high = %d, want 149650", highRaw)
	}

	lowRaw := binary.LittleEndian.Uint32(buf[12:16])
	if lowRaw != 146315 {
		t.Errorf("low = %d, want 146315", lowRaw)
	}

	closeRaw := binary.LittleEndian.Uint32(buf[16:20])
	if closeRaw != 146880 {
		t.Errorf("close = %d, want 146880", closeRaw)
	}

	// Verify volume
	volRaw := binary.LittleEndian.Uint32(buf[24:28])
	if volRaw != 3555100 {
		t.Errorf("volume = %d, want 3555100", volRaw)
	}

	// Verify amount (float32)
	amtBits := binary.LittleEndian.Uint32(buf[20:24])
	amt := math.Float32frombits(amtBits)
	if math.Abs(float64(amt)-5239992522.0) > 1e6 { // float32 precision
		t.Errorf("amount = %f, want ~5239992522", amt)
	}

	// Verify reserved is zero
	reserved := binary.LittleEndian.Uint32(buf[28:32])
	if reserved != 0 {
		t.Errorf("reserved = %d, want 0", reserved)
	}
}

func TestAppendDayRecordDedup(t *testing.T) {
	tmpDir := t.TempDir()
	dayFile := filepath.Join(tmpDir, "test.day")

	rec := md1OHLCV{Open: 10, High: 11, Low: 9, Close: 10.5, Amount: 1000, Volume: 100}
	buf := makeDayRecord(20260318, rec)

	// First write
	if err := appendDayRecord(dayFile, 20260318, buf); err != nil {
		t.Fatalf("first append: %v", err)
	}

	fi1, _ := os.Stat(dayFile)
	if fi1.Size() != int64(recordSize) {
		t.Fatalf("expected %d bytes after first write, got %d", recordSize, fi1.Size())
	}

	// Duplicate write — should be skipped
	if err := appendDayRecord(dayFile, 20260318, buf); err != nil {
		t.Fatalf("duplicate append: %v", err)
	}

	fi2, _ := os.Stat(dayFile)
	if fi2.Size() != int64(recordSize) {
		t.Errorf("dedup failed: size grew from %d to %d", fi1.Size(), fi2.Size())
	}

	// Older date — should also be skipped
	oldBuf := makeDayRecord(20260317, rec)
	if err := appendDayRecord(dayFile, 20260317, oldBuf); err != nil {
		t.Fatalf("old date append: %v", err)
	}

	fi3, _ := os.Stat(dayFile)
	if fi3.Size() != int64(recordSize) {
		t.Errorf("old date was not skipped: size = %d", fi3.Size())
	}

	// Newer date — should be appended
	newBuf := makeDayRecord(20260319, rec)
	if err := appendDayRecord(dayFile, 20260319, newBuf); err != nil {
		t.Fatalf("new date append: %v", err)
	}

	fi4, _ := os.Stat(dayFile)
	if fi4.Size() != int64(2*recordSize) {
		t.Errorf("new date not appended: size = %d, want %d", fi4.Size(), 2*recordSize)
	}
}

func TestParseCodEntries(t *testing.T) {
	// Create a minimal synthetic .cod file with 2 records
	tmpDir := t.TempDir()
	codFile := filepath.Join(tmpDir, "test.cod")

	data := make([]byte, 2*codRecordSize)

	// Record 0: code "600519", seq=42
	copy(data[0:6], "600519")
	binary.LittleEndian.PutUint16(data[32:34], 42)

	// Record 1: code "000001", seq=7
	copy(data[codRecordSize:codRecordSize+6], "000001")
	binary.LittleEndian.PutUint16(data[codRecordSize+32:codRecordSize+34], 7)

	if err := os.WriteFile(codFile, data, 0644); err != nil {
		t.Fatal(err)
	}

	entries, err := parseCodEntries(codFile)
	if err != nil {
		t.Fatalf("parseCodEntries: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if entries[0].StockCode != "600519" || entries[0].SeqNum != 42 {
		t.Errorf("entry 0: got (%q, %d), want (600519, 42)", entries[0].StockCode, entries[0].SeqNum)
	}

	if entries[1].StockCode != "000001" || entries[1].SeqNum != 7 {
		t.Errorf("entry 1: got (%q, %d), want (000001, 7)", entries[1].StockCode, entries[1].SeqNum)
	}
}

func TestReadMd1Block(t *testing.T) {
	// Create a synthetic md1 block at seq=0
	data := make([]byte, md1BlockSize)

	// Write known OHLCV values
	binary.LittleEndian.PutUint64(data[12:20], math.Float64bits(100.50))  // open
	binary.LittleEndian.PutUint64(data[20:28], math.Float64bits(105.00))  // high
	binary.LittleEndian.PutUint64(data[28:36], math.Float64bits(99.00))   // low
	binary.LittleEndian.PutUint64(data[36:44], math.Float64bits(103.25))  // close
	binary.LittleEndian.PutUint32(data[56:60], 50000)                     // volume
	binary.LittleEndian.PutUint64(data[72:80], math.Float64bits(5000000)) // amount

	ohlcv, err := readMd1Block(data, 0)
	if err != nil {
		t.Fatalf("readMd1Block: %v", err)
	}

	if ohlcv.Open != 100.50 {
		t.Errorf("open = %f, want 100.50", ohlcv.Open)
	}
	if ohlcv.High != 105.00 {
		t.Errorf("high = %f, want 105.00", ohlcv.High)
	}
	if ohlcv.Low != 99.00 {
		t.Errorf("low = %f, want 99.00", ohlcv.Low)
	}
	if ohlcv.Close != 103.25 {
		t.Errorf("close = %f, want 103.25", ohlcv.Close)
	}
	if ohlcv.Volume != 50000 {
		t.Errorf("volume = %d, want 50000", ohlcv.Volume)
	}
	if ohlcv.Amount != 5000000 {
		t.Errorf("amount = %f, want 5000000", ohlcv.Amount)
	}

	// Out of range
	if _, err := readMd1Block(data, 1); err == nil {
		t.Error("expected error for out-of-range seq")
	}
}
