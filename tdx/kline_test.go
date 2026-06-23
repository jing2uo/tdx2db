package tdx

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestParseDayVolumeIgnoresReserved(t *testing.T) {
	tests := []struct {
		name     string
		volRaw   uint32
		reserved uint32
		want     int64
	}{
		{"modern reserved", 189_000_000, 0x10000, 189_000_000},
		{"early sz reserved zero", 25_859_600, 0x00000000, 25_859_600},
		{"early sh reserved 1000", 174_085_000, 0x000003E8, 174_085_000},
		{"early sh reserved 3139", 40_631_800, 0x00000C43, 40_631_800},
		{"nonzero low byte", 478_000_000, 0x0000002A, 478_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseDayVolume(tt.volRaw, tt.reserved)
			if got != tt.want {
				t.Errorf("parseDayVolume(%d, 0x%x) = %d, want %d", tt.volRaw, tt.reserved, got, tt.want)
			}
		})
	}
}

func TestProcessDayFileIgnoresReservedForVolume(t *testing.T) {
	data := make([]byte, recordSize)
	binary.LittleEndian.PutUint32(data[0:4], 19991110)
	binary.LittleEndian.PutUint32(data[4:8], 2775)
	binary.LittleEndian.PutUint32(data[8:12], 2775)
	binary.LittleEndian.PutUint32(data[12:16], 2775)
	binary.LittleEndian.PutUint32(data[16:20], 2775)
	binary.LittleEndian.PutUint32(data[20:24], math.Float32bits(float32(4_859_102_208)))
	binary.LittleEndian.PutUint32(data[24:28], 174_085_000)
	binary.LittleEndian.PutUint32(data[28:32], 0x000003E8)

	rows, err := processDayFile(data, "sh600000")
	if err != nil {
		t.Fatalf("processDayFile: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1", len(rows))
	}
	if rows[0].Volume != 174_085_000 {
		t.Errorf("volume = %d, want 174085000", rows[0].Volume)
	}
	if rows[0].Close != 27.75 {
		t.Errorf("close = %f, want 27.75", rows[0].Close)
	}
}
