package tdx

import "testing"

func TestParseVolumeOverflow(t *testing.T) {
	tests := []struct {
		name     string
		volRaw   uint32
		reserved uint32
		want     int64
	}{
		{"normal", 189_000_000, 0x10000, 189_000_000},
		{"overflow sh601868 2025-03-12", 478_000_000, 0x00000000, 47_800_000_000},
		{"overflow with low byte", 478_000_000, 0x0000002A, 47_800_000_042},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseVolumeOverflow(tt.volRaw, tt.reserved)
			if got != tt.want {
				t.Errorf("parseVolumeOverflow(%d, 0x%x) = %d, want %d", tt.volRaw, tt.reserved, got, tt.want)
			}
		})
	}
}
