package services

import (
	"testing"
	"time"
)

func TestSnowflakePacificTimestampHandlesDaylightSaving(t *testing.T) {
	tests := []struct {
		name       string
		in         time.Time
		wantHour   int
		wantOffset int
	}{
		{
			name:       "summer daylight time",
			in:         time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC),
			wantHour:   9,
			wantOffset: -7 * 60 * 60,
		},
		{
			name:       "winter standard time",
			in:         time.Date(2026, 1, 8, 16, 0, 0, 0, time.UTC),
			wantHour:   8,
			wantOffset: -8 * 60 * 60,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := snowflakePacificTimestamp(test.in)
			_, offset := got.Zone()
			if got.Hour() != test.wantHour || offset != test.wantOffset || !got.Equal(test.in) {
				t.Fatalf("snowflakePacificTimestamp(%s) = %s, want hour %d offset %d", test.in, got, test.wantHour, test.wantOffset)
			}
		})
	}
}
