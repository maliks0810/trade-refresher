package ptime

import (
	"testing"
	"time"
)

func TestPacificLogTimestampHandlesDaylightSaving(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{
			name: "summer daylight time",
			in:   time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC),
			want: "2026-07-08T09:00:00-07:00",
		},
		{
			name: "winter standard time",
			in:   time.Date(2026, 1, 8, 16, 0, 0, 0, time.UTC),
			want: "2026-01-08T08:00:00-08:00",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := PacificLogTimestamp(test.in); got != test.want {
				t.Fatalf("PacificLogTimestamp(%s) = %s, want %s", test.in, got, test.want)
			}
		})
	}
}
