package log

import (
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestPacificTimeEncoderUsesLosAngelesTime(t *testing.T) {
	tests := []struct {
		name string
		time time.Time
		want string
	}{
		{name: "PDT", time: time.Date(2026, 7, 8, 16, 0, 0, 0, time.UTC), want: `"ts":"2026-07-08T09:00:00-07:00"`},
		{name: "PST", time: time.Date(2026, 1, 8, 16, 0, 0, 0, time.UTC), want: `"ts":"2026-01-08T08:00:00-08:00"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := zap.NewProductionEncoderConfig()
			config.EncodeTime = pacificTimeEncoder
			encoder := zapcore.NewJSONEncoder(config)
			buffer, err := encoder.EncodeEntry(
				zapcore.Entry{Level: zapcore.InfoLevel, Time: test.time, Message: "test"},
				[]zapcore.Field{zap.Time("event_time", test.time)},
			)
			if err != nil {
				t.Fatalf("EncodeEntry returned error: %v", err)
			}
			defer buffer.Free()
			if !strings.Contains(buffer.String(), test.want) {
				t.Fatalf("encoded log entry did not use Pacific time: %s", buffer.String())
			}
			if !strings.Contains(buffer.String(), strings.Replace(test.want, `"ts"`, `"event_time"`, 1)) {
				t.Fatalf("encoded log field did not use Pacific time: %s", buffer.String())
			}
		})
	}
}
