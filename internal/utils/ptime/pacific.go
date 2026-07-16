package ptime

import (
	"time"
	_ "time/tzdata"
)

const PacificTimeZone = "America/Los_Angeles"

var pacificLocation = loadPacificLocation()

func PacificLocation() *time.Location {
	return pacificLocation
}

func loadPacificLocation() *time.Location {
	location, err := time.LoadLocation(PacificTimeZone)
	if err != nil {
		panic("America/Los_Angeles timezone data is unavailable")
	}
	return location
}

func InPacific(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return value.In(PacificLocation())
}

func PacificLogTimestamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return InPacific(value).Format(time.RFC3339Nano)
}

func SnowflakePacificTimestamp(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return InPacific(value)
}
