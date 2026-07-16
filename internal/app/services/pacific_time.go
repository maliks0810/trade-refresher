package services

import (
	"time"

	"refresher/trade-refresher/internal/utils/ptime"
)

func snowflakePacificTimestamp(value time.Time) time.Time {
	return ptime.SnowflakePacificTimestamp(value)
}

func pacificLocation() *time.Location {
	return ptime.PacificLocation()
}

func pacificLogTimestamp(value time.Time) string {
	return ptime.PacificLogTimestamp(value)
}
