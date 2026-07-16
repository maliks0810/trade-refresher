package services

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

const (
	maxAladdinReadsPerMinute  = 1000
	maxAladdinWritesPerMinute = 250
)

type RateLimitConfig struct {
	ReadPerMinute  int
	WritePerMinute int
	ReadBurst      int
	WriteBurst     int
}

type SharedLimiter struct {
	read           *rate.Limiter
	write          *rate.Limiter
	readPerMinute  int
	writePerMinute int
}

func NewSharedLimiter(cfg RateLimitConfig) *SharedLimiter {
	cfg.ReadPerMinute = boundedQuota(cfg.ReadPerMinute, maxAladdinReadsPerMinute)
	cfg.WritePerMinute = boundedQuota(cfg.WritePerMinute, maxAladdinWritesPerMinute)
	if cfg.ReadBurst <= 0 {
		cfg.ReadBurst = 25
	}
	if cfg.WriteBurst <= 0 {
		cfg.WriteBurst = 5
	}
	if cfg.ReadBurst > cfg.ReadPerMinute {
		cfg.ReadBurst = cfg.ReadPerMinute
	}
	if cfg.WriteBurst > cfg.WritePerMinute {
		cfg.WriteBurst = cfg.WritePerMinute
	}

	return &SharedLimiter{
		read:           rate.NewLimiter(rate.Every(time.Minute/time.Duration(cfg.ReadPerMinute)), cfg.ReadBurst),
		write:          rate.NewLimiter(rate.Every(time.Minute/time.Duration(cfg.WritePerMinute)), cfg.WriteBurst),
		readPerMinute:  cfg.ReadPerMinute,
		writePerMinute: cfg.WritePerMinute,
	}
}

func (l *SharedLimiter) LimitPerMinute(family QuotaFamily, fallback int) int {
	if l == nil {
		return fallback
	}
	if family == QuotaWrite {
		return l.writePerMinute
	}
	return l.readPerMinute
}

func boundedQuota(value, maximum int) int {
	if value <= 0 || value > maximum {
		return maximum
	}
	return value
}

func (l *SharedLimiter) Wait(ctx context.Context, family QuotaFamily) error {
	if l == nil {
		return nil
	}
	if family == QuotaWrite {
		return l.write.Wait(ctx)
	}
	return l.read.Wait(ctx)
}
