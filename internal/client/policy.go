package client

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// ReadPolicy contains operator resource/compatibility budgets, not claimed
// WhatsApp quotas. None of these are agent-facing synchronization arguments.
type ReadPolicy struct {
	Timezone                                                                  string
	Timeout                                                                   time.Duration
	RecentSyncTimeout                                                         time.Duration
	HistoryConcurrency, HistoryQueue, HistoryBatch, MaxPage, MaxResponseBytes int
}

func DefaultReadPolicy() ReadPolicy {
	return ReadPolicy{Timeout: ReadTimeout, RecentSyncTimeout: 3 * time.Second, Timezone: "UTC", HistoryConcurrency: 1, HistoryQueue: 64, HistoryBatch: 50, MaxPage: 100, MaxResponseBytes: 256 * 1024}
}
func readPolicyFromEnv() (ReadPolicy, error) {
	p := DefaultReadPolicy()
	if zone := os.Getenv("TZ"); zone != "" {
		p.Timezone = zone
	}
	if _, err := time.LoadLocation(p.Timezone); err != nil {
		return p, fmt.Errorf("invalid display timezone: %w", err)
	}
	for _, v := range []struct {
		name   string
		target *int
		max    int
	}{
		{"WHATSAPP_HISTORY_CONCURRENCY", &p.HistoryConcurrency, 64}, {"WHATSAPP_HISTORY_QUEUE", &p.HistoryQueue, 4096}, {"WHATSAPP_HISTORY_BATCH_SIZE", &p.HistoryBatch, 2147483647}, {"WHATSAPP_MAX_PAGE_SIZE", &p.MaxPage, 10000}, {"WHATSAPP_MAX_RESPONSE_BYTES", &p.MaxResponseBytes, 16777216},
	} {
		if raw := os.Getenv(v.name); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > v.max {
				return p, fmt.Errorf("%s must be 1..%d", v.name, v.max)
			}
			*v.target = n
		}
	}
	if raw := os.Getenv("WHATSAPP_READ_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return p, fmt.Errorf("WHATSAPP_READ_TIMEOUT must be a positive duration")
		}
		p.Timeout = d
	}
	if raw := os.Getenv("WHATSAPP_RECENT_SYNC_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return p, fmt.Errorf("WHATSAPP_RECENT_SYNC_TIMEOUT must be a positive duration")
		}
		p.RecentSyncTimeout = d
	}
	if p.HistoryQueue < p.HistoryConcurrency {
		return p, fmt.Errorf("history queue must accommodate all active requests")
	}
	return p, nil
}
func (c *Client) ReadPolicy() ReadPolicy {
	if c == nil || c.policy.Timeout == 0 {
		return DefaultReadPolicy()
	}
	return c.policy
}
