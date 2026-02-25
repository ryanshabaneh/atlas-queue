package ratelimit

import (
	"fmt"
	"time"
)

func ConcurrencyLimitKey(queue string) string {
	return fmt.Sprintf("atlas:queue:%s:concurrency_limit", queue)
}

func InflightSetKey(queue string) string {
	return fmt.Sprintf("atlas:queue:%s:inflight", queue)
}

func LatenciesKey(queue string) string {
	return fmt.Sprintf("atlas:queue:%s:latencies", queue)
}

func ErrorBucketKey(queue string, bucket int64) string {
	return fmt.Sprintf("atlas:queue:%s:errors:%d", queue, bucket)
}

func SuccessBucketKey(queue string, bucket int64) string {
	return fmt.Sprintf("atlas:queue:%s:successes:%d", queue, bucket)
}

func CurrentBucket() int64 {
	return time.Now().Unix() / 10
}
