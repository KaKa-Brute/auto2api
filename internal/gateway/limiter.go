// Package gateway 并发限流器：防止上游 API 超时导致连接堆积。
// 用带缓冲 channel 作信号量（令牌桶），等待超时快速失败避免资源耗尽。
package gateway

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ConcurrencyLimiter 限制全局并发请求数，防止连接堆积导致 OOM。
// slots 是容量=maxConcurrent 的令牌桶；waiting 记录当前排队数，
// 超过 maxQueueSize 直接拒绝，避免排队请求无限堆积。
type ConcurrencyLimiter struct {
	maxConcurrent int           // 最大并发数，0=禁用
	maxQueueSize  int           // 最大等待队列长度
	queueTimeout  time.Duration // 队列等待超时
	slots         chan struct{} // 令牌桶：取到令牌即获得并发槽位

	mu            sync.Mutex
	waiting       int   // 当前排队等待数
	totalRejected int64 // 累计拒绝数（队列满）
	totalTimeout  int64 // 累计超时数
}

// NewConcurrencyLimiter 创建并发限流器。maxConcurrent=0 表示不限流。
func NewConcurrencyLimiter(maxConcurrent, maxQueueSize int, queueTimeout time.Duration) *ConcurrencyLimiter {
	if maxConcurrent <= 0 {
		return &ConcurrencyLimiter{maxConcurrent: 0} // 禁用
	}
	if maxQueueSize <= 0 {
		maxQueueSize = maxConcurrent * 2
	}
	if queueTimeout <= 0 {
		queueTimeout = 30 * time.Second
	}
	return &ConcurrencyLimiter{
		maxConcurrent: maxConcurrent,
		maxQueueSize:  maxQueueSize,
		queueTimeout:  queueTimeout,
		slots:         make(chan struct{}, maxConcurrent),
	}
}

// Acquire 获取一个并发槽位。有空闲立即返回；否则排队等待，
// 队列满或等待超时/ctx 取消则返回错误。调用方成功后须调用 Release()。
func (l *ConcurrencyLimiter) Acquire(ctx context.Context) error {
	if l.maxConcurrent == 0 {
		return nil // 禁用限流
	}
	// 快路径：有空闲槽位立即获取
	select {
	case l.slots <- struct{}{}:
		return nil
	default:
	}

	// 无空闲：先登记排队，超过队列上限直接拒绝
	l.mu.Lock()
	if l.waiting >= l.maxQueueSize {
		l.totalRejected++
		l.mu.Unlock()
		return fmt.Errorf("rate_limit_exceeded: queue full (%d)", l.maxQueueSize)
	}
	l.waiting++
	l.mu.Unlock()

	defer func() {
		l.mu.Lock()
		l.waiting--
		l.mu.Unlock()
	}()

	timer := time.NewTimer(l.queueTimeout)
	defer timer.Stop()

	select {
	case l.slots <- struct{}{}:
		return nil
	case <-timer.C:
		atomic.AddInt64(&l.totalTimeout, 1)
		return fmt.Errorf("rate_limit_timeout: waited %s", l.queueTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release 释放一个并发槽位。
func (l *ConcurrencyLimiter) Release() {
	if l.maxConcurrent == 0 {
		return
	}
	select {
	case <-l.slots:
	default:
	}
}

// Stats 返回 (当前并发, 累计拒绝数, 累计超时数)。
func (l *ConcurrencyLimiter) Stats() (int, int64, int64) {
	l.mu.Lock()
	rejected := l.totalRejected
	l.mu.Unlock()
	return len(l.slots), rejected, atomic.LoadInt64(&l.totalTimeout)
}
