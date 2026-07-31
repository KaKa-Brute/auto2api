// Package gateway 内存守护：监控内存使用并自动降级防止 OOM。
// 定期检测内存，超过阈值后拒绝新请求（503 + Retry-After），给 GC 时间回收。
package gateway

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryGuard 内存守护，周期性检测内存使用率，超过阈值时触发自动降级。
type MemoryGuard struct {
	maxMemoryMB    int64         // 最大允许内存（MB），0 表示禁用
	warningPercent float64       // 警告阈值百分比（如 0.8 = 80%）
	criticalPercent float64      // 临界阈值百分比（如 0.9 = 90%），超过后拒绝请求
	checkInterval  time.Duration // 检查周期
	stopCh         chan struct{}
	
	mu             sync.RWMutex
	degraded       int32  // 原子标志：1=已降级，0=正常
	lastCheckMB    int64  // 最近一次检测的内存使用（MB）
	lastGC         time.Time
	totalRejected  int64  // 累计降级拒绝数
}

// NewMemoryGuard 创建内存守护。maxMemoryMB=0 表示禁用。
func NewMemoryGuard(maxMemoryMB int64, warningPercent, criticalPercent float64, checkInterval time.Duration) *MemoryGuard {
	if maxMemoryMB <= 0 {
		return &MemoryGuard{maxMemoryMB: 0} // 禁用
	}
	if warningPercent <= 0 || warningPercent > 1 {
		warningPercent = 0.8
	}
	if criticalPercent <= 0 || criticalPercent > 1 {
		criticalPercent = 0.9
	}
	if checkInterval <= 0 {
		checkInterval = 10 * time.Second
	}
	return &MemoryGuard{
		maxMemoryMB:     maxMemoryMB,
		warningPercent:  warningPercent,
		criticalPercent: criticalPercent,
		checkInterval:   checkInterval,
		stopCh:          make(chan struct{}),
	}
}

// Start 启动后台监控 goroutine。
func (g *MemoryGuard) Start() {
	if g.maxMemoryMB == 0 {
		return
	}
	go g.loop()
	log.Printf("[memguard] started: max=%dMB, warn=%.0f%%, critical=%.0f%%, interval=%s",
		g.maxMemoryMB, g.warningPercent*100, g.criticalPercent*100, g.checkInterval)
}

// Stop 停止后台监控。
func (g *MemoryGuard) Stop() {
	if g.maxMemoryMB == 0 {
		return
	}
	select {
	case <-g.stopCh:
	default:
		close(g.stopCh)
	}
}

func (g *MemoryGuard) loop() {
	ticker := time.NewTicker(g.checkInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.check()
		}
	}
}

func (g *MemoryGuard) check() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	usedMB := int64(m.Alloc / 1024 / 1024)
	g.mu.Lock()
	g.lastCheckMB = usedMB
	g.mu.Unlock()
	
	usagePercent := float64(usedMB) / float64(g.maxMemoryMB)
	
	// 超过临界阈值：降级并强制 GC
	if usagePercent >= g.criticalPercent {
		if atomic.CompareAndSwapInt32(&g.degraded, 0, 1) {
			log.Printf("[memguard] CRITICAL: memory=%dMB/%.1f%%, entering degraded mode", usedMB, usagePercent*100)
		}
		g.mu.Lock()
		if time.Since(g.lastGC) > 5*time.Second {
			runtime.GC()
			g.lastGC = time.Now()
			log.Printf("[memguard] forced GC triggered")
		}
		g.mu.Unlock()
		return
	}
	
	// 超过警告阈值但未到临界：仅日志警告 + 主动 GC
	if usagePercent >= g.warningPercent {
		if atomic.LoadInt32(&g.degraded) == 1 {
			log.Printf("[memguard] WARNING: memory=%dMB/%.1f%%, still in degraded mode", usedMB, usagePercent*100)
		} else {
			log.Printf("[memguard] WARNING: memory=%dMB/%.1f%%, approaching limit", usedMB, usagePercent*100)
		}
		g.mu.Lock()
		if time.Since(g.lastGC) > 10*time.Second {
			runtime.GC()
			g.lastGC = time.Now()
		}
		g.mu.Unlock()
		return
	}
	
	// 低于警告阈值：恢复正常
	if atomic.CompareAndSwapInt32(&g.degraded, 1, 0) {
		log.Printf("[memguard] RECOVERED: memory=%dMB/%.1f%%, back to normal", usedMB, usagePercent*100)
	}
}

// AllowRequest 检查是否允许请求。降级模式下拒绝新请求。
func (g *MemoryGuard) AllowRequest() (allowed bool, reason string) {
	if g.maxMemoryMB == 0 {
		return true, ""
	}
	
	if atomic.LoadInt32(&g.degraded) == 1 {
		g.mu.Lock()
		g.totalRejected++
		usedMB := g.lastCheckMB
		g.mu.Unlock()
		return false, fmt.Sprintf("memory_overload: %dMB/%dMB", usedMB, g.maxMemoryMB)
	}
	return true, ""
}

// Stats 返回 (当前内存MB, 是否降级, 累计拒绝数)。
func (g *MemoryGuard) Stats() (int64, bool, int64) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.lastCheckMB, atomic.LoadInt32(&g.degraded) == 1, g.totalRejected
}