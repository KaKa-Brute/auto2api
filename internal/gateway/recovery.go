// Package gateway panic recovery：捕获所有 panic 防止进程退出。
// 记录完整堆栈并返回 500，保证服务持续可用。
package gateway

import (
	"log"
	"net/http"
	"runtime/debug"
	"sync/atomic"

	"github.com/gin-gonic/gin"
)

var (
	totalPanics int64 // 累计 panic 次数（供监控）
)

// RecoveryMiddleware 捕获 panic 并返回 500，记录完整堆栈，避免进程退出。
// 比 gin.Recovery() 更详细的日志 + 计数器。
func RecoveryMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				atomic.AddInt64(&totalPanics, 1)
				stack := debug.Stack()
				
				log.Printf("[PANIC RECOVERED] %v\n%s", err, string(stack))
				
				// 如果响应头已提交，无法再写错误响应
				if c.Writer.Written() {
					log.Printf("[PANIC] response already committed, cannot send error")
					c.Abort()
					return
				}
				
				// 按出站格式返回错误
				format := c.GetString("outbound_format")
				if format == "claude" {
					c.JSON(http.StatusInternalServerError, gin.H{
						"type": "error",
						"error": gin.H{
							"type":    "internal_server_error",
							"message": "internal server error (panic recovered)",
						},
					})
				} else {
					c.JSON(http.StatusInternalServerError, gin.H{
						"error": gin.H{
							"message": "internal server error (panic recovered)",
							"type":    "internal_server_error",
						},
					})
				}
				c.Abort()
			}
		}()
		c.Next()
	}
}

// GetPanicStats 返回累计 panic 次数。
func GetPanicStats() int64 {
	return atomic.LoadInt64(&totalPanics)
}
