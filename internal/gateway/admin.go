// 可视化管理页（/chat）：免鉴权的配置编辑、服务启动/重启、日志查看。
// 安全提示：本页面不鉴权，会暴露配置（含上游 api_key）与重启能力，仅建议内网/本机使用。
package gateway

import (
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

// AdminHandler 提供 /chat 管理页及其后端 API。
type AdminHandler struct {
	configPath string        // 配置文件路径，用于读写
	logDir     string        // 日志目录，用于列出/读取日志
	restart    func() error // 触发进程重启的回调（由 main 注入）
}

// NewAdminHandler 构造管理页处理器。restart 为触发服务重启的回调，可为 nil（此时重启接口返回未启用）。
func NewAdminHandler(configPath, logDir string, restart func() error) *AdminHandler {
	return &AdminHandler{configPath: configPath, logDir: logDir, restart: restart}
}

// Register 把管理页与其 API 挂到 gin 引擎（全部免鉴权）。
func (a *AdminHandler) Register(r *gin.Engine) {
	r.GET("/chat", a.page)
	r.GET("/chat/api/config", a.getConfig)
	r.POST("/chat/api/config", a.saveConfig)
	r.POST("/chat/api/restart", a.doRestart)
	r.GET("/chat/api/logs", a.listLogs)
	r.GET("/chat/api/logs/content", a.logContent)
}

// getConfig 返回当前配置文件原始内容。
func (a *AdminHandler) getConfig(c *gin.Context) {
	b, err := os.ReadFile(a.configPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取配置失败: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"path": a.configPath, "content": string(b)})
}

// saveConfig 校验并写回配置文件（先做 YAML 语法校验，避免写入非法配置）。
func (a *AdminHandler) saveConfig(c *gin.Context) {
	var req struct {
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求体无效: " + err.Error()})
		return
	}
	// YAML 语法校验：能解析成 map 才认为合法
	var probe map[string]interface{}
	if err := yaml.Unmarshal([]byte(req.Content), &probe); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "YAML 语法错误: " + err.Error()})
		return
	}
	if err := os.WriteFile(a.configPath, []byte(req.Content), 0o644); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "写入配置失败: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "message": "配置已保存，重启服务后生效"})
}

// doRestart 触发进程重启（重新加载配置）。
func (a *AdminHandler) doRestart(c *gin.Context) {
	if a.restart == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "重启功能未启用"})
		return
	}
	if err := a.restart(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "重启失败: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "message": "服务正在重启，请稍候刷新页面"})
}

// listLogs 返回日志目录下的日志文件列表（按修改时间倒序）。
func (a *AdminHandler) listLogs(c *gin.Context) {
	entries, err := os.ReadDir(a.logDir)
	if err != nil {
		// 目录不存在时返回空列表而非报错
		c.JSON(http.StatusOK, gin.H{"dir": a.logDir, "files": []gin.H{}})
		return
	}
	type fileInfo struct {
		name    string
		size    int64
		modUnix int64
	}
	files := make([]fileInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fileInfo{name: e.Name(), size: info.Size(), modUnix: info.ModTime().Unix()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modUnix > files[j].modUnix })
	out := make([]gin.H, 0, len(files))
	for _, f := range files {
		out = append(out, gin.H{"name": f.name, "size": f.size, "mod": f.modUnix})
	}
	c.JSON(http.StatusOK, gin.H{"dir": a.logDir, "files": out})
}

// logContent 返回指定日志文件的尾部内容（默认最后 500 行，防止大文件卡死浏览器）。
func (a *AdminHandler) logContent(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "缺少文件名"})
		return
	}
	// 防目录穿越：只允许纯文件名
	if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "非法文件名"})
		return
	}
	path := filepath.Join(a.logDir, name)
	b, err := os.ReadFile(path)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取日志失败: " + err.Error()})
		return
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	// 末尾行数：默认 500，可由 lines 查询参数指定（1~5000）
	maxLines := 500
	if v := c.Query("lines"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 5000 {
				n = 5000
			}
			maxLines = n
		}
	}
	truncated := false
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
		truncated = true
	}
	c.JSON(http.StatusOK, gin.H{
		"name":      name,
		"lines":     lines,
		"truncated": truncated,
		"total":     len(lines),
	})
}

// page 输出管理页 HTML（内联，无外部依赖）。
func (a *AdminHandler) page(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(adminPageHTML))
}
