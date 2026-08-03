"""可视化管理台（/chat）：免鉴权的配置编辑、服务重启、日志查看。

对齐 Go 版 internal/gateway/admin.go。
安全提示：本页面不鉴权，会暴露配置（含上游 api_key）与重启能力，仅建议内网/本机使用。
"""
import os

import yaml
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route


class AdminHandler:
    """提供 /chat 管理页及其后端 API。"""

    def __init__(self, config_path: str, log_dir: str, restart):
        self.config_path = config_path
        self.log_dir = log_dir
        self.restart = restart  # 触发重启的回调，可为 None

    def routes(self):
        """返回管理台路由列表（全部免鉴权）。"""
        return [
            Route("/chat", self.page, methods=["GET"]),
            Route("/chat/api/config", self.get_config, methods=["GET"]),
            Route("/chat/api/config", self.save_config, methods=["POST"]),
            Route("/chat/api/restart", self.do_restart, methods=["POST"]),
            Route("/chat/api/logs", self.list_logs, methods=["GET"]),
            Route("/chat/api/logs/content", self.log_content, methods=["GET"]),
        ]

    async def page(self, request: Request) -> HTMLResponse:
        return HTMLResponse(ADMIN_PAGE_HTML)

    async def get_config(self, request: Request) -> JSONResponse:
        """返回当前配置文件原始内容。"""
        try:
            with open(self.config_path, "r", encoding="utf-8") as fp:
                content = fp.read()
        except OSError as e:
            return JSONResponse({"error": "读取配置失败: " + str(e)}, status_code=500)
        return JSONResponse({"path": self.config_path, "content": content})

    async def save_config(self, request: Request) -> JSONResponse:
        """校验并写回配置文件（先做 YAML 语法校验，避免写入非法配置）。"""
        try:
            req = await request.json()
        except Exception as e:  # noqa: BLE001
            return JSONResponse({"error": "请求体无效: " + str(e)}, status_code=400)
        content = req.get("content", "")
        # YAML 语法校验：能解析成 dict 才认为合法
        try:
            probe = yaml.safe_load(content)
        except yaml.YAMLError as e:
            return JSONResponse({"error": "YAML 语法错误: " + str(e)}, status_code=400)
        if not isinstance(probe, dict):
            return JSONResponse({"error": "YAML 语法错误: 顶层必须是映射"}, status_code=400)
        try:
            with open(self.config_path, "w", encoding="utf-8") as fp:
                fp.write(content)
        except OSError as e:
            return JSONResponse({"error": "写入配置失败: " + str(e)}, status_code=500)
        return JSONResponse({"ok": True, "message": "配置已保存，重启服务后生效"})

    async def do_restart(self, request: Request) -> JSONResponse:
        """触发进程重启（重新加载配置）。"""
        if self.restart is None:
            return JSONResponse({"error": "重启功能未启用"}, status_code=503)
        try:
            self.restart()
        except Exception as e:  # noqa: BLE001
            return JSONResponse({"error": "重启失败: " + str(e)}, status_code=500)
        return JSONResponse({"ok": True, "message": "服务正在重启，请稍候刷新页面"})

    async def list_logs(self, request: Request) -> JSONResponse:
        """返回日志目录下的日志文件列表（按修改时间倒序）。"""
        try:
            names = os.listdir(self.log_dir)
        except OSError:
            # 目录不存在时返回空列表而非报错
            return JSONResponse({"dir": self.log_dir, "files": []})
        files = []
        for name in names:
            path = os.path.join(self.log_dir, name)
            if not os.path.isfile(path):
                continue
            try:
                st = os.stat(path)
            except OSError:
                continue
            files.append({"name": name, "size": st.st_size, "mod": int(st.st_mtime)})
        files.sort(key=lambda f: f["mod"], reverse=True)
        return JSONResponse({"dir": self.log_dir, "files": files})

    async def log_content(self, request: Request) -> JSONResponse:
        """返回指定日志文件的尾部内容（默认最后 500 行，防止大文件卡死浏览器）。"""
        name = request.query_params.get("name", "")
        if not name:
            return JSONResponse({"error": "缺少文件名"}, status_code=400)
        # 防目录穿越：只允许纯文件名
        if "/" in name or "\\" in name or ".." in name:
            return JSONResponse({"error": "非法文件名"}, status_code=400)
        path = os.path.join(self.log_dir, name)
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fp:
                data = fp.read()
        except OSError as e:
            return JSONResponse({"error": "读取日志失败: " + str(e)}, status_code=500)
        lines = data.rstrip("\n").split("\n")
        # 末尾行数：默认 500，可由 lines 查询参数指定（1~5000）
        max_lines = 500
        lv = request.query_params.get("lines", "")
        if lv:
            try:
                n = int(lv)
                if n > 0:
                    max_lines = min(n, 5000)
            except ValueError:
                pass
        truncated = False
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
            truncated = True
        return JSONResponse({
            "name": name,
            "lines": lines,
            "truncated": truncated,
            "total": len(lines),
        })


# /chat 管理页的内联 HTML（无外部依赖，纯原生 JS）。与 Go 版 admin_page.go 保持一致。
ADMIN_PAGE_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>auto2api 管理台</title>
<style>
* { box-sizing: border-box; }
body { margin:0; font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif; background:#0f1115; color:#e6e6e6; }
header { padding:16px 24px; background:#171a21; border-bottom:1px solid #2a2f3a; display:flex; align-items:center; gap:16px; }
header h1 { font-size:18px; margin:0; }
header .warn { font-size:12px; color:#e0a458; }
.tabs { display:flex; gap:8px; padding:12px 24px 0; }
.tab { padding:8px 16px; cursor:pointer; border-radius:6px 6px 0 0; background:#171a21; color:#9aa0aa; border:1px solid transparent; }
.tab.active { background:#0f1115; color:#fff; border-color:#2a2f3a; border-bottom-color:#0f1115; }
.panel { display:none; padding:20px 24px; }
.panel.active { display:block; }
button { background:#2f6feb; color:#fff; border:none; padding:8px 16px; border-radius:6px; cursor:pointer; font-size:14px; }
button:hover { background:#3b7cf5; }
button.danger { background:#c93c3c; }
button.danger:hover { background:#e04848; }
button.ghost { background:#2a2f3a; }
textarea { width:100%; height:60vh; background:#0b0d11; color:#d6e0ee; border:1px solid #2a2f3a; border-radius:6px; padding:12px; font-family:"SFMono-Regular",Consolas,monospace; font-size:13px; line-height:1.5; }
.row { display:flex; gap:10px; align-items:center; margin-bottom:12px; flex-wrap:wrap; }
.msg { font-size:13px; padding:6px 10px; border-radius:5px; }
.msg.ok { background:#1e3a24; color:#7ee08a; }
.msg.err { background:#3a1e1e; color:#f08a8a; }
select { background:#0b0d11; color:#d6e0ee; border:1px solid #2a2f3a; border-radius:6px; padding:7px 10px; font-size:14px; min-width:240px; }
pre#logView { background:#0b0d11; border:1px solid #2a2f3a; border-radius:6px; padding:12px; height:62vh; overflow:auto; font-size:12px; line-height:1.5; white-space:pre-wrap; word-break:break-all; margin:0; }
.hint { color:#7a828f; font-size:12px; }
</style>
</head>
<body>
<header>
  <h1>auto2api 管理台</h1>
  <span class="warn">⚠ 本页面无鉴权，请仅在内网/本机使用</span>
</header>
<div class="tabs">
  <div class="tab active" data-tab="config">配置编辑</div>
  <div class="tab" data-tab="service">服务控制</div>
  <div class="tab" data-tab="logs">日志查看</div>
</div>

<div class="panel active" id="panel-config">
  <div class="row">
    <button onclick="loadConfig()" class="ghost">重新加载</button>
    <button onclick="saveConfig()">保存配置</button>
    <span class="hint" id="cfgPath"></span>
    <span id="cfgMsg"></span>
  </div>
  <textarea id="cfgText" spellcheck="false" placeholder="加载中..."></textarea>
  <p class="hint">保存后需在「服务控制」中重启服务才会生效。</p>
</div>

<div class="panel" id="panel-service">
  <div class="row">
    <button onclick="restart()" class="danger">重启服务</button>
    <span id="svcMsg"></span>
  </div>
  <p class="hint">重启会以最新配置启动新进程并平滑接管端口，在途请求会被优雅处理。重启期间接口短暂不可用。</p>
</div>

<div class="panel" id="panel-logs">
  <div class="row">
    <select id="logSelect" onchange="loadLog()"></select>
    <button onclick="refreshLogs()" class="ghost">刷新列表</button>
    <button onclick="loadLog(20)" class="ghost">加载末尾20行</button>
    <span id="logMsg"></span>
  </div>
  <pre id="logView">选择一个日志文件查看...</pre>
</div>

<script>
function $(id){ return document.getElementById(id); }
function setMsg(id, text, ok){
  var el = $(id);
  el.textContent = text;
  el.className = text ? ('msg ' + (ok ? 'ok' : 'err')) : '';
}

// Tab 切换
document.querySelectorAll('.tab').forEach(function(t){
  t.onclick = function(){
    document.querySelectorAll('.tab').forEach(function(x){ x.classList.remove('active'); });
    document.querySelectorAll('.panel').forEach(function(x){ x.classList.remove('active'); });
    t.classList.add('active');
    $('panel-' + t.dataset.tab).classList.add('active');
    if (t.dataset.tab === 'logs') { refreshLogs(); }
  };
});

// ---- 配置 ----
function loadConfig(){
  setMsg('cfgMsg','',true);
  fetch('/chat/api/config').then(function(r){ return r.json(); }).then(function(d){
    if (d.error){ setMsg('cfgMsg', d.error, false); return; }
    $('cfgText').value = d.content;
    $('cfgPath').textContent = d.path;
  }).catch(function(e){ setMsg('cfgMsg', String(e), false); });
}
function saveConfig(){
  setMsg('cfgMsg','保存中...',true);
  fetch('/chat/api/config', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({content: $('cfgText').value})
  }).then(function(r){ return r.json(); }).then(function(d){
    if (d.error){ setMsg('cfgMsg', d.error, false); return; }
    setMsg('cfgMsg', d.message || '已保存', true);
  }).catch(function(e){ setMsg('cfgMsg', String(e), false); });
}

// ---- 服务控制 ----
function restart(){
  if (!confirm('确定要重启服务吗？重启期间接口会短暂不可用。')) return;
  setMsg('svcMsg','正在重启...',true);
  fetch('/chat/api/restart', {method:'POST'}).then(function(r){ return r.json(); }).then(function(d){
    if (d.error){ setMsg('svcMsg', d.error, false); return; }
    setMsg('svcMsg', (d.message || '重启中') + '（约数秒后自动检测恢复）', true);
    waitForRestart();
  }).catch(function(e){
    // 连接被断开也可能是正在重启
    setMsg('svcMsg', '重启请求已发出，等待服务恢复...', true);
    waitForRestart();
  });
}
function waitForRestart(){
  var tries = 0;
  var timer = setInterval(function(){
    tries++;
    fetch('/v1/health').then(function(r){
      if (r.ok){ clearInterval(timer); setMsg('svcMsg','服务已恢复', true); }
    }).catch(function(){});
    if (tries > 30){ clearInterval(timer); setMsg('svcMsg','等待超时，请手动刷新确认', false); }
  }, 1000);
}

// ---- 日志 ----
function fmtSize(n){
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n/1024).toFixed(1) + ' KB';
  return (n/1048576).toFixed(1) + ' MB';
}
function refreshLogs(){
  fetch('/chat/api/logs').then(function(r){ return r.json(); }).then(function(d){
    var sel = $('logSelect');
    var cur = sel.value;
    sel.innerHTML = '';
    if (!d.files || d.files.length === 0){
      var o = document.createElement('option');
      o.textContent = '（无日志文件）'; o.value = '';
      sel.appendChild(o);
      return;
    }
    d.files.forEach(function(f){
      var o = document.createElement('option');
      o.value = f.name;
      o.textContent = f.name + '  (' + fmtSize(f.size) + ')';
      sel.appendChild(o);
    });
    if (cur){ sel.value = cur; }
  }).catch(function(e){ setMsg('logMsg', String(e), false); });
}
function loadLog(lines){
  var name = $('logSelect').value;
  if (!name){ return; }
  setMsg('logMsg','加载中...',true);
  var url = '/chat/api/logs/content?name=' + encodeURIComponent(name);
  if (lines){ url += '&lines=' + lines; }
  fetch(url).then(function(r){ return r.json(); }).then(function(d){
    if (d.error){ setMsg('logMsg', d.error, false); return; }
    $('logView').textContent = d.lines.join('\n');
    setMsg('logMsg', (d.truncated ? '仅显示末尾 ' + d.total + ' 行' : '共 ' + d.total + ' 行'), true);
    var v = $('logView'); v.scrollTop = v.scrollHeight;
  }).catch(function(e){ setMsg('logMsg', String(e), false); });
}

// 初始化
loadConfig();
</script>
</body>
</html>
"""

