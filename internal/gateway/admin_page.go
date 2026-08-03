package gateway

// adminPageHTML 是 /chat 管理页的内联 HTML（无外部依赖，纯原生 JS）。
const adminPageHTML = `<!DOCTYPE html>
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
`
