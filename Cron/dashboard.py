import asyncio
import json
import time
from typing import Optional, Set

from ErisPulse import sdk
from fastapi import Request, WebSocket
from fastapi.responses import JSONResponse


# ==================== SVG Icons ====================

_TASK_LIST_ICON = (
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">'
    '<line x1="8" y1="6" x2="21" y2="6"/>'
    '<line x1="8" y1="12" x2="21" y2="12"/>'
    '<line x1="8" y1="18" x2="21" y2="18"/>'
    '<line x1="3" y1="6" x2="3.01" y2="6"/>'
    '<line x1="3" y1="12" x2="3.01" y2="12"/>'
    '<line x1="3" y1="18" x2="3.01" y2="18"/>'
    '</svg>'
)

_CREATE_TASK_ICON = (
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">'
    '<circle cx="12" cy="12" r="10"/>'
    '<line x1="12" y1="8" x2="12" y2="16"/>'
    '<line x1="8" y1="12" x2="16" y2="12"/>'
    '</svg>'
)


# ==================== Task List View ====================

_TASK_LIST_HTML = '''
<h1 class="page-title">定时任务</h1>
<p class="cron-subtitle">管理和监控所有已注册的定时任务</p>

<div class="cron-toolbar">
    <div class="cron-filters">
        <button class="btn cron-filter-btn active" data-filter="all" onclick="CronList.filter('all')">全部</button>
        <button class="btn cron-filter-btn" data-filter="pending" onclick="CronList.filter('pending')">待执行</button>
        <button class="btn cron-filter-btn" data-filter="paused" onclick="CronList.filter('paused')">已暂停</button>
        <button class="btn cron-filter-btn" data-filter="completed" onclick="CronList.filter('completed')">已完成</button>
        <button class="btn cron-filter-btn" data-filter="cancelled" onclick="CronList.filter('cancelled')">已取消</button>
    </div>
    <div class="cron-toolbar-actions">
        <button class="btn btn-secondary" onclick="CronList.refresh()">刷新</button>
        <button class="btn btn-danger" onclick="CronList.cleanup()">清理已完成</button>
    </div>
</div>

<div class="card">
    <div class="card-body" style="padding:0">
        <table class="cron-table">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>类型</th>
                    <th>状态</th>
                    <th>标签</th>
                    <th>动作</th>
                    <th>下次执行</th>
                    <th>运行</th>
                    <th>来源</th>
                    <th>操作</th>
                </tr>
            </thead>
            <tbody id="cron-task-tbody">
                <tr><td colspan="9" class="cron-empty">加载中...</td></tr>
            </tbody>
        </table>
    </div>
</div>

<div id="cron-toast" class="cron-toast" style="display:none"></div>
'''

_TASK_LIST_CSS = '''
.cron-subtitle { color: var(--tx-s); margin-bottom: 16px; font-size: 14px; }
.cron-toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; flex-wrap: wrap; gap: 8px; }
.cron-filters { display: flex; gap: 4px; flex-wrap: wrap; }
.cron-filter-btn {
    padding: 6px 14px; font-size: 13px; border-radius: 6px;
    background: var(--bg-t); color: var(--tx-s); border: 1px solid var(--bd);
    cursor: pointer; transition: all 0.15s;
}
.cron-filter-btn:hover { background: var(--bg-s); color: var(--tx-p); }
.cron-filter-btn.active { background: var(--accent); color: #fff; border-color: var(--accent); }
.cron-toolbar-actions { display: flex; gap: 8px; }

.cron-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.cron-table th {
    text-align: left; padding: 10px 12px; font-weight: 600;
    color: var(--tx-s); border-bottom: 2px solid var(--bd);
    background: var(--bg-s); white-space: nowrap;
}
.cron-table td {
    padding: 8px 12px; border-bottom: 1px solid var(--bd);
    color: var(--tx-p); vertical-align: middle;
}
.cron-table tbody tr:hover { background: var(--bg-s); }
.cron-empty { text-align: center; color: var(--tx-t); padding: 32px 12px !important; }

.cron-badge {
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 11px; font-weight: 600; letter-spacing: 0.3px;
}
.cron-badge-pending { background: rgba(34,197,94,0.15); color: #22c55e; }
.cron-badge-paused { background: rgba(234,179,8,0.15); color: #eab308; }
.cron-badge-completed { background: rgba(59,130,246,0.15); color: #3b82f6; }
.cron-badge-cancelled { background: rgba(239,68,68,0.15); color: #ef4444; }
.cron-badge-once { background: rgba(168,85,247,0.15); color: #a855f7; }
.cron-badge-interval { background: rgba(6,182,212,0.15); color: #06b6d4; }
.cron-badge-cron { background: rgba(249,115,22,0.15); color: #f97316; }
.cron-badge-action { background: rgba(34,197,94,0.12); color: #22c55e; }
.cron-badge-action.shell { background: rgba(239,68,68,0.12); color: #ef4444; }
.cron-badge-action.python { background: rgba(59,130,246,0.12); color: #3b82f6; }
.cron-badge-action.http { background: rgba(168,85,247,0.12); color: #a855f7; }
.cron-badge-action.message { background: rgba(6,182,212,0.12); color: #06b6d4; }

.cron-actions { display: flex; gap: 4px; flex-wrap: nowrap; }
.cron-act-btn {
    width: 28px; height: 28px; border: none; border-radius: 4px; cursor: pointer;
    display: inline-flex; align-items: center; justify-content: center;
    font-size: 14px; transition: all 0.15s; background: var(--bg-t); color: var(--tx-s);
}
.cron-act-btn:hover { background: var(--accent); color: #fff; }
.cron-act-btn.danger:hover { background: #ef4444; color: #fff; }
.cron-act-btn.warn:hover { background: #eab308; color: #fff; }

.cron-toast {
    position: fixed; bottom: 24px; right: 24px; padding: 12px 20px;
    border-radius: 8px; font-size: 13px; z-index: 9999;
    background: var(--bg-t); color: var(--tx-p); border: 1px solid var(--bd);
    box-shadow: 0 4px 12px rgba(0,0,0,0.15); transition: opacity 0.3s;
}
.cron-toast.success { border-color: #22c55e; }
.cron-toast.error { border-color: #ef4444; }
'''

_TASK_LIST_JS = '''
var CronList = {
    tasks: [],
    currentFilter: 'all',
    ws: null,
    _timer: null,
    _reconnectDelay: 2000,

    init: function() {
        this.refresh();
        this.connectWs();
        var self = this;
        this._timer = setInterval(function() { self._updateCountdowns(); }, 1000);
    },

    destroy: function() {
        if (this._timer) { clearInterval(this._timer); this._timer = null; }
        if (this.ws) { try { this.ws.close(); } catch(e) {} this.ws = null; }
    },

    refresh: function() {
        var self = this;
        var url = '/Cron/api/tasks?status=';
        if (this.currentFilter !== 'all') url += this.currentFilter;
        this._fetch(url).then(function(data) {
            self.tasks = data.tasks || [];
            self.render();
        });
    },

    filter: function(status) {
        this.currentFilter = status;
        var btns = document.querySelectorAll('.cron-filter-btn');
        btns.forEach(function(b) { b.classList.toggle('active', b.getAttribute('data-filter') === status); });
        this.refresh();
    },

    render: function() {
        var tbody = document.getElementById('cron-task-tbody');
        if (!tbody) return;
        if (!this.tasks.length) {
            tbody.innerHTML = '<tr><td colspan="9" class="cron-empty">暂无任务</td></tr>';
            return;
        }
        var self = this;
        tbody.innerHTML = this.tasks.map(function(t) { return self._renderRow(t); }).join('');
    },

    _getActionLabel: function(t) {
        var cb = t.callback_data;
        if (!cb || typeof cb !== 'object' || !cb.__cron_action) return '-';
        var a = cb.__cron_action;
        if (a === 'shell') return '<span class="cron-badge-action shell">Shell</span>';
        if (a === 'python') return '<span class="cron-badge-action python">Python</span>';
        if (a === 'http') return '<span class="cron-badge-action http">HTTP</span>';
        if (a === 'message') {
            var target = (cb.platform || '?') + '/' + (cb.session_type || '?');
            return '<span class="cron-badge-action message">Msg</span>';
        }
        return '<span class="cron-badge-action">' + a + '</span>';
    },

    _renderRow: function(t) {
        var id = (t.id || '').substring(0, 8);
        var typeBadge = '<span class="cron-badge cron-badge-' + t.type + '">' + t.type + '</span>';
        var statusBadge = '<span class="cron-badge cron-badge-' + t.status + '">' + this._statusText(t.status) + '</span>';
        var label = this._esc(t.label || '-');
        var actionLabel = this._getActionLabel(t);
        var nextRun = this._formatNextRun(t);
        var runs = (t.run_count || 0) + '/' + (t.max_runs || '\\u221e');
        var source = this._esc(t.source || '');
        var actions = this._renderActions(t);

        return '<tr data-id="' + t.id + '">'
            + '<td style="font-family:monospace;font-size:12px">' + id + '</td>'
            + '<td>' + typeBadge + '</td>'
            + '<td>' + statusBadge + '</td>'
            + '<td>' + label + '</td>'
            + '<td>' + actionLabel + '</td>'
            + '<td class="cron-countdown" data-next-run="' + (t.next_run || 0) + '" data-status="' + t.status + '">' + nextRun + '</td>'
            + '<td style="white-space:nowrap">' + runs + '</td>'
            + '<td>' + source + '</td>'
            + '<td>' + actions + '</td>'
            + '</tr>';
    },

    _renderActions: function(t) {
        var s = t.status;
        var id = t.id;
        var html = '<div class="cron-actions">';
        if (s === 'pending') {
            html += '<button class="cron-act-btn warn" title="暂停" onclick="CronList.action(\\'' + id + '\\',\\'pause\\')">&#9208;</button>';
            html += '<button class="cron-act-btn" title="立即触发" onclick="CronList.action(\\'' + id + '\\',\\'trigger\\')">&#9889;</button>';
            html += '<button class="cron-act-btn danger" title="取消" onclick="CronList.action(\\'' + id + '\\',\\'cancel\\')">&#10005;</button>';
        } else if (s === 'paused') {
            html += '<button class="cron-act-btn" title="恢复" onclick="CronList.action(\\'' + id + '\\',\\'resume\\')">&#9654;</button>';
            html += '<button class="cron-act-btn danger" title="取消" onclick="CronList.action(\\'' + id + '\\',\\'cancel\\')">&#10005;</button>';
        }
        if (s === 'completed' || s === 'cancelled') {
            html += '<button class="cron-act-btn danger" title="删除" onclick="CronList.action(\\'' + id + '\\',\\'delete\\')">&#128465;</button>';
        }
        html += '</div>';
        return html;
    },

    action: function(taskId, act) {
        var self = this;
        this._post('/Cron/api/tasks/action', { task_id: taskId, action: act }).then(function(data) {
            if (data.ok) {
                self.toast(act + ' 操作成功', 'success');
                self.refresh();
            } else {
                self.toast(data.error || '操作失败', 'error');
            }
        }).catch(function(e) { self.toast('请求失败: ' + e, 'error'); });
    },

    cleanup: function() {
        var self = this;
        this._post('/Cron/api/tasks/cleanup', {}).then(function(data) {
            self.toast('已清理 ' + (data.cleaned || 0) + ' 条记录', 'success');
            self.refresh();
        }).catch(function(e) { self.toast('清理失败: ' + e, 'error'); });
    },

    connectWs: function() {
        var self = this;
        var token = localStorage.getItem('__ep_tk__') || '';
        var proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        var url = proto + '//' + window.location.host + '/Cron/ws?token=' + encodeURIComponent(token);
        try { this.ws = new WebSocket(url); } catch(e) { return; }
        this.ws.onopen = function() { self._reconnectDelay = 2000; };
        this.ws.onmessage = function(evt) {
            try { var msg = JSON.parse(evt.data); self._handleWs(msg); } catch(e) {}
        };
        this.ws.onclose = function() {
            setTimeout(function() { self.connectWs(); }, self._reconnectDelay);
            self._reconnectDelay = Math.min(self._reconnectDelay * 1.5, 15000);
        };
        this.ws.onerror = function() { try { self.ws.close(); } catch(e) {} };
    },

    _handleWs: function(msg) {
        var evt = msg.event;
        if (!evt) return;
        if (evt === 'tasks_cleaned') { this.refresh(); return; }
        var found = false;
        for (var i = 0; i < this.tasks.length; i++) {
            if (this.tasks[i].id === (msg.data && msg.data.id)) {
                if (evt === 'task_deleted') { this.tasks.splice(i, 1); }
                else { this.tasks[i] = msg.data; }
                found = true; break;
            }
        }
        if (!found && msg.data && msg.data.id && evt === 'task_created') {
            this.tasks.unshift(msg.data);
        }
        this.render();
    },

    _updateCountdowns: function() {
        var cells = document.querySelectorAll('.cron-countdown');
        var now = Date.now() / 1000;
        cells.forEach(function(td) {
            if (td.getAttribute('data-status') !== 'pending') return;
            var nr = parseFloat(td.getAttribute('data-next-run')) || 0;
            var diff = nr - now;
            td.textContent = diff <= 0 ? '即将执行' : CronList._formatDuration(diff);
        });
    },

    _formatNextRun: function(t) {
        if (t.status !== 'pending') return '-';
        var diff = (t.next_run || 0) - Date.now() / 1000;
        return diff <= 0 ? '即将执行' : this._formatDuration(diff);
    },

    _formatDuration: function(s) {
        if (s < 60) return Math.round(s) + 's';
        if (s < 3600) return Math.floor(s / 60) + 'm ' + Math.round(s % 60) + 's';
        if (s < 86400) return Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm';
        return Math.floor(s / 86400) + 'd ' + Math.floor((s % 86400) / 3600) + 'h';
    },

    _statusText: function(s) {
        return { pending: '待执行', paused: '已暂停', completed: '已完成', cancelled: '已取消' }[s] || s;
    },

    _esc: function(s) { var d = document.createElement('div'); d.textContent = s; return d.innerHTML; },

    toast: function(msg, type) {
        var el = document.getElementById('cron-toast');
        if (!el) return;
        el.textContent = msg; el.className = 'cron-toast ' + (type || '');
        el.style.display = 'block';
        clearTimeout(this._toastTimer);
        this._toastTimer = setTimeout(function() { el.style.display = 'none'; }, 3000);
    },

    _fetch: function(url) {
        return fetch(url, { headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('__ep_tk__') || '') } })
            .then(function(r) { return r.json(); });
    },

    _post: function(url, body) {
        return fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + (localStorage.getItem('__ep_tk__') || '') },
            body: JSON.stringify(body)
        }).then(function(r) { return r.json(); });
    }
};

function CronListInit() { CronList.init(); }
'''


# ==================== Create Task View ====================

_CREATE_TASK_HTML = '''
<h1 class="page-title">创建任务</h1>
<p class="cron-subtitle">创建定时任务并设置执行动作，任务将持久化保存</p>

<div class="card">
    <div class="card-header">调度配置</div>
    <div class="card-body">
        <div class="cron-form-group">
            <label class="cron-label">任务类型</label>
            <select id="cron-create-type" class="cron-input" onchange="CronCreate.onTypeChange()">
                <option value="once">一次性 (Once)</option>
                <option value="interval">间隔循环 (Interval)</option>
                <option value="cron">Cron 表达式</option>
            </select>
        </div>

        <div id="cron-field-once" class="cron-type-fields">
            <div class="cron-form-group">
                <label class="cron-label">延迟时间（秒）</label>
                <input type="number" id="cron-once-delay" class="cron-input" placeholder="例如: 60" min="1" value="60">
                <span class="cron-hint">任务将在指定秒数后执行一次</span>
            </div>
        </div>

        <div id="cron-field-interval" class="cron-type-fields" style="display:none">
            <div class="cron-form-group">
                <label class="cron-label">间隔时间（秒）</label>
                <input type="number" id="cron-interval-secs" class="cron-input" placeholder="例如: 300" min="1" value="300">
            </div>
            <div class="cron-form-group">
                <label class="cron-label">初始延迟（秒，可选）</label>
                <input type="number" id="cron-interval-delay" class="cron-input" placeholder="留空则立即开始" min="0">
            </div>
            <div class="cron-form-group">
                <label class="cron-label">最大执行次数（0=无限）</label>
                <input type="number" id="cron-interval-maxruns" class="cron-input" value="0" min="0">
            </div>
        </div>

        <div id="cron-field-cron" class="cron-type-fields" style="display:none">
            <div class="cron-form-group">
                <label class="cron-label">Cron 表达式</label>
                <input type="text" id="cron-cron-expr" class="cron-input" placeholder="*/5 * * * *" value="*/5 * * * *">
                <span class="cron-hint">格式: 分 时 日 月 星期 (例如: 0 8 * * * = 每天8:00)</span>
            </div>
            <div class="cron-form-group">
                <label class="cron-label">时区</label>
                <select id="cron-cron-tz" class="cron-input">
                    <option value="Asia/Shanghai">Asia/Shanghai (UTC+8)</option>
                    <option value="Asia/Tokyo">Asia/Tokyo (UTC+9)</option>
                    <option value="America/New_York">America/New_York (UTC-5)</option>
                    <option value="America/Los_Angeles">America/Los_Angeles (UTC-8)</option>
                    <option value="Europe/London">Europe/London (UTC+0)</option>
                    <option value="UTC">UTC</option>
                </select>
            </div>
            <div class="cron-form-group">
                <label class="cron-label">最大执行次数（0=无限）</label>
                <input type="number" id="cron-cron-maxruns" class="cron-input" value="0" min="0">
            </div>
        </div>
    </div>
</div>

<div class="card" style="margin-top:16px">
    <div class="card-header">执行动作</div>
    <div class="card-body">
        <div class="cron-form-group">
            <label class="cron-label">动作类型</label>
            <select id="cron-action-type" class="cron-input" onchange="CronCreate.onActionChange()">
                <option value="none">无（自定义回调数据）</option>
                <option value="shell">执行 Shell 命令</option>
                <option value="python">运行 Python 代码</option>
                <option value="http">发送 HTTP 请求</option>
                <option value="message">发送消息</option>
            </select>
        </div>

        <div id="cron-action-none" class="cron-action-fields">
            <div class="cron-form-group">
                <label class="cron-label">自定义回调数据（JSON）</label>
                <textarea id="cron-callback" class="cron-input cron-textarea" placeholder='{"key": "value"}' rows="3"></textarea>
                <span class="cron-hint">任务触发时将传递给处理函数的数据</span>
            </div>
        </div>

        <div id="cron-action-shell" class="cron-action-fields" style="display:none">
            <div class="cron-form-group">
                <label class="cron-label">Shell 命令</label>
                <textarea id="cron-shell-cmd" class="cron-input cron-textarea" placeholder="echo 'Hello from Cron'" rows="3"></textarea>
                <span class="cron-hint">任务触发时将执行此命令。命令在系统 shell 中异步运行。</span>
            </div>
        </div>

        <div id="cron-action-python" class="cron-action-fields" style="display:none">
            <div class="cron-form-group">
                <label class="cron-label">Python 代码</label>
                <textarea id="cron-python-code" class="cron-input cron-textarea cron-code" placeholder="import asyncio&#10;print('Hello from Cron')" rows="8"></textarea>
                <span class="cron-hint">任务触发时将执行此代码。可用变量: <code>sdk</code>, <code>asyncio</code>, <code>json</code>, <code>os</code>, <code>time</code></span>
            </div>
        </div>

        <div id="cron-action-http" class="cron-action-fields" style="display:none">
            <div class="cron-form-group">
                <label class="cron-label">请求方法</label>
                <select id="cron-http-method" class="cron-input">
                    <option value="GET">GET</option>
                    <option value="POST">POST</option>
                    <option value="PUT">PUT</option>
                    <option value="DELETE">DELETE</option>
                    <option value="PATCH">PATCH</option>
                </select>
            </div>
            <div class="cron-form-group">
                <label class="cron-label">URL</label>
                <input type="text" id="cron-http-url" class="cron-input" placeholder="https://example.com/api">
            </div>
            <div class="cron-form-group">
                <label class="cron-label">请求头（JSON，可选）</label>
                <textarea id="cron-http-headers" class="cron-input cron-textarea" placeholder='{"Content-Type": "application/json"}' rows="2"></textarea>
            </div>
            <div class="cron-form-group">
                <label class="cron-label">请求体（可选）</label>
                <textarea id="cron-http-body" class="cron-input cron-textarea" placeholder="JSON 或纯文本" rows="2"></textarea>
            </div>
        </div>

        <div id="cron-action-message" class="cron-action-fields" style="display:none">
            <div class="cron-form-group">
                <label class="cron-label">平台</label>
                <select id="cron-msg-platform" class="cron-input">
                    <option value="">-- 加载中 --</option>
                </select>
            </div>
            <div class="cron-form-group">
                <label class="cron-label">会话类型</label>
                <select id="cron-msg-session" class="cron-input">
                    <option value="user">私聊 (user)</option>
                    <option value="group">群组 (group)</option>
                    <option value="channel">频道 (channel)</option>
                    <option value="guild">服务器 (guild)</option>
                    <option value="thread">话题 (thread)</option>
                </select>
            </div>
            <div class="cron-form-group">
                <label class="cron-label">目标 ID</label>
                <input type="text" id="cron-msg-target" class="cron-input" placeholder="用户ID / 群组ID">
            </div>
            <div class="cron-form-group">
                <label class="cron-label">消息内容</label>
                <textarea id="cron-msg-content" class="cron-input cron-textarea" placeholder="要发送的消息文本" rows="3"></textarea>
            </div>
        </div>
    </div>
</div>

<div class="card" style="margin-top:16px">
    <div class="card-header">其他设置</div>
    <div class="card-body">
        <div class="cron-form-group">
            <label class="cron-label">标签（可选）</label>
            <input type="text" id="cron-label" class="cron-input" placeholder="给任务起个名字">
        </div>
        <div class="cron-form-group">
            <label class="cron-label">来源</label>
            <input type="text" id="cron-source" class="cron-input" value="Dashboard">
        </div>
        <div class="cron-form-group">
            <label class="cron-label">错过执行策略</label>
            <select id="cron-missed-policy" class="cron-input">
                <option value="fire_immediately">立即执行</option>
                <option value="skip">跳过</option>
                <option value="reschedule">重新调度</option>
            </select>
        </div>
        <div style="margin-top:20px">
            <button class="btn btn-primary" onclick="CronCreate.submit()">创建任务</button>
        </div>
    </div>
</div>

<div id="cron-create-toast" class="cron-toast" style="display:none"></div>
'''

_CREATE_TASK_CSS = '''
.cron-subtitle { color: var(--tx-s); margin-bottom: 16px; font-size: 14px; }
.cron-form-group { margin-bottom: 16px; }
.cron-label { display: block; margin-bottom: 6px; font-weight: 600; font-size: 13px; color: var(--tx-p); }
.cron-input {
    width: 100%; padding: 8px 12px; border-radius: 6px; font-size: 13px;
    background: var(--bg-s); color: var(--tx-p); border: 1px solid var(--bd);
    box-sizing: border-box; outline: none; transition: border-color 0.15s;
}
.cron-input:focus { border-color: var(--accent); }
.cron-textarea { resize: vertical; min-height: 60px; font-family: monospace; }
.cron-code { font-size: 12px; line-height: 1.5; tab-size: 4; }
select.cron-input { cursor: pointer; }
.cron-hint { display: block; margin-top: 4px; font-size: 11px; color: var(--tx-t); }
.cron-hint code { background: var(--bg-t); padding: 1px 4px; border-radius: 3px; font-size: 11px; }

.cron-toast {
    position: fixed; bottom: 24px; right: 24px; padding: 12px 20px;
    border-radius: 8px; font-size: 13px; z-index: 9999;
    background: var(--bg-t); color: var(--tx-p); border: 1px solid var(--bd);
    box-shadow: 0 4px 12px rgba(0,0,0,0.15); transition: opacity 0.3s;
}
.cron-toast.success { border-color: #22c55e; }
.cron-toast.error { border-color: #ef4444; }
'''

_CREATE_TASK_JS = '''
var CronCreate = {
    platforms: [],

    init: function() {
        this.onTypeChange();
        this.onActionChange();
        this._loadPlatforms();
    },

    _loadPlatforms: function() {
        var self = this;
        this._fetch('/Cron/api/platforms').then(function(data) {
            self.platforms = data.platforms || [];
            var sel = document.getElementById('cron-msg-platform');
            if (!sel) return;
            sel.innerHTML = '<option value="">-- 请选择平台 --</option>';
            self.platforms.forEach(function(p) {
                var opt = document.createElement('option');
                opt.value = p.name;
                opt.textContent = p.name + (p.running ? '' : ' (未运行)');
                sel.appendChild(opt);
            });
        }).catch(function() {});
    },

    onTypeChange: function() {
        var sel = document.getElementById('cron-create-type');
        if (!sel) return;
        var val = sel.value;
        var map = { once: 'cron-field-once', interval: 'cron-field-interval', cron: 'cron-field-cron' };
        for (var k in map) {
            var el = document.getElementById(map[k]);
            if (el) el.style.display = (k === val) ? 'block' : 'none';
        }
    },

    onActionChange: function() {
        var sel = document.getElementById('cron-action-type');
        if (!sel) return;
        var val = sel.value;
        var map = { none: 'cron-action-none', shell: 'cron-action-shell', python: 'cron-action-python', http: 'cron-action-http', message: 'cron-action-message' };
        for (var k in map) {
            var el = document.getElementById(map[k]);
            if (el) el.style.display = (k === val) ? 'block' : 'none';
        }
    },

    submit: function() {
        var type = document.getElementById('cron-create-type').value;
        var label = document.getElementById('cron-label').value.trim() || null;
        var source = document.getElementById('cron-source').value.trim() || 'Dashboard';
        var policy = document.getElementById('cron-missed-policy').value;
        var actionType = document.getElementById('cron-action-type').value;

        var callbackData = this._buildCallbackData(actionType);

        var body = { type: type, label: label, source: source, callback_data: callbackData, missed_policy: policy };

        if (type === 'once') {
            var delay = parseFloat(document.getElementById('cron-once-delay').value);
            if (!delay || delay <= 0) { this.toast('请输入有效的延迟秒数', 'error'); return; }
            body.delay = delay;
        } else if (type === 'interval') {
            var secs = parseFloat(document.getElementById('cron-interval-secs').value);
            if (!secs || secs <= 0) { this.toast('请输入有效的间隔秒数', 'error'); return; }
            body.interval_seconds = secs;
            var d = document.getElementById('cron-interval-delay').value;
            if (d) body.delay = parseFloat(d);
            body.max_runs = parseInt(document.getElementById('cron-interval-maxruns').value) || 0;
        } else if (type === 'cron') {
            var expr = document.getElementById('cron-cron-expr').value.trim();
            if (!expr) { this.toast('请输入 Cron 表达式', 'error'); return; }
            body.expression = expr;
            body.timezone = document.getElementById('cron-cron-tz').value;
            body.max_runs = parseInt(document.getElementById('cron-cron-maxruns').value) || 0;
        }

        var self = this;
        this._post('/Cron/api/tasks', body).then(function(data) {
            if (data.ok) {
                self.toast('任务创建成功! ID: ' + (data.task_id || '').substring(0, 8), 'success');
                self._resetForm();
            } else {
                self.toast('创建失败: ' + (data.error || '未知错误'), 'error');
            }
        }).catch(function(e) { self.toast('请求失败: ' + e, 'error'); });
    },

    _buildCallbackData: function(actionType) {
        if (actionType === 'none') {
            var raw = document.getElementById('cron-callback').value.trim();
            if (!raw) return null;
            try { return JSON.parse(raw); }
            catch(e) { return raw; }
        }
        if (actionType === 'shell') {
            var cmd = document.getElementById('cron-shell-cmd').value.trim();
            if (!cmd) { this.toast('请输入 Shell 命令', 'error'); return null; }
            return { __cron_action: 'shell', command: cmd };
        }
        if (actionType === 'python') {
            var code = document.getElementById('cron-python-code').value.trim();
            if (!code) { this.toast('请输入 Python 代码', 'error'); return null; }
            return { __cron_action: 'python', code: code };
        }
        if (actionType === 'http') {
            var url = document.getElementById('cron-http-url').value.trim();
            if (!url) { this.toast('请输入 URL', 'error'); return null; }
            var headers = {};
            var hRaw = document.getElementById('cron-http-headers').value.trim();
            if (hRaw) { try { headers = JSON.parse(hRaw); } catch(e) { this.toast('请求头 JSON 格式错误', 'error'); return null; } }
            var bRaw = document.getElementById('cron-http-body').value.trim();
            var httpBody = null;
            if (bRaw) { try { httpBody = JSON.parse(bRaw); } catch(e) { httpBody = bRaw; } }
            return {
                __cron_action: 'http',
                url: url,
                method: document.getElementById('cron-http-method').value,
                headers: headers,
                body: httpBody
            };
        }
        if (actionType === 'message') {
            var platform = document.getElementById('cron-msg-platform').value;
            var sessionType = document.getElementById('cron-msg-session').value;
            var target = document.getElementById('cron-msg-target').value.trim();
            var msg = document.getElementById('cron-msg-content').value.trim();
            if (!platform) { this.toast('请选择平台', 'error'); return null; }
            if (!target) { this.toast('请输入目标 ID', 'error'); return null; }
            if (!msg) { this.toast('请输入消息内容', 'error'); return null; }
            return {
                __cron_action: 'message',
                platform: platform,
                session_type: sessionType,
                target_id: target,
                message: msg
            };
        }
        return null;
    },

    _resetForm: function() {
        document.getElementById('cron-label').value = '';
        document.getElementById('cron-callback').value = '';
        document.getElementById('cron-shell-cmd').value = '';
        document.getElementById('cron-python-code').value = '';
        document.getElementById('cron-http-url').value = '';
        document.getElementById('cron-http-headers').value = '';
        document.getElementById('cron-http-body').value = '';
        document.getElementById('cron-msg-target').value = '';
        document.getElementById('cron-msg-content').value = '';
    },

    toast: function(msg, type) {
        var el = document.getElementById('cron-create-toast');
        if (!el) return;
        el.textContent = msg; el.className = 'cron-toast ' + (type || '');
        el.style.display = 'block';
        clearTimeout(this._toastTimer);
        this._toastTimer = setTimeout(function() { el.style.display = 'none'; }, 4000);
    },

    _fetch: function(url) {
        return fetch(url, { headers: { 'Authorization': 'Bearer ' + (localStorage.getItem('__ep_tk__') || '') } })
            .then(function(r) { return r.json(); });
    },

    _post: function(url, body) {
        return fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + (localStorage.getItem('__ep_tk__') || '') },
            body: JSON.stringify(body)
        }).then(function(r) { return r.json(); });
    }
};

function CronCreateInit() { CronCreate.init(); }
'''


# ==================== Dashboard Integration ====================

class DashboardIntegration:
    GROUP = "cron"
    GROUP_TITLE = "定时任务"
    GROUP_TITLE_EN = "Cron"

    def __init__(self, core):
        self._core = core
        self._ws_clients: Set[WebSocket] = set()
        self._trigger_handler = None
        self._registered = False

    # ==================== Lifecycle ====================

    def setup(self):
        try:
            if not (hasattr(sdk, 'Dashboard') and sdk.Dashboard):
                self._core.logger.debug("Dashboard not available, skipping integration")
                return
            self._register_routes()
            self._register_ws()
            self._register_views()
            self._register_trigger_handler()
            self._registered = True
            self._core.logger.info("Dashboard integration ready")
        except Exception as e:
            self._core.logger.warning(f"Dashboard setup failed: {e}")

    def teardown(self):
        if not self._registered:
            return
        try:
            self._unregister_routes()
            self._unregister_ws()
            self._unregister_views()
            self._unregister_trigger_handler()
        except Exception as e:
            self._core.logger.warning(f"Dashboard teardown error: {e}")
        self._registered = False

    # ==================== HTTP Routes ====================

    def _register_routes(self):
        r = sdk.router
        r.register_http_route("Cron", "/api/tasks", self._api_list_tasks, methods=["GET"])
        r.register_http_route("Cron", "/api/tasks", self._api_create_task, methods=["POST"])
        r.register_http_route("Cron", "/api/tasks/action", self._api_task_action, methods=["POST"])
        r.register_http_route("Cron", "/api/tasks/cleanup", self._api_cleanup, methods=["POST"])
        r.register_http_route("Cron", "/api/platforms", self._api_platforms, methods=["GET"])

    def _unregister_routes(self):
        r = sdk.router
        for path in ["/api/tasks", "/api/tasks/action", "/api/tasks/cleanup", "/api/platforms"]:
            try:
                r.unregister_http_route("Cron", path)
            except Exception:
                pass

    # ==================== WebSocket ====================

    def _register_ws(self):
        sdk.router.register_websocket(
            module_name="Cron",
            path="/ws",
            handler=self._ws_handler,
            auth_handler=self._ws_auth,
        )

    def _unregister_ws(self):
        try:
            sdk.router.unregister_websocket("Cron", "/ws")
        except Exception:
            pass

    async def _ws_auth(self, websocket: WebSocket) -> bool:
        token = websocket.query_params.get("token", "")
        return bool(token)

    async def _ws_handler(self, websocket: WebSocket):
        self._ws_clients.add(websocket)
        try:
            while True:
                data = await websocket.receive_text()
                try:
                    msg = json.loads(data)
                    if msg.get("type") == "ping":
                        await websocket.send_text(json.dumps({"type": "pong"}))
                except (json.JSONDecodeError, TypeError):
                    pass
        except Exception:
            pass
        finally:
            self._ws_clients.discard(websocket)

    async def _broadcast(self, event: str, data: dict):
        if not self._ws_clients:
            return
        msg = json.dumps({"event": event, "data": data}, ensure_ascii=False, default=str)
        disconnected = set()
        for ws in list(self._ws_clients):
            try:
                await ws.send_text(msg)
            except Exception:
                disconnected.add(ws)
        self._ws_clients -= disconnected

    # ==================== Views ====================

    def _register_views(self):
        db = sdk.Dashboard

        db.register_view(
            id="CronTasks",
            title="任务列表", title_en="Task List",
            icon_svg=_TASK_LIST_ICON,
            html_content=_TASK_LIST_HTML,
            js_content=_TASK_LIST_JS,
            css_content=_TASK_LIST_CSS,
            loader="CronListInit",
            group=self.GROUP,
            group_title=self.GROUP_TITLE,
            group_title_en=self.GROUP_TITLE_EN,
        )

        db.register_view(
            id="CronCreate",
            title="创建任务", title_en="Create Task",
            icon_svg=_CREATE_TASK_ICON,
            html_content=_CREATE_TASK_HTML,
            js_content=_CREATE_TASK_JS,
            css_content=_CREATE_TASK_CSS,
            loader="CronCreateInit",
            group=self.GROUP,
            group_title=self.GROUP_TITLE,
            group_title_en=self.GROUP_TITLE_EN,
        )

    def _unregister_views(self):
        try:
            sdk.Dashboard.unregister_view("CronTasks")
        except Exception:
            pass
        try:
            sdk.Dashboard.unregister_view("CronCreate")
        except Exception:
            pass

    # ==================== Trigger Handler ====================

    def _register_trigger_handler(self):
        async def _on_trigger(info):
            await self._broadcast("task_triggered", info)

        self._trigger_handler = _on_trigger
        self._core.on_trigger(_on_trigger)

    def _unregister_trigger_handler(self):
        if self._trigger_handler:
            self._core.off_trigger(self._trigger_handler)
            self._trigger_handler = None

    # ==================== API Handlers ====================

    async def _api_list_tasks(self, request: Request):
        status = request.query_params.get("status")
        task_type = request.query_params.get("type")
        tasks = self._core.list_tasks(status=status or None, task_type=task_type or None)
        now = time.time()
        for t in tasks:
            t["remaining"] = max(0, (t.get("next_run") or 0) - now)
        return JSONResponse({"tasks": tasks})

    async def _api_create_task(self, request: Request):
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, status_code=400)

        task_type = body.get("type")
        if not task_type or task_type not in ("once", "interval", "cron"):
            return JSONResponse({"error": "Invalid or missing 'type' (once/interval/cron)"}, status_code=400)

        try:
            task_id = self._create_task_from_body(body)
            task = self._core.get_task(task_id)
            await self._broadcast("task_created", task)
            return JSONResponse({"ok": True, "task_id": task_id, "task": task})
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=400)

    def _create_task_from_body(self, body: dict) -> str:
        task_type = body["type"]
        label = body.get("label") or None
        source = body.get("source", "Dashboard") or "Dashboard"
        callback_data = body.get("callback_data")
        missed_policy = body.get("missed_policy", "fire_immediately")

        if task_type == "once":
            delay = body.get("delay")
            trigger_at = body.get("trigger_at")
            return self._core.once(
                delay=float(delay) if delay is not None else None,
                trigger_at=float(trigger_at) if trigger_at is not None else None,
                callback_data=callback_data,
                label=label,
                source=source,
                missed_policy=missed_policy,
            )
        elif task_type == "interval":
            return self._core.interval(
                interval_seconds=float(body["interval_seconds"]),
                callback_data=callback_data,
                delay=float(body["delay"]) if body.get("delay") else None,
                max_runs=int(body.get("max_runs", 0)),
                label=label,
                source=source,
                missed_policy=missed_policy,
            )
        elif task_type == "cron":
            return self._core.cron(
                expression=body["expression"],
                callback_data=callback_data,
                timezone=body.get("timezone", "Asia/Shanghai"),
                max_runs=int(body.get("max_runs", 0)),
                label=label,
                source=source,
                missed_policy=missed_policy,
            )

    async def _api_task_action(self, request: Request):
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON"}, status_code=400)

        task_id = body.get("task_id")
        action = body.get("action")
        if not task_id or not action:
            return JSONResponse({"error": "Missing task_id or action"}, status_code=400)

        task = self._core.get_task(task_id)
        if task is None:
            return JSONResponse({"error": "Task not found"}, status_code=404)

        try:
            if action == "pause":
                ok = self._core.pause(task_id)
            elif action == "resume":
                ok = self._core.resume(task_id)
            elif action == "cancel":
                ok = self._core.cancel(task_id)
            elif action == "trigger":
                result = await self._core.trigger_now(task_id)
                if result is None:
                    return JSONResponse({"error": "Trigger failed"}, status_code=400)
                return JSONResponse({"ok": True, "trigger_info": result})
            elif action == "delete":
                ok = self._core.delete_task(task_id)
                if ok:
                    await self._broadcast("task_deleted", {"task_id": task_id})
                return JSONResponse({"ok": ok})
            else:
                return JSONResponse({"error": f"Unknown action: {action}"}, status_code=400)

            updated_task = self._core.get_task(task_id)
            event_map = {
                "pause": "task_paused",
                "resume": "task_resumed",
                "cancel": "task_cancelled",
            }
            await self._broadcast(event_map.get(action, "task_updated"), updated_task)
            return JSONResponse({"ok": ok, "task": updated_task})
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

    async def _api_cleanup(self, request: Request):
        count = self._core.cleanup()
        await self._broadcast("tasks_cleaned", {"count": count})
        return JSONResponse({"ok": True, "cleaned": count})

    async def _api_platforms(self, request: Request):
        platforms = []
        for name in sdk.adapter.list_registered():
            info = {
                "name": name,
                "running": sdk.adapter.is_running(name),
            }
            try:
                info["send_methods"] = sdk.adapter.list_sends(name)
            except Exception:
                info["send_methods"] = []
            platforms.append(info)
        return JSONResponse({"platforms": platforms})
