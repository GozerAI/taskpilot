/* TaskPilot Dashboard */
(function () {
  'use strict';

  var API = '';
  var token = sessionStorage.getItem('tp_token') || null;
  var userEmail = sessionStorage.getItem('tp_email') || '';
  var currentPage = 'tasks';

  function toast(msg, type) {
    type = type || 'info';
    var c = document.getElementById('toast-container');
    var el = document.createElement('div');
    el.className = 'toast toast-' + type;
    el.textContent = msg;
    c.appendChild(el);
    setTimeout(function() { el.remove(); }, 4000);
  }

  function apiFetch(method, path, body) {
    var opts = { method: method, headers: { 'Content-Type': 'application/json' } };
    if (token) opts.headers['Authorization'] = 'Bearer ' + token;
    if (body) opts.body = JSON.stringify(body);
    return fetch(API + path, opts).then(function(res) {
      if (res.status === 204) return null;
      return res.json().catch(function() { return {}; }).then(function(data) {
        if (!res.ok) throw new Error(data.detail || data.message || ('Error ' + res.status));
        return data;
      });
    });
  }

  function cap(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1) : ''; }

  function escHtml(s) {
    if (!s) return '';
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function statusBadge(status) {
    var s = (status || 'pending').toLowerCase();
    return '<span class="badge badge-' + s + '"><span class="badge-dot"></span>' + cap(s) + '</span>';
  }

  function priorityBadge(p) {
    p = (p || 'normal').toLowerCase();
    return '<span class="badge badge-' + p + '">' + cap(p) + '</span>';
  }

  function fmtDate(iso) {
    if (!iso) return '-';
    try { return new Date(iso).toLocaleString(); } catch(e) { return iso; }
  }

  function fmtRel(iso) {
    if (!iso) return '-';
    try {
      var s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
      if (s < 60) return s + 's ago';
      if (s < 3600) return Math.floor(s/60) + 'm ago';
      if (s < 86400) return Math.floor(s/3600) + 'h ago';
      return Math.floor(s/86400) + 'd ago';
    } catch(e) { return iso; }
  }

  function statCard(val, lbl) {
    return '<div class="card"><div class="stat-value">' + val + '</div><div class="stat-label">' + lbl + '</div></div>';
  }

  function emptyState(title, desc) {
    return '<div class="empty-state"><div class="empty-state-icon">&#128274;</div>' +
      '<div class="empty-state-title">' + title + '</div>' +
      '<div class="empty-state-desc">' + desc + '</div></div>';
  }

  function showErr(el, msg) { if (el) { el.textContent = msg; el.classList.remove('hidden'); } }

  function handleLogin(e) {
    e.preventDefault();
    var form = e.target;
    var errEl = document.getElementById('login-error');
    errEl.classList.add('hidden');
    var btn = form.querySelector('button[type=submit]');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Signing in...';
    apiFetch('POST', '/api/auth/login', {
      email: form.email.value.trim(), password: form.password.value
    }).then(function(data) {
      token = data.access_token;
      userEmail = form.email.value.trim();
      sessionStorage.setItem('tp_token', token);
      sessionStorage.setItem('tp_email', userEmail);
      showApp();
    }).catch(function(err) {
      showErr(errEl, err.message);
    }).finally(function() { btn.disabled = false; btn.textContent = 'Sign in'; });
  }

  function handleRegister(e) {
    e.preventDefault();
    var form = e.target;
    var errEl = document.getElementById('register-error');
    errEl.classList.add('hidden');
    var btn = form.querySelector('button[type=submit]');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Creating account...';
    apiFetch('POST', '/api/auth/register', {
      name: form.reg_name.value.trim(),
      email: form.reg_email.value.trim(),
      password: form.reg_password.value
    }).then(function() {
      toast('Account created. Please sign in.', 'success');
      switchAuthTab('login');
    }).catch(function(err) {
      showErr(errEl, err.message);
    }).finally(function() { btn.disabled = false; btn.textContent = 'Create account'; });
  }

  function logout() {
    token = null; userEmail = '';
    sessionStorage.removeItem('tp_token'); sessionStorage.removeItem('tp_email');
    showAuth();
  }

  function showAuth() {
    document.getElementById('auth-view').classList.remove('hidden');
    document.getElementById('app-view').classList.add('hidden');
  }

  function showApp() {
    document.getElementById('auth-view').classList.add('hidden');
    document.getElementById('app-view').classList.remove('hidden');
    document.getElementById('user-email-display').textContent = userEmail;
    document.getElementById('user-avatar-display').textContent = (userEmail[0] || 'U').toUpperCase();
    navigateTo(currentPage);
  }

  function switchAuthTab(tab) {
    document.querySelectorAll('.auth-tab').forEach(function(t) { t.classList.remove('active'); });
    document.querySelector('[data-tab="' + tab + '"]').classList.add('active');
    document.getElementById('login-form-wrap').classList.toggle('hidden', tab !== 'login');
    document.getElementById('register-form-wrap').classList.toggle('hidden', tab !== 'register');
  }

  function navigateTo(page) {
    currentPage = page;
    document.querySelectorAll('.nav-item').forEach(function(el) {
      el.classList.toggle('active', el.dataset.page === page);
    });
    var content = document.getElementById('page-content');
    content.innerHTML = '<div class="page-loading"><span class="spinner"></span> Loading...</div>';
    loadPage(page, content);
  }

  function loadPage(page, container) {
    var p;
    if (page === 'tasks') p = renderTasks(container);
    else if (page === 'schedules') p = renderSchedules(container);
    else if (page === 'workflows') p = renderWorkflows(container);
    else if (page === 'history') p = renderHistory(container);
    else if (page === 'settings') p = renderSettings(container);
    else if (page === 'billing') { renderBilling(container); return; }
    else { container.innerHTML = emptyState('Page not found', ''); return; }
    if (p && p.catch) {
      p.catch(function(err) {
        container.innerHTML = '<div class="page-body"><p style="color:var(--error)">' + escHtml(err.message) + '</p></div>';
      });
    }
  }

  /* --- Tasks Page --- */
  function renderTasks(container) {
    return Promise.all([
      apiFetch('GET', '/v1/tasks').catch(function() { return []; }),
      apiFetch('GET', '/v1/stats').catch(function() { return {}; })
    ]).then(function(results) {
      var tasks = results[0] || [];
      var stats = results[1] || {};
      var qs = (stats.engine && stats.engine.queue_stats) || {};
      var total = qs.total_tasks || tasks.length || 0;
      var running = qs.running_tasks || 0;
      var scheduled = tasks.filter(function(t) { return t.status === 'scheduled'; }).length;
      var failed = tasks.filter(function(t) { return t.status === 'failed'; }).length;

      var html = '<div class="page-header"><div><h1 class="page-title">Tasks</h1>' +
        '<div class="page-subtitle">Scheduled task runs</div></div>' +
        '<button class="btn btn-primary" style="width:auto" data-testid="new-task-btn" onclick="TP.showCreateTaskModal()">+ New Task</button></div>' +
        '<div class="page-body"><div class="stats-grid">' +
        statCard(total, 'Total Tasks') + statCard(running, 'Running') +
        statCard(scheduled, 'Scheduled') + statCard(failed, 'Failed') + '</div>';

      if (!tasks.length) {
        html += emptyState('No tasks scheduled', 'Add your first task to automate repetitive work.');
      } else {
        html += '<div class="table-wrap"><table><thead><tr>' +
          '<th>Name</th><th>Status</th><th>Priority</th><th>Frequency</th>' +
          '<th>Next Run</th><th>Runs</th><th>Actions</th>' +
          '</tr></thead><tbody>';
        tasks.forEach(function(t) {
          var freq = t.schedule ? t.schedule.frequency : '-';
          html += '<tr><td><strong>' + escHtml(t.name) + '</strong>';
          if (t.description) html += '<br><small style="color:var(--text-muted)">' + escHtml(t.description) + '</small>';
          html += '</td><td>' + statusBadge(t.status) + '</td>' +
            '<td>' + priorityBadge(t.priority) + '</td>' +
            '<td>' + cap(freq || 'daily') + '</td>' +
            '<td style="white-space:nowrap">' + fmtDate(t.next_run) + '</td>' +
            '<td>' + (t.execution_count || 0) + '</td>' +
            '<td style="white-space:nowrap">' +
            '<button class="btn btn-ghost btn-sm" data-testid="run-task-btn" onclick="TP.execTask(event,'' + t.id + '')">Run</button> ' +
            '<button class="btn btn-ghost btn-sm" style="color:var(--error)" onclick="TP.delTask(event,'' + t.id + '')">Delete</button>' +
            '</td></tr>';
        });
        html += '</tbody></table></div>';
      }
      html += '</div>';
      container.innerHTML = html;
    });
  }

  /* --- Schedules Page --- */
  function renderSchedules(container) {
    return apiFetch('GET', '/v1/tasks').catch(function() { return []; }).then(function(tasks) {
      var html = '<div class="page-header"><div><h1 class="page-title">Schedules</h1>' +
        '<div class="page-subtitle">Task schedule configuration</div></div></div>' +
        '<div class="page-body">';

      if (!tasks.length) {
        html += emptyState('No schedules configured', 'Tasks you create will appear here with their schedule details.');
      } else {
        html += '<div class="table-wrap"><table><thead><tr>' +
          '<th>Task</th><th>Frequency</th><th>Time (UTC)</th><th>Next Run</th><th>Last Run</th><th>Enabled</th>' +
          '</tr></thead><tbody>';
        tasks.forEach(function(t) {
          var s = t.schedule || {};
          var timeStr = s.time_of_day || (s.hour !== undefined ? s.hour + ':' + String(s.minute || 0).padStart(2,'0') : '-');
          var lastRun = t.last_execution ? t.last_execution.completed_at : null;
          html += '<tr>' +
            '<td><strong>' + escHtml(t.name) + '</strong></td>' +
            '<td>' + cap(s.frequency || 'daily') + '</td>' +
            '<td>' + escHtml(timeStr) + '</td>' +
            '<td style="white-space:nowrap">' + fmtDate(t.next_run) + '</td>' +
            '<td style="white-space:nowrap">' + fmtDate(lastRun) + '</td>' +
            '<td><span class="badge ' + (t.enabled ? 'badge-completed' : 'badge-paused') + '">' +
            (t.enabled ? 'Active' : 'Paused') + '</span></td></tr>';
        });
        html += '</tbody></table></div>';
      }
      html += '</div>';
      container.innerHTML = html;
    });
  }

  /* --- Workflows Page --- */
  function renderWorkflows(container) {
    return apiFetch('GET', '/v1/workflows').catch(function() { return []; }).then(function(workflows) {
      var html = '<div class="page-header"><div><h1 class="page-title">Workflows</h1>' +
        '<div class="page-subtitle">Multi-step workflow definitions</div></div></div>' +
        '<div class="page-body">';

      if (!workflows.length) {
        html += emptyState('No workflows defined', 'Workflows let you chain tasks together. Requires Pro plan.');
      } else {
        html += '<div class="table-wrap"><table><thead><tr>' +
          '<th>Name</th><th>Owner</th><th>Steps</th><th>Parallel</th><th>Created</th><th>Actions</th>' +
          '</tr></thead><tbody>';
        workflows.forEach(function(w) {
          html += '<tr>' +
            '<td><strong>' + escHtml(w.name) + '</strong>';
          if (w.description) html += '<br><small style="color:var(--text-muted)">' + escHtml(w.description) + '</small>';
          html += '</td>' +
            '<td>' + escHtml(w.owner_executive || '-') + '</td>' +
            '<td>' + ((w.steps && w.steps.length) || 0) + '</td>' +
            '<td>' + (w.parallel_execution ? 'Yes' : 'No') + '</td>' +
            '<td style="white-space:nowrap">' + fmtDate(w.created_at) + '</td>' +
            '<td><button class="btn btn-ghost btn-sm" data-testid="run-workflow-btn" onclick="TP.execWorkflow(event,'' + w.id + '')">Execute</button></td>' +
            '</tr>';
        });
        html += '</tbody></table></div>';
      }
      html += '</div>';
      container.innerHTML = html;
    });
  }

  /* --- History Page --- */
  function renderHistory(container) {
    return apiFetch('GET', '/v1/tasks').catch(function() { return []; }).then(function(tasks) {
      var executions = [];
      tasks.forEach(function(t) {
        if (t.last_execution) {
          executions.push({
            name: t.name,
            task_id: t.id,
            status: t.last_execution.status,
            started_at: t.last_execution.started_at,
            completed_at: t.last_execution.completed_at,
            result: t.last_execution.result
          });
        }
      });
      executions.sort(function(a,b) {
        return (new Date(b.completed_at || b.started_at)) - (new Date(a.completed_at || a.started_at));
      });

      var html = '<div class="page-header"><div><h1 class="page-title">History</h1>' +
        '<div class="page-subtitle">Recent execution log</div></div></div>' +
        '<div class="page-body">';

      if (!executions.length) {
        html += emptyState('No execution history', 'Run tasks to see their execution results here.');
      } else {
        html += '<div class="card" style="padding:0">';
        executions.forEach(function(ex) {
          var success = ex.result ? ex.result.success : (ex.status === 'completed');
          var dotClass = success ? 'history-dot-success' : (ex.status === 'running' ? 'history-dot-pending' : 'history-dot-failure');
          var outcome = success ? 'Succeeded' : (ex.status === 'running' ? 'Running' : 'Failed');
          var duration = (ex.result && ex.result.duration_seconds) ? (ex.result.duration_seconds.toFixed(2) + 's') : '';
          html += '<div class="history-entry">' +
            '<div class="history-dot ' + dotClass + '"></div>' +
            '<div class="history-info">' +
            '<div class="history-name">' + escHtml(ex.name) + '</div>' +
            '<div class="history-meta">' + outcome + (duration ? ' &middot; ' + duration : '') + '</div>' +
            '</div>' +
            '<div class="history-time">' + fmtRel(ex.completed_at || ex.started_at) + '</div>' +
            '</div>';
        });
        html += '</div>';
      }
      html += '</div>';
      container.innerHTML = html;
    });
  }

  /* --- Settings Page --- */
  function renderSettings(container) {
    return apiFetch('GET', '/health/detailed').catch(function() { return {}; }).then(function(health) {
      var html = '<div class="page-header"><div><h1 class="page-title">Settings</h1>' +
        '<div class="page-subtitle">System configuration and status</div></div></div>' +
        '<div class="page-body">';

      html += '<div class="settings-section"><div class="settings-section-title">Service Status</div>';
      var checks = health.checks || {};
      html += '<div class="settings-row"><span class="settings-key">API Health</span><span class="settings-val">' +
        statusBadge(health.status || 'unknown') + '</span></div>';
      html += '<div class="settings-row"><span class="settings-key">Scheduler</span><span class="settings-val">' +
        ((checks.service && checks.service.scheduler_running) ? '<span style="color:var(--success)">Running</span>' : '<span style="color:var(--text-muted)">Stopped</span>') + '</span></div>';
      html += '<div class="settings-row"><span class="settings-key">Registered Handlers</span><span class="settings-val">' +
        ((checks.service && checks.service.registered_handlers) || 0) + '</span></div>';
      html += '<div class="settings-row"><span class="settings-key">Telemetry</span><span class="settings-val">' +
        ((checks.telemetry && checks.telemetry.status) || 'unavailable') + '</span></div>';
      html += '</div>';

      html += '<div class="settings-section"><div class="settings-section-title">Account</div>';
      html += '<div class="settings-row"><span class="settings-key">Logged in as</span><span class="settings-val">' + escHtml(userEmail) + '</span></div>';
      html += '<div class="settings-row"><span class="settings-key">Auth token</span><span class="settings-val">' + (token ? token.substring(0,20) + '...' : 'None') + '</span></div>';
      html += '<div class="settings-row"><span class="settings-key">Session</span><span class="settings-val"><button class="btn btn-ghost btn-sm" onclick="TP.logout()">Sign out</button></span></div>';
      html += '</div>';
      html += '</div>';
      container.innerHTML = html;
    });
  }

  /* --- Billing Page --- */
  function renderBilling(container) {
    var plans = [
      { name: 'Community', price: '$0', cycle: 'forever', current: false, tag: null,
        features: ['Task scheduling API', 'Basic workflow listing', 'Health monitoring', 'API authentication'] },
      { name: 'Pro', price: '$19', cycle: '/month', current: true, tag: 'Founder pricing',
        features: ['Everything in Community', 'Advanced workflows', 'Autonomous cycles', '50 tasks', 'Email support'] },
      { name: 'Growth', price: '$49', cycle: '/month', current: false, tag: null,
        features: ['Everything in Pro', 'Executive reports', 'Unlimited tasks', 'Priority support', 'SLA'] }
    ];
    var html = '<div class="page-header"><div><h1 class="page-title">Billing</h1>' +
      '<div class="page-subtitle">Plans and subscription</div></div></div><div class="page-body">';
    html += '<p style="color:var(--text-muted);margin-bottom:1.5rem">Upgrade to unlock advanced features.</p>';
    html += '<div class="billing-grid">';
    plans.forEach(function(plan) {
      html += '<div class="plan-card' + (plan.current ? ' current' : '') + '">';
      html += '<div class="plan-name">' + plan.name + (plan.current ? '<span class="plan-tag">Current</span>' : '') + '</div>';
      html += '<div class="plan-price">' + plan.price + '<span>' + plan.cycle + '</span></div>';
      if (plan.tag) html += '<div style="font-size:.75rem;color:var(--warning)">' + plan.tag + ' (reg $29/mo)</div>';
      html += '<ul class="plan-features">';
      plan.features.forEach(function(ft) { html += '<li><span class="plan-check">&#10003;</span>' + ft + '</li>'; });
      html += '</ul>';
      if (!plan.current) {
        html += '<a href="https://gozerai.com/pricing" target="_blank" class="btn btn-primary" style="margin-top:auto;text-decoration:none">Upgrade</a>';
      } else {
        html += '<div class="btn btn-ghost" style="text-align:center;margin-top:auto;cursor:default">Active</div>';
      }
      html += '</div>';
    });
    html += '</div></div>';
    container.innerHTML = html;
  }

  /* --- Create Task Modal --- */
  function showCreateTaskModal() {
    var backdrop = document.createElement('div');
    backdrop.className = 'modal-backdrop';
    backdrop.id = 'create-task-modal';
    var modalHtml = '<div class="modal" role="dialog" aria-modal="true" aria-labelledby="modal-h">' +
      '<h2 class="modal-title" id="modal-h">New Scheduled Task</h2>' +
      '<form id="create-task-form">' +
      '<div class="form-group"><label class="form-label" for="ct-name">Task name *</label>' +
      '<input class="form-input" id="ct-name" type="text" required placeholder="Daily report"></div>' +
      '<div class="form-group"><label class="form-label" for="ct-handler">Handler name *</label>' +
      '<input class="form-input" id="ct-handler" type="text" required placeholder="my_module.run"></div>' +
      '<div class="form-group"><label class="form-label" for="ct-desc">Description</label>' +
      '<input class="form-input" id="ct-desc" type="text" placeholder="Optional"></div>' +
      '<div class="form-row">' +
      '<div class="form-group"><label class="form-label" for="ct-freq">Frequency</label>' +
      '<select class="form-input" id="ct-freq">' +
      '<option value="daily">Daily</option><option value="hourly">Hourly</option>' +
      '<option value="weekly">Weekly</option><option value="monthly">Monthly</option>' +
      '<option value="on_demand">On Demand</option></select></div>' +
      '<div class="form-group"><label class="form-label" for="ct-pri">Priority</label>' +
      '<select class="form-input" id="ct-pri">' +
      '<option value="normal">Normal</option><option value="low">Low</option>' +
      '<option value="high">High</option><option value="critical">Critical</option>' +
      '</select></div></div>' +
      '<div class="form-row">' +
      '<div class="form-group"><label class="form-label" for="ct-hour">Hour (0-23)</label>' +
      '<input class="form-input" id="ct-hour" type="number" min="0" max="23" value="0"></div>' +
      '<div class="form-group"><label class="form-label" for="ct-min">Minute (0-59)</label>' +
      '<input class="form-input" id="ct-min" type="number" min="0" max="59" value="0"></div>' +
      '</div>' +
      '<div id="ct-error" class="inline-error hidden"></div>' +
      '<div class="modal-footer">' +
      '<button type="button" class="btn btn-ghost" onclick="TP.closeModal()">Cancel</button>' +
      '<button type="submit" class="btn btn-primary" style="width:auto" data-testid="create-task-submit">Create Task</button>' +
      '</div></form></div>';
    backdrop.innerHTML = modalHtml;
    document.body.appendChild(backdrop);
    backdrop.querySelector('#ct-name').focus();
    backdrop.addEventListener('click', function(e) { if (e.target === backdrop) TP.closeModal(); });
    document.getElementById('create-task-form').addEventListener('submit', function(e) {
      e.preventDefault();
      var errEl = document.getElementById('ct-error');
      errEl.classList.add('hidden');
      var btn = e.target.querySelector('button[type=submit]');
      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span>';
      apiFetch('POST', '/v1/tasks', {
        name: document.getElementById('ct-name').value.trim(),
        handler_name: document.getElementById('ct-handler').value.trim(),
        description: document.getElementById('ct-desc').value.trim() || null,
        frequency: document.getElementById('ct-freq').value,
        priority: document.getElementById('ct-pri').value,
        hour: parseInt(document.getElementById('ct-hour').value) || 0,
        minute: parseInt(document.getElementById('ct-min').value) || 0,
        enabled: true
      }).then(function() {
        toast('Task created', 'success');
        TP.closeModal();
        navigateTo('tasks');
      }).catch(function(err) {
        showErr(errEl, err.message);
        btn.disabled = false;
        btn.textContent = 'Create Task';
      });
    });
  }

  function closeModal() {
    var m = document.getElementById('create-task-modal');
    if (m) m.remove();
  }

  function execTask(e, taskId) {
    var btn = e.currentTarget;
    var orig = btn.textContent;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>';
    apiFetch('POST', '/v1/tasks/' + taskId + '/execute')
      .then(function() { toast('Task triggered', 'success'); navigateTo('tasks'); })
      .catch(function(err) { toast(err.message, 'error'); })
      .finally(function() { btn.disabled = false; btn.textContent = orig; });
  }

  function delTask(e, taskId) {
    if (!confirm('Delete this task?')) return;
    apiFetch('DELETE', '/v1/tasks/' + taskId)
      .then(function() { toast('Task deleted', 'success'); navigateTo('tasks'); })
      .catch(function(err) { toast(err.message, 'error'); });
  }

  function execWorkflow(e, wfId) {
    var btn = e.currentTarget;
    var orig = btn.textContent;
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>';
    apiFetch('POST', '/v1/workflows/' + wfId + '/execute')
      .then(function() { toast('Workflow started', 'success'); navigateTo('workflows'); })
      .catch(function(err) { toast(err.message, 'error'); })
      .finally(function() { btn.disabled = false; btn.textContent = orig; });
  }

  /* --- Public API --- */
  var TP = {
    showCreateTaskModal: showCreateTaskModal,
    closeModal: closeModal,
    execTask: execTask,
    delTask: delTask,
    execWorkflow: execWorkflow,
    logout: logout
  };
  window.TP = TP;

  /* --- Init --- */
  function init() {
    // Auth tabs
    document.querySelectorAll('.auth-tab').forEach(function(tab) {
      tab.addEventListener('click', function() { switchAuthTab(tab.dataset.tab); });
    });

    // Login form
    var loginForm = document.getElementById('login-form');
    if (loginForm) loginForm.addEventListener('submit', handleLogin);

    // Register form
    var regForm = document.getElementById('register-form');
    if (regForm) regForm.addEventListener('submit', handleRegister);

    // Nav items
    document.querySelectorAll('.nav-item').forEach(function(item) {
      item.addEventListener('click', function() { navigateTo(item.dataset.page); });
    });

    // Logout button
    var logoutBtn = document.getElementById('logout-btn');
    if (logoutBtn) logoutBtn.addEventListener('click', logout);

    // Keyboard nav for modals
    document.addEventListener('keydown', function(e) {
      if (e.key === 'Escape') closeModal();
    });

    // Check existing session
    if (token) {
      showApp();
    } else {
      showAuth();
    }
  }

  document.addEventListener('DOMContentLoaded', init);

})();
