import { api, analyzeMongoStream } from './api.js';
import { esc, confirmModal, errorBanner, fmtInt, fmtDate, fmtDuration, redactMongoURI } from './ui.js';

const app = document.getElementById('app');
let cleanup = null;

function setCleanup(fn) { cleanup = fn; }
function navigate(hash) { location.hash = hash; }

// Session-lifetime cache of each project's last analyze() result, keyed by
// project id. Revisiting a project's tables screen (hash navigation, browser
// back/forward) shows the last known result instantly instead of re-running
// analyze — a full page reload naturally clears it. "Re-analyze" always
// bypasses this and refreshes it.
const tablesCache = new Map();

const PG_TYPES = ['text', 'varchar(255)', 'integer', 'bigint', 'smallint', 'boolean', 'numeric',
  'real', 'double precision', 'date', 'timestamp', 'timestamptz', 'jsonb', 'uuid', 'bytea'];
const MYSQL_TYPES = ['text', 'varchar(255)', 'int', 'bigint', 'smallint', 'tinyint(1)',
  'decimal(10,2)', 'float', 'double', 'date', 'datetime', 'timestamp', 'json', 'blob', 'char(36)'];
const SQLITE_TYPES = ['TEXT', 'INTEGER', 'REAL', 'BLOB', 'NUMERIC'];
const STRATEGIES = ['direct', 'as_jsonb', 'flatten', 'skip'];
const ACTIVE_STATUSES = ['pending', 'in_progress', 'paused'];

function typesForDialect(type) {
  if (type === 'mysql') return MYSQL_TYPES;
  if (type === 'sqlite') return SQLITE_TYPES;
  return PG_TYPES;
}

function findActiveMigration(migs) {
  return (migs || []).find((m) => ACTIVE_STATUSES.includes(m.status));
}

// ---------------------------------------------------------------------------
// Connection config editor (shared by create + edit forms)
// ---------------------------------------------------------------------------

function connTypeOptions(isTarget, selected) {
  const types = isTarget ? ['postgres', 'mysql', 'sqlite'] : ['postgres', 'mysql', 'sqlite', 'mongodb'];
  return types.map((t) => `<option value="${t}" ${t === selected ? 'selected' : ''}>${t}</option>`).join('');
}

function shortTail(s) {
  if (!s) return '';
  return s.length > 16 ? '…' + s.slice(-16) : s;
}

function subFieldsHTML(prefix, type, cfg) {
  cfg = cfg || {};
  if (type === 'sqlite') {
    return `
      <label>File path</label>
      <input type="text" id="${prefix}-dsn" placeholder="/path/to/database.db" value="${esc(cfg.dsn || '')}">
    `;
  }
  if (type === 'mongodb') {
    const hasDSN = !!cfg.dsn;
    return `
      <label>Connection URI</label>
      <input type="text" id="${prefix}-dsn" placeholder="${hasDSN ? 'leave blank to keep current connection string' : 'mongodb://user:pass@host:27017/dbname'}">
      ${hasDSN ? `<div class="hint">Currently ends in "${esc(shortTail(cfg.dsn))}". Leave blank to keep it.</div>` : ''}
    `;
  }
  const hasPassword = !!cfg.password;
  return `
    <div class="field-row">
      <div><label>Host</label><input type="text" id="${prefix}-host" value="${esc(cfg.host || '')}" placeholder="localhost"></div>
      <div><label>Port</label><input type="number" id="${prefix}-port" value="${cfg.port || ''}" placeholder="${type === 'mysql' ? '3306' : '5432'}"></div>
    </div>
    <div class="field-row">
      <div><label>Database</label><input type="text" id="${prefix}-database" value="${esc(cfg.database || '')}"></div>
      <div><label>Username</label><input type="text" id="${prefix}-username" value="${esc(cfg.username || '')}"></div>
    </div>
    <div class="field-row">
      <div>
        <label>Password</label>
        <input type="password" id="${prefix}-password" placeholder="${hasPassword ? 'leave blank to keep current password' : ''}">
      </div>
      ${type === 'postgres' ? `
      <div>
        <label>SSL mode</label>
        <select id="${prefix}-sslmode">
          ${['', 'disable', 'require', 'prefer', 'verify-ca', 'verify-full']
            .map((m) => `<option value="${m}" ${cfg.ssl_mode === m ? 'selected' : ''}>${m || '(default)'}</option>`).join('')}
        </select>
      </div>` : '<div></div>'}
    </div>
  `;
}

// Mounts a connection-config editor into `container`. Attaches a .read()
// method to the container element that returns the current ConnectionConfig.
function mountConnEditor(container, prefix, cfg, isTarget) {
  cfg = cfg || {};
  const type = cfg.type || 'postgres';
  container.innerHTML = `
    <label>Type</label>
    <select id="${prefix}-type">${connTypeOptions(isTarget, type)}</select>
    <div id="${prefix}-sub">${subFieldsHTML(prefix, type, cfg)}</div>
  `;
  const typeSel = container.querySelector(`#${prefix}-type`);
  typeSel.value = type;
  typeSel.addEventListener('change', () => {
    container.querySelector(`#${prefix}-sub`).innerHTML = subFieldsHTML(prefix, typeSel.value, {});
  });
  container.read = () => {
    const t = typeSel.value;
    const g = (id) => {
      const el = container.querySelector('#' + id);
      return el ? el.value : '';
    };
    if (t === 'sqlite' || t === 'mongodb') {
      return { type: t, dsn: g(`${prefix}-dsn`).trim(), host: '', port: 0, database: '', username: '', password: '', ssl_mode: '' };
    }
    return {
      type: t,
      host: g(`${prefix}-host`).trim(),
      port: parseInt(g(`${prefix}-port`), 10) || 0,
      database: g(`${prefix}-database`).trim(),
      username: g(`${prefix}-username`).trim(),
      password: g(`${prefix}-password`),
      ssl_mode: g(`${prefix}-sslmode`) || '',
      dsn: '',
    };
  };
}

// ---------------------------------------------------------------------------
// Projects list
// ---------------------------------------------------------------------------

async function renderProjectsList() {
  document.title = 'XferDB — Projects';
  app.innerHTML = `
    <div class="page-header">
      <div><h1>Projects</h1><div class="sub">Migration projects on this server</div></div>
      <button class="btn primary" id="newProjectBtn">+ New Project</button>
    </div>
    <div id="list" class="loading-block"><span class="spinner"></span> Loading projects…</div>
  `;
  document.getElementById('newProjectBtn').addEventListener('click', () => navigate('#/projects/new'));
  await loadProjectsList();
}

async function loadProjectsList() {
  const list = document.getElementById('list');
  let refreshInterval = null;

  try {
    const projects = await api.listProjects();
    if (!projects || projects.length === 0) {
      list.innerHTML = `
        <div class="empty-state">
          <div class="icon">📦</div>
          <p>No projects yet.</p>
          <button class="btn primary" id="emptyNewBtn">Create your first project</button>
        </div>`;
      document.getElementById('emptyNewBtn').addEventListener('click', () => navigate('#/projects/new'));
      return;
    }
    list.innerHTML = projects.map((p) => `
      <div class="list-row" data-id="${esc(p.id)}">
        <div style="flex:1;text-align:left;">
          <div class="name">${esc(p.name)}</div>
          <div class="meta">${esc(p.source_config.type)} → ${esc(p.target_config.type)}${p.description ? ' · ' + esc(p.description) : ''} · created ${esc(fmtDate(p.created_at))}</div>
          <div class="mig-progress" data-proj="${esc(p.id)}" style="display:none;margin-top:6px;"></div>
        </div>
        <div class="right"><button class="btn small danger" data-del="${esc(p.id)}">Delete</button></div>
      </div>
    `).join('');

    // Function to update migration progress for all projects
    async function updateMigrationProgress() {
      for (const p of projects) {
        try {
          const migs = await api.listMigrations(p.id);
          const active = migs.find((m) => m.status === 'pending' || m.status === 'in_progress' || m.status === 'paused');
          const progressEl = list.querySelector(`.mig-progress[data-proj="${p.id}"]`);
          if (!progressEl) continue;

          if (active) {
            progressEl.style.display = 'block';
            try {
              const snap = await api.getStats(active.id);
              const pct = snap.Rows.Total > 0 ? Math.round(snap.Rows.Transferred / snap.Rows.Total * 100) : 0;
              const statusLabel = active.status === 'in_progress' ? 'migrating' : active.status;
              progressEl.innerHTML = `
                <div style="display:flex;align-items:center;gap:8px;">
                  <span class="badge ${esc(active.status)}">${esc(statusLabel)}</span>
                  <div class="progress-bar" style="flex:1;max-width:150px;"><div class="fill${pct >= 100 ? ' done' : ''}" style="width:${pct}%"></div></div>
                  <span style="font-size:12px;color:var(--muted);">${pct}%</span>
                </div>`;
            } catch {
              progressEl.innerHTML = `<span class="badge ${esc(active.status)}">${esc(active.status)}</span>`;
            }
          } else {
            // Check if last migration completed
            const lastMig = migs.length > 0 ? migs[0] : null;
            if (lastMig && (lastMig.status === 'completed' || lastMig.status === 'failed' || lastMig.status === 'cancelled')) {
              progressEl.style.display = 'block';
              progressEl.innerHTML = `<span class="badge ${esc(lastMig.status)}">${esc(lastMig.status)}</span>`;
            } else {
              progressEl.style.display = 'none';
            }
          }
        } catch { /* ignore */ }
      }
    }

    // Initial update
    updateMigrationProgress();
    // Refresh every 3 seconds
    refreshInterval = setInterval(updateMigrationProgress, 3000);

    list.querySelectorAll('.list-row').forEach((row) => {
      row.addEventListener('click', (e) => {
        if (e.target.closest('[data-del]')) return;
        navigate(`#/projects/${encodeURIComponent(row.dataset.id)}`);
      });
    });
    list.querySelectorAll('[data-del]').forEach((btn) => {
      btn.addEventListener('click', async (e) => {
        e.stopPropagation();
        const id = btn.dataset.del;
        const proj = projects.find((p) => p.id === id);
        const ok = await confirmModal({
          title: 'Delete project?',
          body: `This deletes "${proj ? proj.name : id}" and all of its migration history. This cannot be undone.`,
          confirmLabel: 'Delete',
          danger: true,
        });
        if (!ok) return;
        try {
          await api.deleteProject(id);
          await loadProjectsList();
        } catch (err) {
          list.insertAdjacentHTML('afterbegin', errorBanner('Delete failed: ' + err.message));
        }
      });
    });

    // Set cleanup function to stop the refresh interval
    setCleanup(() => {
      if (refreshInterval) clearInterval(refreshInterval);
    });
  } catch (err) {
    list.innerHTML = errorBanner('Could not load projects: ' + err.message, { retry: loadProjectsList });
  }
}

// ---------------------------------------------------------------------------
// Create project
// ---------------------------------------------------------------------------

async function renderProjectCreate() {
  document.title = 'XferDB — New Project';
  app.innerHTML = `
    <div class="breadcrumb"><a href="#/">Projects</a> / New</div>
    <div class="page-header"><h1>New Project</h1></div>
    <label>Name</label>
    <input type="text" id="p-name" placeholder="prod-to-staging">
    <label>Description</label>
    <input type="text" id="p-desc" placeholder="optional">
    <fieldset><legend>Source</legend><div id="srcEditor"></div></fieldset>
    <fieldset><legend>Target</legend><div id="tgtEditor"></div></fieldset>
    <details class="advanced">
      <summary>Advanced transfer options</summary>
      <div class="field-row">
        <div><label>Batch size</label><input type="number" id="p-batch" value="1000"><div class="hint">Rows per INSERT batch</div></div>
        <div><label>Table workers</label><input type="number" id="p-workers" value="1"><div class="hint">Parallel tables to migrate</div></div>
      </div>
      <div class="field-row">
        <div id="p-segment-row"><label>Segment workers</label><input type="number" id="p-segment" value="0"><div class="hint" id="p-segment-hint">Workers per table (0 = disabled)</div></div>
        <div></div>
      </div>
    </details>
    <div id="createErr"></div>
    <div class="actions-row">
      <button class="btn primary" id="createBtn">Create Project</button>
      <a class="btn ghost" href="#/">Cancel</a>
    </div>
  `;
  const srcEditor = document.getElementById('srcEditor');
  const tgtEditor = document.getElementById('tgtEditor');
  mountConnEditor(srcEditor, 'src', { type: 'postgres' }, false);
  mountConnEditor(tgtEditor, 'tgt', { type: 'postgres' }, true);

  // Disable segment workers for MongoDB sources
  function updateSegmentState() {
    const srcType = document.getElementById('src-type').value;
    const segmentInput = document.getElementById('p-segment');
    const segmentHint = document.getElementById('p-segment-hint');
    if (srcType === 'mongodb') {
      segmentInput.disabled = true;
      segmentInput.value = '0';
      segmentHint.textContent = 'Not supported for MongoDB sources';
    } else {
      segmentInput.disabled = false;
      segmentHint.textContent = 'Workers per table (0 = disabled)';
    }
  }
  document.getElementById('src-type').addEventListener('change', updateSegmentState);

  document.getElementById('createBtn').addEventListener('click', async () => {
    const name = document.getElementById('p-name').value.trim();
    const errEl = document.getElementById('createErr');
    errEl.innerHTML = '';
    if (!name) {
      errEl.innerHTML = errorBanner('Name is required.');
      return;
    }
    const body = {
      name,
      description: document.getElementById('p-desc').value.trim(),
      source_config: srcEditor.read(),
      target_config: tgtEditor.read(),
      transfer_config: {
        batch_size: parseInt(document.getElementById('p-batch').value, 10) || 1000,
        table_workers: parseInt(document.getElementById('p-workers').value, 10) || 1,
        segment_workers: parseInt(document.getElementById('p-segment').value, 10) || 0,
        on_error: 'abort',
      },
    };
    const btn = document.getElementById('createBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Creating…';
    try {
      const proj = await api.createProject(body);
      navigate(`#/projects/${encodeURIComponent(proj.id)}`);
    } catch (err) {
      errEl.innerHTML = errorBanner('Could not create project: ' + err.message);
      btn.disabled = false;
      btn.textContent = 'Create Project';
    }
  });
}

// ---------------------------------------------------------------------------
// Project detail
// ---------------------------------------------------------------------------

function connSummaryHTML(cfg) {
  const rows = [];
  rows.push(['Type', cfg.type]);
  if (cfg.type === 'sqlite') {
    rows.push(['File', cfg.dsn || cfg.database || '']);
  } else if (cfg.type === 'mongodb') {
    rows.push(['Database', cfg.database || '(from URI)']);
    rows.push(['Connection URI', redactMongoURI(cfg.dsn)]);
  } else {
    if (cfg.host) rows.push(['Host', cfg.host]);
    if (cfg.port) rows.push(['Port', cfg.port]);
    if (cfg.database) rows.push(['Database', cfg.database]);
    if (cfg.username) rows.push(['Username', cfg.username]);
    rows.push(['Password', cfg.password ? '•'.repeat(10) : '(none set)']);
    if (cfg.ssl_mode) rows.push(['SSL mode', cfg.ssl_mode]);
  }
  return `<table class="kv">${rows.map(([k, v]) => `<tr><td>${esc(k)}</td><td>${esc(v)}</td></tr>`).join('')}</table>`;
}

function renderConnView(project) {
  document.getElementById('connView').innerHTML = `
    <div><h3>Source</h3>${connSummaryHTML(project.source_config)}</div>
    <div><h3>Target</h3>${connSummaryHTML(project.target_config)}</div>
  `;
}

function toggleEditConn(project) {
  const area = document.getElementById('editConnArea');
  if (area.dataset.open === '1') {
    area.innerHTML = '';
    area.dataset.open = '0';
    return;
  }
  area.dataset.open = '1';
  area.innerHTML = `
    <fieldset>
      <legend>Edit connection</legend>
      <label>Name</label>
      <input type="text" id="e-name" value="${esc(project.name)}">
      <label>Description</label>
      <input type="text" id="e-desc" value="${esc(project.description || '')}">
      <div class="field-row">
        <div><h3>Source</h3><div id="e-src"></div></div>
        <div><h3>Target</h3><div id="e-tgt"></div></div>
      </div>
      <div id="editErr"></div>
      <div class="actions-row">
        <button class="btn primary" id="saveConnBtn">Save &amp; Retest</button>
        <button class="btn ghost" id="cancelConnBtn">Cancel</button>
      </div>
    </fieldset>
  `;
  const srcEditor = document.getElementById('e-src');
  const tgtEditor = document.getElementById('e-tgt');
  mountConnEditor(srcEditor, 'esrc', project.source_config, false);
  mountConnEditor(tgtEditor, 'etgt', project.target_config, true);

  document.getElementById('cancelConnBtn').addEventListener('click', () => {
    area.innerHTML = '';
    area.dataset.open = '0';
  });
  document.getElementById('saveConnBtn').addEventListener('click', async () => {
    const name = document.getElementById('e-name').value.trim();
    const errEl = document.getElementById('editErr');
    errEl.innerHTML = '';
    if (!name) {
      errEl.innerHTML = errorBanner('Name is required.');
      return;
    }
    const body = {
      name,
      description: document.getElementById('e-desc').value.trim(),
      source_config: srcEditor.read(),
      target_config: tgtEditor.read(),
    };
    const btn = document.getElementById('saveConnBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Saving…';
    try {
      const updated = await api.updateProject(project.id, body);
      area.innerHTML = '';
      area.dataset.open = '0';
      document.querySelector('main h1').textContent = updated.name;
      project.name = updated.name;
      project.description = updated.description;
      project.source_config = updated.source_config;
      project.target_config = updated.target_config;
      renderConnView(project);
      await runPreflight(project.id);
    } catch (err) {
      errEl.innerHTML = errorBanner('Save failed: ' + err.message);
      btn.disabled = false;
      btn.textContent = 'Save & Retest';
    }
  });
}

function permBlock(label, info, perm) {
  const errs = (perm && perm.errors) || [];
  return `
    <div class="card">
      <strong>${esc(label)}</strong>
      ${info ? `
        <table class="kv">
          <tr><td>Type</td><td>${esc(info.type)} ${esc(info.version)}</td></tr>
          ${info.host ? `<tr><td>Host</td><td>${esc(info.host)}</td></tr>` : ''}
          ${info.database ? `<tr><td>Database</td><td>${esc(info.database)}</td></tr>` : ''}
          <tr><td>Tables</td><td>${esc(info.tables)}</td></tr>
          ${info.size_human ? `<tr><td>Size</td><td>${esc(info.size_human)}</td></tr>` : ''}
          ${info.ssl ? `<tr><td>SSL</td><td>${esc(info.ssl)}</td></tr>` : ''}
        </table>` : '<div class="hint">No connection info available.</div>'}
      ${perm ? `<div class="hint">read: ${perm.can_read ? '✓' : '✗'} · write: ${perm.can_write ? '✓' : '✗'} · create table: ${perm.can_create_table ? '✓' : '✗'}</div>` : ''}
      ${errs.map((e) => `<div class="banner error"><span>${esc(e)}</span></div>`).join('')}
    </div>
  `;
}

function renderPreflightResult(result) {
  return `
    <div class="banner ${result.status === 'ready' ? 'ok' : 'error'}">
      <span>Status: <span class="badge ${result.status === 'ready' ? 'ok' : 'fail'}">${esc(result.status)}</span></span>
    </div>
    <div class="split">
      ${permBlock('Source', result.source_info, result.source)}
      ${permBlock('Target', result.target_info, result.target)}
    </div>
  `;
}

async function runPreflight(id) {
  const area = document.getElementById('preflightArea');
  if (!area) return;
  area.innerHTML = `<div class="loading-block"><span class="spinner"></span> Testing connection…</div>`;
  try {
    const result = await api.preflight(id);
    area.innerHTML = renderPreflightResult(result);
  } catch (err) {
    area.innerHTML = errorBanner('Preflight failed: ' + err.message, { retry: () => runPreflight(id) });
  }
}

async function renderProjectSettings(id) {
  document.title = 'XferDB — Project Settings';
  app.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading project…</div>`;
  let project;
  try {
    project = await api.getProject(id);
  } catch (err) {
    app.innerHTML = `
      <div class="breadcrumb"><a href="#/">Projects</a></div>
      ${errorBanner('Project not found or could not be loaded: ' + err.message)}
      <a class="btn" href="#/">Back to projects</a>
    `;
    return;
  }
  document.title = `XferDB — ${project.name} settings`;

  app.innerHTML = `
    <div class="breadcrumb">
      <a href="#/">Projects</a> / <a href="#/projects/${encodeURIComponent(id)}">${esc(project.name)}</a> / Settings
    </div>
    <div class="page-header">
      <div><h1>${esc(project.name)}</h1><div class="sub">${esc(project.description || '')}</div></div>
      <div class="actions-row">
        <a class="btn" href="#/projects/${encodeURIComponent(id)}">← Back to tables</a>
        <button class="btn danger" id="deleteProjBtn">Delete Project</button>
      </div>
    </div>
    <div class="section">
      <h2>Connection</h2>
      <div id="connBanner"></div>
      <div class="split" id="connView"></div>
      <div class="actions-row">
        <button class="btn" id="editConnBtn">Edit Connection</button>
        <button class="btn" id="retryPreflightBtn">Test Connection</button>
      </div>
      <div id="editConnArea" data-open="0"></div>
      <div id="preflightArea"></div>
    </div>
    <div class="section">
      <h2>Schema Plan</h2>
      <p>The schema plan stores auto-detected column types and any manual overrides you've made.</p>
      <div id="resetAllArea"></div>
      <button class="btn danger" id="resetAllBtn">Reset All Schema Plans</button>
    </div>
    <div class="section">
      <h2>Support &amp; Diagnostics</h2>
      <p>Download a support bundle containing project configuration, migration history, and schema information for troubleshooting. Credentials are automatically redacted.</p>
      <button class="btn" id="downloadBundleBtn">Download Support Bundle</button>
    </div>
  `;

  document.getElementById('deleteProjBtn').addEventListener('click', async () => {
    const ok = await confirmModal({
      title: 'Delete project?',
      body: `This deletes "${project.name}" and all of its migration history. This cannot be undone.`,
      confirmLabel: 'Delete',
      danger: true,
    });
    if (!ok) return;
    try {
      await api.deleteProject(id);
      navigate('#/');
    } catch (err) {
      document.getElementById('connBanner').innerHTML = errorBanner('Delete failed: ' + err.message);
    }
  });

  document.getElementById('resetAllBtn').addEventListener('click', async () => {
    const ok = await confirmModal({
      title: 'Reset all schema plans?',
      body: 'This removes all schema plans and overrides for every table/collection in this project. You can re-analyze to regenerate them.',
      confirmLabel: 'Reset All',
      danger: true,
    });
    if (!ok) return;
    const btn = document.getElementById('resetAllBtn');
    const area = document.getElementById('resetAllArea');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Resetting…';
    try {
      await api.resetSchemaPlan(id, null);
      area.innerHTML = `<div class="banner ok"><span>All schema plans have been reset.</span></div>`;
      btn.disabled = false;
      btn.textContent = 'Reset All Schema Plans';
    } catch (err) {
      area.innerHTML = errorBanner('Reset failed: ' + err.message);
      btn.disabled = false;
      btn.textContent = 'Reset All Schema Plans';
    }
  });

  document.getElementById('downloadBundleBtn').addEventListener('click', async () => {
    const btn = document.getElementById('downloadBundleBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Generating…';
    try {
      // Trigger download via hidden link
      const url = `/api/v1/projects/${encodeURIComponent(id)}/support-bundle`;
      const a = document.createElement('a');
      a.href = url;
      a.download = '';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      btn.disabled = false;
      btn.textContent = 'Download Support Bundle';
    } catch (err) {
      btn.disabled = false;
      btn.textContent = 'Download Support Bundle';
    }
  });

  renderConnView(project);
  document.getElementById('editConnBtn').addEventListener('click', () => toggleEditConn(project));
  document.getElementById('retryPreflightBtn').addEventListener('click', () => runPreflight(id));

  await runPreflight(id);
}

// ---------------------------------------------------------------------------
// Tables / collections list
// ---------------------------------------------------------------------------

function renderTableCards(area, projectId, items, locked) {
  if (items.length === 0) {
    area.innerHTML = `<div class="empty-state"><div class="icon">📂</div><p>No tables found.</p></div>`;
    return;
  }
  area.innerHTML = items.map((t) => `
    <div class="list-row${locked ? ' disabled' : ''}" data-name="${esc(t.name)}">
      <div class="name mono">${esc(t.name)}</div>
      <span class="badge ${t.badge}">${esc(t.badgeText)}</span>
    </div>
  `).join('');
  if (locked) return;
  area.querySelectorAll('.list-row').forEach((row) => {
    row.addEventListener('click', () => navigate(`#/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(row.dataset.name)}`));
  });
}

// startProgressTicker renders a spinner + a label into container and keeps
// the label refreshed on its own timer (independent of any server events),
// so the display visibly moves even during a long gap between them —
// mirroring the CLI's spinner, which ticks every 100ms regardless of new
// NDJSON events. Without this, a stretch of no new events (connecting,
// scanning a large collection, a slow relational diff) reads as a hang.
// Returns a stop function; call it once the operation settles.
function startProgressTicker(container, labelFn) {
  container.innerHTML = `<div class="loading-block"><span class="spinner"></span> <span class="ticker-text"></span></div>`;
  const textEl = container.querySelector('.ticker-text');
  const render = () => { textEl.textContent = labelFn(); };
  render();
  const timer = setInterval(render, 400);
  return () => clearInterval(timer);
}

// loadTables runs analyze and returns the row items for renderTableCards;
// it throws on failure so the caller can drive the loading/error UI itself
// (needed so a later lock-state change can re-render from cached items
// without re-running analyze).
async function loadTables(id, project, analyzeOpts = {}) {
  const area = document.getElementById('tablesArea');
  const startedAt = Date.now();
  const elapsed = () => fmtDuration(Math.floor((Date.now() - startedAt) / 1000));

  if (project.source_config.type === 'mongodb') {
    let current = null; // { collection, scanned, target }
    const stopTicker = startProgressTicker(area, () => {
      if (!current) return `Listing collections… ${elapsed()} elapsed`;
      if (current.scanned === 0) return `Counting ${current.collection}… ${elapsed()} elapsed`;
      return `Sampling ${current.collection}: ${fmtInt(current.scanned)} / ${fmtInt(current.target)} — ${elapsed()} elapsed`;
    });
    const collections = [];
    try {
      await analyzeMongoStream(id, analyzeOpts, (evt) => {
        if (evt.type === 'progress') {
          current = { collection: evt.collection, scanned: evt.scanned, target: evt.target };
        } else if (evt.type === 'collection') {
          const fields = (evt.schema && evt.schema.fields) || [];
          const docCount = (evt.schema && evt.schema.estimated_count) || 0;
          collections.push({ name: evt.collection, fieldCount: fields.length, docCount, warnings: evt.warnings || [] });
          current = null;
        } else if (evt.type === 'error') {
          throw new Error(evt.error);
        }
      });
    } finally {
      stopTicker();
    }
    return collections.map((c) => ({
      name: c.name,
      badge: c.warnings.length ? 'warn' : 'ok',
      badgeText: c.warnings.length ? 'warning' : `${fmtInt(c.docCount)} docs · ${c.fieldCount} fields`,
    }));
  }

  const stopTicker = startProgressTicker(area, () => `Analyzing — ${elapsed()} elapsed`);
  let result;
  try {
    result = await api.analyze(id, analyzeOpts);
  } finally {
    stopTicker();
  }
  const tables = result.Tables || [];
  return tables.map((t) => ({
    name: t.Name,
    badge: t.Status === 'compatible' ? 'ok' : 'warn',
    badgeText: `${t.Status}${t.Issues && t.Issues.length ? ' · ' + t.Issues.length + ' issue(s)' : ''}`,
  }));
}

async function renderTablesList(id) {
  document.title = 'XferDB — Tables';
  app.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading project…</div>`;
  let project;
  try {
    project = await api.getProject(id);
  } catch (err) {
    app.innerHTML = errorBanner('Project not found: ' + err.message) + `<a class="btn" href="#/">Back to projects</a>`;
    return;
  }
  const label = project.source_config.type === 'mongodb' ? 'Collections' : 'Tables';
  const isMongo = project.source_config.type === 'mongodb';
  document.title = `XferDB — ${project.name}`;
  app.innerHTML = `
    <div class="breadcrumb"><a href="#/">Projects</a> / ${esc(project.name)}</div>
    <div class="page-header">
      <div><h1>${esc(project.name)}</h1><div class="sub">${esc(project.description || '')}</div></div>
      <a class="btn" href="#/projects/${encodeURIComponent(id)}/settings">Settings</a>
    </div>
    <div class="section" id="migCardSection"></div>
    <div class="section">
      <div class="page-header"><h2>${label}</h2><button class="btn" id="reanalyzeBtn">Re-analyze</button></div>
      ${isMongo ? `
      <details class="advanced" id="analyzeOptions">
        <summary>Sampling options</summary>
        <div class="field-row">
          <div><label>Sample size</label><input type="number" id="opt-sampleSize" value="2000"><div class="hint">Docs to sample for small collections</div></div>
          <div><label>Sample %</label><input type="number" id="opt-samplePct" value="1" step="0.1"><div class="hint">% of docs to sample for large collections</div></div>
        </div>
        <div class="field-row">
          <div><label>Threshold</label><input type="number" id="opt-sampleThreshold" value="100000"><div class="hint">Doc count to switch from size to %</div></div>
          <div></div>
        </div>
        <div class="checkbox-row">
          <input type="checkbox" id="opt-ai" disabled><label for="opt-ai">Enable AI annotations</label><span class="hint">(coming soon)</span>
        </div>
        <div class="checkbox-row">
          <input type="checkbox" id="opt-accurateCounts"><label for="opt-accurateCounts">Accurate counts</label><span class="hint">Use exact counts instead of estimates (slower)</span>
        </div>
      </details>` : ''}
      <div id="lockBanner"></div>
      <div id="tablesArea"></div>
    </div>
  `;

  const area = document.getElementById('tablesArea');
  let cachedItems = tablesCache.get(id) || [];
  let locked = false;

  function renderRows() {
    renderTableCards(area, id, cachedItems, locked);
  }

  function readAnalyzeOpts() {
    if (!isMongo) return {};
    const sampleSize = parseInt(document.getElementById('opt-sampleSize')?.value, 10);
    const samplePct = parseFloat(document.getElementById('opt-samplePct')?.value);
    const sampleThreshold = parseInt(document.getElementById('opt-sampleThreshold')?.value, 10);
    const ai = document.getElementById('opt-ai')?.checked;
    const accurateCounts = document.getElementById('opt-accurateCounts')?.checked;
    return {
      sample_size: sampleSize || 2000,
      sample_pct: samplePct || 1,
      sample_threshold: sampleThreshold || 100000,
      ai: ai || false,
      accurate_counts: accurateCounts || false,
    };
  }

  const migSection = mountMigSection(document.getElementById('migCardSection'), id, project.source_config.type, project.target_config.type, () => cachedItems, (isLocked, status) => {
    locked = isLocked;
    document.getElementById('lockBanner').innerHTML = locked
      ? `<div class="banner info">Table changes are disabled while a migration is ${esc(status)}.</div>`
      : '';
    const btn = document.getElementById('reanalyzeBtn');
    if (btn) btn.disabled = locked;
    renderRows();
  });

  async function refreshTables() {
    if (locked) return;
    try {
      const analyzeOpts = readAnalyzeOpts();
      cachedItems = await loadTables(id, project, analyzeOpts);
      tablesCache.set(id, cachedItems);
      renderRows();
      // Refresh migration section so table checkboxes appear
      migSection.refresh();
    } catch (err) {
      area.innerHTML = errorBanner('Analyze failed: ' + err.message, { retry: refreshTables });
    }
  }

  document.getElementById('reanalyzeBtn').addEventListener('click', refreshTables);

  setCleanup(() => migSection.stop());

  // Show the last known result immediately if we have one (revisiting via
  // hash nav / browser back-forward shouldn't force a live re-analyze);
  // Re-analyze above always bypasses this. First visit this session (or a
  // hard page reload, which clears the cache) still analyzes live.
  if (tablesCache.has(id)) {
    renderRows();
  } else {
    await refreshTables();
  }
}

// ---------------------------------------------------------------------------
// Table detail (split view)
// ---------------------------------------------------------------------------

function colRowReadOnly(c, pending) {
  return `
    <div class="col-row">
      <div class="col-name">${esc(c.name)} ${c.primary_key ? '<span class="badge ok">PK</span>' : ''} ${pending ? '<span class="badge warn">to be created</span>' : ''}</div>
      <div class="col-type mono">${esc(c.type)} ${c.nullable ? '· nullable' : '· not null'}</div>
    </div>
  `;
}

function rowId(fieldName) {
  return fieldName.replace(/[^a-zA-Z0-9_-]/g, '_');
}

function editableRow(r, { showRename = true, showStrategy = true, typeOptions = PG_TYPES } = {}) {
  const rid = rowId(r.field_name);
  const knownType = typeOptions.includes(r.pg_type);
  return `
    <div class="col-row" id="row-${rid}">
      <div class="edit-grid">
        ${showRename
          ? `<input type="text" id="col-${rid}" value="${esc(r.pg_column)}" placeholder="column name">`
          : `<div class="col-name mono">${esc(r.pg_column)}</div>`}
        <select id="type-${rid}">
          ${typeOptions.map((t) => `<option value="${t}" ${t === r.pg_type ? 'selected' : ''}>${t}</option>`).join('')}
          ${!knownType ? `<option value="${esc(r.pg_type)}" selected>${esc(r.pg_type)}</option>` : ''}
        </select>
        ${showStrategy ? `
        <select id="strat-${rid}">
          ${STRATEGIES.map((s) => `<option value="${s}" ${s === r.strategy ? 'selected' : ''}>${s}</option>`).join('')}
        </select>` : '<span></span>'}
        <span></span>
      </div>
      <div class="checkbox-row">
        <input type="checkbox" id="null-${rid}" ${r.nullable ? 'checked' : ''}><label for="null-${rid}">Nullable</label>
        <input type="checkbox" id="pk-${rid}" ${r.is_pk ? 'checked' : ''}><label for="pk-${rid}">Primary key</label>
      </div>
    </div>
  `;
}

function readEditedRow(fieldName, orig, { showRename, showStrategy }) {
  const rid = rowId(fieldName);
  return {
    field_name: fieldName,
    pg_column: showRename ? document.getElementById(`col-${rid}`).value.trim() : orig.pg_column,
    pg_type: document.getElementById(`type-${rid}`).value,
    strategy: showStrategy ? document.getElementById(`strat-${rid}`).value : (orig.strategy || 'direct'),
    nullable: document.getElementById(`null-${rid}`).checked,
    is_pk: document.getElementById(`pk-${rid}`).checked,
  };
}

// mountEditableTargetPanel wires up dirty-tracking, a Save button that
// appears in saveAreaEl once something changed, and a confirm-then-PUT save
// flow against the project-wide schema-plan override endpoint, for editable
// rows (SchemaPlanRow-shaped) the caller has already rendered into the DOM
// via editableRow() — each field's target cell interleaved right after its
// source cell so they share one grid row (see .schema-rows in app.css).
// panelEl just needs to contain all of it so the input/select listeners
// below can be scoped to it. Shared by the Mongo→Postgres field editor and
// the generalized relational column-type editor.
function mountEditableTargetPanel(panelEl, saveAreaEl, rows, opts) {
  const { collection, projectId, showRename = true, showStrategy = true, onSaved } = opts;
  const original = {};
  rows.forEach((r) => { original[r.field_name] = { ...r }; });

  function readCur(fieldName) {
    return readEditedRow(fieldName, original[fieldName], { showRename, showStrategy });
  }

  function isDirty(cur, orig) {
    return cur.pg_column !== orig.pg_column || cur.pg_type !== orig.pg_type ||
      cur.strategy !== orig.strategy || cur.nullable !== orig.nullable || cur.is_pk !== orig.is_pk;
  }

  function refreshDirty() {
    let anyDirty = false;
    rows.forEach((r) => {
      const dirty = isDirty(readCur(r.field_name), original[r.field_name]);
      const rowEl = document.getElementById('row-' + rowId(r.field_name));
      if (rowEl) rowEl.classList.toggle('dirty', dirty);
      if (dirty) anyDirty = true;
    });
    if (anyDirty) {
      saveAreaEl.innerHTML = `<div class="actions-row"><button class="btn primary" id="saveSchemaBtn">Save Changes</button></div>`;
      document.getElementById('saveSchemaBtn').addEventListener('click', onSave);
    } else {
      saveAreaEl.innerHTML = '';
    }
  }

  panelEl.querySelectorAll('input, select').forEach((el) => {
    el.addEventListener('input', refreshDirty);
    el.addEventListener('change', refreshDirty);
  });

  async function onSave() {
    const changed = [];
    const diffLines = [];
    rows.forEach((r) => {
      const cur = readCur(r.field_name);
      const orig = original[r.field_name];
      if (isDirty(cur, orig)) {
        changed.push({ collection, ...cur });
        diffLines.push(`${r.field_name}: ${orig.pg_type} → ${cur.pg_type}${showStrategy && orig.strategy !== cur.strategy ? ` (strategy ${orig.strategy} → ${cur.strategy})` : ''}`);
      }
    });
    if (changed.length === 0) return;
    const ok = await confirmModal({
      title: 'Save schema changes?',
      body: `This updates the migration plan for ${changed.length} field(s):\n\n${diffLines.join('\n')}`,
      confirmLabel: 'Save',
    });
    if (!ok) return;
    const btn = document.getElementById('saveSchemaBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Saving…';
    try {
      await api.putSchemaPlanOverrides(projectId, changed);
      if (onSaved) await onSaved();
    } catch (err) {
      saveAreaEl.insertAdjacentHTML('beforeend', errorBanner('Save failed: ' + err.message));
      btn.disabled = false;
      btn.textContent = 'Save Changes';
    }
  }
}

async function renderRelationalSplit(area, id, tableName, locked, targetDialect) {
  area.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading schema…</div>`;
  let schema, plan;
  try {
    [schema, plan] = await Promise.all([
      api.getTableSchema(id, tableName),
      api.getSchemaPlan(id),
    ]);
  } catch (err) {
    area.innerHTML = errorBanner('Could not load table schema: ' + err.message, { retry: () => renderRelationalSplit(area, id, tableName, locked, targetDialect) });
    return;
  }
  const srcCols = (schema.source && schema.source.columns) || [];
  const tgtCols = (schema.target && schema.target.columns) || [];
  const tgtExists = tgtCols.length > 0;
  const tgtByName = {};
  tgtCols.forEach((c) => { tgtByName[c.name] = c; });
  const overrideByField = {};
  (plan[tableName] || []).forEach((r) => { overrideByField[r.field_name] = r; });

  // Normalize each source column into a SchemaPlanRow-shaped row: a saved
  // override wins, else the existing target column (table already exists on
  // the target), else the source column's own type as a starting suggestion.
  const rows = srcCols.map((c) => {
    const override = overrideByField[c.name];
    const tgtCol = tgtByName[c.name];
    return {
      field_name: c.name,
      pg_column: c.name,
      pg_type: override ? override.pg_type : (tgtCol ? tgtCol.type : c.type),
      strategy: 'direct',
      is_pk: override ? override.is_pk : (tgtCol ? tgtCol.primary_key : c.primary_key),
      nullable: override ? override.nullable : (tgtCol ? tgtCol.nullable : c.nullable),
      overridden: !!override,
    };
  });

  if (locked || rows.length === 0) {
    area.innerHTML = `
      ${!tgtExists ? '<div class="banner info">This table doesn’t exist on the target yet — it will be created automatically when the migration runs.</div>' : ''}
      <div class="split"><h3>Source</h3><h3>Target ${tgtExists ? '' : '(not yet created)'}</h3></div>
      <div class="schema-rows">
        ${srcCols.map((c, i) => colRowReadOnly(c) +
          colRowReadOnly({ name: rows[i].pg_column, type: rows[i].pg_type, nullable: rows[i].nullable, primary_key: rows[i].is_pk }, !tgtByName[rows[i].field_name])
        ).join('') || '<div class="hint">No columns.</div>'}
      </div>
    `;
    return;
  }

  const editOpts = { showRename: false, showStrategy: false, typeOptions: typesForDialect(targetDialect) };

  const hasOverrides = rows.some((r) => r.overridden);
  area.innerHTML = `
    ${!tgtExists
      ? '<div class="banner info">This table doesn’t exist on the target yet — it will be created with these settings when the migration runs.</div>'
      : '<div class="banner info">This table already exists on the target. Type overrides only take effect if the migration is run with "Drop &amp; recreate target tables".</div>'}
    <div class="banner info">Changing the type edits the <code>CREATE TABLE</code> statement only — row values are transferred as-is, so make sure the new type can still hold the source data.</div>
    <div class="split"><h3>Source</h3><h3>Suggested target</h3></div>
    <div class="schema-rows" id="schemaRows">
      ${srcCols.map((c, i) => colRowReadOnly(c) + editableRow(rows[i], editOpts)).join('')}
    </div>
    <div id="saveArea"></div>
    ${hasOverrides ? `
    <details class="advanced">
      <summary>Reset schema plan</summary>
      <p>This removes any overrides for this table and reverts to the auto-detected schema.</p>
      <button class="btn danger" id="resetTableBtn">Reset this table's schema</button>
    </details>` : ''}
  `;

  mountEditableTargetPanel(
    document.getElementById('schemaRows'),
    document.getElementById('saveArea'),
    rows,
    {
      collection: tableName,
      projectId: id,
      ...editOpts,
      onSaved: () => renderRelationalSplit(area, id, tableName, locked, targetDialect),
    }
  );

  const resetBtn = document.getElementById('resetTableBtn');
  if (resetBtn) {
    resetBtn.addEventListener('click', async () => {
      const ok = await confirmModal({
        title: 'Reset schema plan?',
        body: `This removes all overrides for "${tableName}" and reverts to the auto-detected schema.`,
        confirmLabel: 'Reset',
        danger: true,
      });
      if (!ok) return;
      resetBtn.disabled = true;
      resetBtn.innerHTML = '<span class="spinner"></span> Resetting…';
      try {
        await api.resetSchemaPlan(id, tableName);
        navigate(`#/projects/${encodeURIComponent(id)}`);
      } catch (err) {
        area.insertAdjacentHTML('beforeend', errorBanner('Reset failed: ' + err.message));
        resetBtn.disabled = false;
        resetBtn.textContent = "Reset this table's schema";
      }
    });
  }
}

async function renderMongoSplit(area, id, tableName, locked) {
  area.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading schema plan…</div>`;
  let plan;
  try {
    plan = await api.getSchemaPlan(id);
  } catch (err) {
    area.innerHTML = errorBanner('Could not load schema plan: ' + err.message, { retry: () => renderMongoSplit(area, id, tableName, locked) });
    return;
  }
  const rows = plan[tableName];
  if (!rows || rows.length === 0) {
    area.innerHTML = `
      <div class="banner info">No schema plan found for this collection yet.</div>
      <a class="btn" href="#/projects/${encodeURIComponent(id)}">Back to collections (Re-analyze there)</a>
    `;
    return;
  }

  function sourceCellHTML(r) {
    return `
      <div class="col-row">
        <div class="col-name">${esc(r.field_name)} ${r.is_pk ? '<span class="badge ok">PK</span>' : ''}</div>
        ${r.overridden ? '<div class="hint">previously overridden</div>' : ''}
      </div>
    `;
  }

  if (locked) {
    area.innerHTML = `
      <div class="split"><h3>Source (MongoDB field)</h3><h3>Target (PostgreSQL)</h3></div>
      <div class="schema-rows">
        ${rows.map((r) => sourceCellHTML(r) +
          colRowReadOnly({ name: r.pg_column, type: r.pg_type, nullable: r.nullable, primary_key: r.is_pk })
        ).join('')}
      </div>
    `;
    return;
  }

  const editOpts = { showRename: true, showStrategy: true, typeOptions: PG_TYPES };

  area.innerHTML = `
    <div class="split"><h3>Source (MongoDB field)</h3><h3>Suggested target (PostgreSQL)</h3></div>
    <div class="schema-rows" id="schemaRows">
      ${rows.map((r) => sourceCellHTML(r) + editableRow(r, editOpts)).join('')}
    </div>
    <div id="saveArea"></div>
    <details class="advanced">
      <summary>Reset schema plan</summary>
      <p>This removes the schema plan for this collection. You can re-analyze from the collections list to regenerate it.</p>
      <button class="btn danger" id="resetCollectionBtn">Reset this collection's schema</button>
    </details>
  `;

  mountEditableTargetPanel(
    document.getElementById('schemaRows'),
    document.getElementById('saveArea'),
    rows,
    {
      collection: tableName,
      projectId: id,
      ...editOpts,
      onSaved: () => renderMongoSplit(area, id, tableName, locked),
    }
  );

  const resetBtn = document.getElementById('resetCollectionBtn');
  if (resetBtn) {
    resetBtn.addEventListener('click', async () => {
      const ok = await confirmModal({
        title: 'Reset schema plan?',
        body: `This removes the schema plan for "${tableName}". You can re-analyze from the collections list to regenerate it.`,
        confirmLabel: 'Reset',
        danger: true,
      });
      if (!ok) return;
      resetBtn.disabled = true;
      resetBtn.innerHTML = '<span class="spinner"></span> Resetting…';
      try {
        await api.resetSchemaPlan(id, tableName);
        navigate(`#/projects/${encodeURIComponent(id)}`);
      } catch (err) {
        area.insertAdjacentHTML('beforeend', errorBanner('Reset failed: ' + err.message));
        resetBtn.disabled = false;
        resetBtn.textContent = "Reset this collection's schema";
      }
    });
  }
}

async function renderTableDetail(id, tableName) {
  document.title = 'XferDB — ' + tableName;
  app.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading…</div>`;
  let project;
  try {
    project = await api.getProject(id);
  } catch (err) {
    app.innerHTML = errorBanner('Project not found: ' + err.message) + `<a class="btn" href="#/">Back to projects</a>`;
    return;
  }
  let locked = false;
  let lockedStatus = '';
  try {
    const active = findActiveMigration(await api.listMigrations(id));
    if (active) { locked = true; lockedStatus = active.status; }
  } catch (err) { /* a failed migrations lookup shouldn't block viewing the table */ }

  app.innerHTML = `
    <div class="breadcrumb">
      <a href="#/">Projects</a> / <a href="#/projects/${encodeURIComponent(id)}">${esc(project.name)}</a> / ${esc(tableName)}
    </div>
    <div class="page-header"><h1 class="mono">${esc(tableName)}</h1></div>
    ${locked ? `<div class="banner info">A migration is currently ${esc(lockedStatus)} for this project — schema editing is disabled until it finishes.</div>` : ''}
    <div id="detailArea"></div>
  `;
  const area = document.getElementById('detailArea');
  if (project.source_config.type === 'mongodb') {
    await renderMongoSplit(area, id, tableName, locked);
  } else {
    await renderRelationalSplit(area, id, tableName, locked, project.target_config.type);
  }
}

// ---------------------------------------------------------------------------
// Migration actions + status (shared by the tables screen card and the full
// progress page)
// ---------------------------------------------------------------------------

// mountMigActions renders Pause/Resume/Cancel buttons for the given phase
// into actionsEl and wires them up. onDone fires after a successful action
// so the caller can refresh its own view; errors render into errorArea.
function mountMigActions(actionsEl, migId, phase, { onDone, errorArea } = {}) {
  const buttons = [];
  if (['in_progress', 'schema', 'post_schema'].includes(phase)) buttons.push('<button class="btn" id="pauseBtn">Pause</button>');
  if (phase === 'paused') buttons.push('<button class="btn primary" id="resumeBtn">Resume</button>');
  if (!['complete', 'failed', 'cancelled'].includes(phase)) buttons.push('<button class="btn danger" id="cancelBtn">Cancel</button>');
  actionsEl.innerHTML = buttons.join(' ');

  async function doAction(action) {
    try {
      await api.patchMigration(migId, action);
      if (onDone) onDone();
    } catch (err) {
      if (errorArea) errorArea.insertAdjacentHTML('afterbegin', errorBanner(action + ' failed: ' + err.message));
    }
  }

  const pauseBtn = actionsEl.querySelector('#pauseBtn');
  if (pauseBtn) pauseBtn.addEventListener('click', () => doAction('pause'));
  const resumeBtn = actionsEl.querySelector('#resumeBtn');
  if (resumeBtn) resumeBtn.addEventListener('click', () => doAction('resume'));
  const cancelBtn = actionsEl.querySelector('#cancelBtn');
  if (cancelBtn) cancelBtn.addEventListener('click', async () => {
    const ok = await confirmModal({
      title: 'Cancel migration?',
      body: 'This stops the migration. Rows already written are kept, but the run is marked cancelled and cannot be resumed.',
      confirmLabel: 'Cancel Migration',
      danger: true,
    });
    if (!ok) return;
    await doAction('cancel');
  });
}

// mountMigSection renders the migration status/controls card on a project's
// tables screen: a live-polling progress card with Pause/Resume/Cancel while
// a migration is pending/in_progress/paused, or a "Run Migration" form plus
// history otherwise. Returns a cleanup function that stops polling.
// getTables() returns the current list of tables for selection.
// onLockChange(locked, status) fires whenever table rows should lock/unlock.
function mountMigSection(container, projectId, sourceType, targetType, getTables, onLockChange) {
  let stopped = false;
  let timer = null;
  function clearTimer() {
    if (timer) { clearTimeout(timer); timer = null; }
  }

  async function render() {
    if (stopped) return;
    container.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading migration status…</div>`;
    let migs;
    try {
      migs = await api.listMigrations(projectId);
    } catch (err) {
      container.innerHTML = errorBanner('Could not load migrations: ' + err.message, { retry: render });
      onLockChange(false, '');
      return;
    }
    if (stopped) return;
    const active = findActiveMigration(migs);
    if (active) {
      onLockChange(true, active.status);
      mountActiveCard(active.id, active.status);
    } else {
      onLockChange(false, '');
      mountStartForm(migs, sourceType, targetType, getTables());
    }
  }

  function mountActiveCard(migId, initialStatus) {
    container.innerHTML = `
      <div class="page-header">
        <h2>Migration</h2>
        <div class="actions-row" id="migActions"></div>
      </div>
      <div id="migCardBody"></div>
      <div class="actions-row"><a class="btn small" href="#/migrations/${encodeURIComponent(migId)}">View full progress</a></div>
    `;

    let currentStatus = initialStatus;

    function renderPausedState(isOrphaned = false) {
      const body = document.getElementById('migCardBody');
      const actionsEl = document.getElementById('migActions');
      if (!body || !actionsEl) return;
      body.innerHTML = `
        <div class="stat-grid">
          <div class="stat-box"><div class="label">Phase</div><div class="value"><span class="badge warn">paused</span></div></div>
          <div class="stat-box"><div class="label">Status</div><div class="value">${isOrphaned ? 'Migration is orphaned' : 'Migration is paused'}</div></div>
        </div>
        <div class="hint">${isOrphaned
          ? 'This migration was paused when the server restarted. Delete it to start a new migration.'
          : 'Stats are not available while paused. Resume to continue.'}</div>
        ${isOrphaned ? '<div id="deleteArea" style="margin-top:12px;"></div>' : ''}
      `;
      if (isOrphaned) {
        const deleteArea = document.getElementById('deleteArea');
        deleteArea.innerHTML = `<button class="btn danger" id="deleteMigBtn">Delete Migration</button>`;
        document.getElementById('deleteMigBtn').addEventListener('click', async () => {
          const ok = await confirmModal({
            title: 'Delete migration?',
            body: 'This removes the orphaned migration record. Any partial data written to the target remains.',
            confirmLabel: 'Delete',
            danger: true,
          });
          if (!ok) return;
          try {
            await api.deleteMigration(migId);
            render();
          } catch (err) {
            deleteArea.innerHTML = errorBanner('Delete failed: ' + err.message);
          }
        });
        actionsEl.innerHTML = ''; // No resume/cancel for orphaned
      } else {
        mountMigActions(actionsEl, migId, 'paused', {
          onDone: () => { clearTimer(); render(); },
          errorArea: body,
        });
      }
    }

    async function tick() {
      if (stopped) return;
      try {
        const snap = await api.getStats(migId);
        if (stopped) return;
        currentStatus = snap.Phase;
        const body = document.getElementById('migCardBody');
        const actionsEl = document.getElementById('migActions');
        if (!body || !actionsEl) return;
        const rows = snap.Rows || {};
        const pct = rows.Total > 0 ? Math.round((rows.Transferred * 100) / rows.Total) : 0;
        const phaseBadge = snap.Phase === 'complete' ? 'ok' : snap.Phase === 'failed' ? 'fail' : snap.Phase === 'cancelled' ? 'cancelled' : snap.Phase === 'paused' ? 'warn' : 'progress';
        body.innerHTML = `
          <div class="stat-grid">
            <div class="stat-box"><div class="label">Phase</div><div class="value"><span class="badge ${phaseBadge}">${esc(snap.Phase)}</span></div></div>
            <div class="stat-box"><div class="label">Rows</div><div class="value">${fmtInt(rows.Transferred)} / ${fmtInt(rows.Total)} (${pct}%)</div></div>
            <div class="stat-box"><div class="label">ETA</div><div class="value">${snap.ETASeconds ? fmtDuration(snap.ETASeconds) : '—'}</div></div>
          </div>
          <div class="progress-bar"><div class="fill ${snap.Phase === 'complete' ? 'done' : snap.Phase === 'failed' ? 'failed' : snap.Phase === 'cancelled' ? 'cancelled' : ''}" style="width:${pct}%"></div></div>
          ${snap.Phase === 'cancelled' ? `<div class="banner info"><span>Migration cancelled.</span></div>` : ''}
        `;
        mountMigActions(actionsEl, migId, snap.Phase, {
          onDone: () => { clearTimer(); tick(); },
          errorArea: body,
        });
        if (['complete', 'failed', 'cancelled'].includes(snap.Phase)) {
          clearTimer();
          render();
          return;
        }
      } catch (err) {
        // Stats not available - migration may be paused or orphaned
        if (currentStatus === 'paused') {
          // Stats fail for paused migration = orphaned (server restarted)
          renderPausedState(true);
          // Poll to detect if user deletes via API
          if (!stopped) timer = setTimeout(render, 5000);
          return;
        }
        const body = document.getElementById('migCardBody');
        if (body) body.insertAdjacentHTML('afterbegin', errorBanner('Stats fetch failed: ' + err.message));
      }
      if (!stopped) timer = setTimeout(tick, 1500);
    }

    // Try to get stats first - if it fails and we're paused, it's orphaned
    if (initialStatus === 'paused') {
      // Try stats to check if migration is actually running
      api.getStats(migId).then(() => {
        // Stats available = migration is actually running, just paused
        renderPausedState(false);
        timer = setTimeout(render, 3000);
      }).catch(() => {
        // Stats unavailable = orphaned
        renderPausedState(true);
        timer = setTimeout(render, 5000);
      });
    } else {
      tick();
    }
  }

  function mountStartForm(migs, sourceType, targetType, tables) {
    const segmentDisabled = sourceType === 'mongodb';
    const isPgTarget = targetType === 'postgres';
    const tableNames = (tables || []).map(t => t.name);
    container.innerHTML = `
      <h2>Migration</h2>
      <details class="advanced" open>
        <summary>Run a migration</summary>
        <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeNone" value="none" checked><label for="modeNone">Keep existing target schema/data</label></div>
        <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeTruncate" value="truncate"><label for="modeTruncate">Truncate target tables first (keep schema)</label></div>
        <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeRecreate" value="recreate"><label for="modeRecreate">Drop &amp; recreate target tables</label></div>
        <div class="field-row">
          <div><label>Table workers</label><input type="number" id="m-workers" value="1"><div class="hint">Parallel tables to migrate</div></div>
          <div><label>Batch size</label><input type="number" id="m-batch" value="1000"><div class="hint">Rows per INSERT batch</div></div>
        </div>
        <div class="field-row">
          <div><label>Segment workers</label><input type="number" id="m-segment" value="0" ${segmentDisabled ? 'disabled' : ''}><div class="hint">${segmentDisabled ? 'Not supported for MongoDB sources' : 'Workers per table (0 = disabled)'}</div></div>
          <div></div>
        </div>
        <details class="advanced" style="margin-top:12px;">
          <summary>Performance options</summary>
          <div class="checkbox-row"><input type="checkbox" id="m-async"><label for="m-async">Async pipeline</label><span class="hint" style="margin-left:8px;">Overlap reading and writing for faster throughput</span></div>
          ${isPgTarget ? `<div class="checkbox-row"><input type="checkbox" id="m-bulk"><label for="m-bulk">Bulk copy (COPY protocol)</label><span class="hint" style="margin-left:8px;">Faster writes; only available with truncate or drop &amp; recreate above</span></div>` : ''}
          <div class="checkbox-row"><input type="checkbox" id="m-accurate"><label for="m-accurate">Accurate row counts</label><span class="hint" style="margin-left:8px;">Use exact counts instead of estimates (slower)</span></div>
          <div class="checkbox-row" id="m-offset-row" style="display:none;"><input type="checkbox" id="m-offset"><label for="m-offset">Force OFFSET segments</label><span class="hint" style="margin-left:8px;">Use OFFSET-based splitting instead of PK range</span></div>
        </details>
        ${tableNames.length > 0 ? `
        <div style="margin-top:12px;">
          <label style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
            Tables to migrate
            <span style="font-weight:normal;color:var(--muted);">(<span id="tableCount">${tableNames.length}</span> of ${tableNames.length} selected)</span>
          </label>
          <div class="checkbox-row" style="margin-bottom:6px;">
            <input type="checkbox" id="selectAllTables" checked>
            <label for="selectAllTables">Select all</label>
          </div>
          <div id="tableCheckboxes" style="max-height:200px;overflow-y:auto;border:1px solid var(--border);border-radius:6px;padding:8px;">
            ${tableNames.map(name => `
              <div class="checkbox-row" style="margin:4px 0;">
                <input type="checkbox" class="table-cb" id="tbl-${esc(name)}" value="${esc(name)}" checked>
                <label for="tbl-${esc(name)}" class="mono" style="font-size:12px;">${esc(name)}</label>
              </div>
            `).join('')}
          </div>
        </div>
        ` : ''}
      </details>
      <div id="startErr"></div>
      <button class="btn primary" id="startMigBtn">Run Migration</button>
      <div style="margin-top:18px;">
        <details class="advanced"><summary>History</summary><div id="migHistory"></div></details>
      </div>
    `;

    // Show offset option when segment workers > 0
    const segmentInput = document.getElementById('m-segment');
    const offsetRow = document.getElementById('m-offset-row');
    if (segmentInput && offsetRow) {
      const updateOffsetVisibility = () => {
        const val = parseInt(segmentInput.value, 10) || 0;
        offsetRow.style.display = val > 0 ? 'flex' : 'none';
      };
      segmentInput.addEventListener('input', updateOffsetVisibility);
      updateOffsetVisibility();
    }

    // Bulk copy (COPY protocol) has no ON CONFLICT support, so it only
    // produces correct results when the target tables are guaranteed empty
    // going in -- tie its checkbox to the truncate/recreate schema mode
    // instead of letting it be picked alongside "keep existing data", which
    // fails every table with a duplicate-key error.
    const bulkInput = document.getElementById('m-bulk');
    if (bulkInput) {
      const updateBulkAvailability = () => {
        const mode = document.querySelector('input[name=schemaMode]:checked').value;
        const allowed = mode !== 'none';
        bulkInput.disabled = !allowed;
        if (!allowed) bulkInput.checked = false;
      };
      document.querySelectorAll('input[name=schemaMode]').forEach(r => r.addEventListener('change', updateBulkAvailability));
      updateBulkAvailability();
    }

    // Table selection handlers
    if (tableNames.length > 0) {
      const updateCount = () => {
        const checked = container.querySelectorAll('.table-cb:checked').length;
        document.getElementById('tableCount').textContent = checked;
      };
      document.getElementById('selectAllTables').addEventListener('change', (e) => {
        container.querySelectorAll('.table-cb').forEach(cb => { cb.checked = e.target.checked; });
        updateCount();
      });
      container.querySelectorAll('.table-cb').forEach(cb => {
        cb.addEventListener('change', () => {
          const allChecked = container.querySelectorAll('.table-cb:checked').length === tableNames.length;
          document.getElementById('selectAllTables').checked = allChecked;
          updateCount();
        });
      });
    }
    document.getElementById('startMigBtn').addEventListener('click', async () => {
      const mode = document.querySelector('input[name=schemaMode]:checked').value;
      const body = {
        table_workers: parseInt(document.getElementById('m-workers').value, 10) || 1,
        segment_workers: parseInt(document.getElementById('m-segment').value, 10) || 0,
        batch_size: parseInt(document.getElementById('m-batch').value, 10) || 1000,
        recreate_schema: mode === 'recreate',
        truncate: mode === 'truncate',
        async_pipeline: document.getElementById('m-async')?.checked || false,
        bulk_copy: document.getElementById('m-bulk')?.checked || false,
        accurate_counts: document.getElementById('m-accurate')?.checked || false,
        offset_fallback: document.getElementById('m-offset')?.checked || false,
      };
      // Add selected tables if not all are selected
      if (tableNames.length > 0) {
        const selectedTables = Array.from(container.querySelectorAll('.table-cb:checked')).map(cb => cb.value);
        if (selectedTables.length === 0) {
          document.getElementById('startErr').innerHTML = errorBanner('Please select at least one table to migrate.');
          return;
        }
        if (selectedTables.length < tableNames.length) {
          body.tables = selectedTables;
        }
      }
      const btn = document.getElementById('startMigBtn');
      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Starting…';
      try {
        const mig = await api.startMigration(projectId, body);
        navigate(`#/migrations/${encodeURIComponent(mig.id)}`);
      } catch (err) {
        document.getElementById('startErr').innerHTML = errorBanner('Could not start migration: ' + err.message);
        btn.disabled = false;
        btn.textContent = 'Run Migration';
      }
    });

    const histEl = document.getElementById('migHistory');
    if (!migs || migs.length === 0) {
      histEl.innerHTML = `<div class="hint">No migrations run yet.</div>`;
    } else {
      histEl.innerHTML = migs.map((m) => `
        <div class="list-row" data-id="${esc(m.id)}">
          <div>
            <div class="name mono">${esc(m.id.slice(0, 8))}</div>
            <div class="meta">created ${esc(fmtDate(m.created_at))}${m.error ? ' · ' + esc(m.error) : ''}</div>
          </div>
          <span class="badge ${esc(m.status)}">${esc(m.status)}</span>
        </div>
      `).join('');
      histEl.querySelectorAll('.list-row').forEach((row) => {
        row.addEventListener('click', () => navigate(`#/migrations/${encodeURIComponent(row.dataset.id)}`));
      });
    }
  }

  render();
  return {
    stop: () => { stopped = true; clearTimer(); },
    refresh: render,
  };
}

// ---------------------------------------------------------------------------
// Migration progress
// ---------------------------------------------------------------------------

function tableProgressRow(t) {
  const pct = t.Total > 0 ? Math.round((t.Transferred * 100) / t.Total) : 0;
  const icons = { done: '✓', in_progress: '●', failed: '✗', cancelled: '–', pending: '·' };
  const icon = icons[t.Status] || '○';
  const fillClass = t.Status === 'done' ? 'done' : t.Status === 'failed' ? 'failed' : t.Status === 'cancelled' ? 'cancelled' : '';
  return `
    <div class="table-progress-row">
      <div class="top"><span><span class="status-icon">${icon}</span>${esc(t.Name)}</span><span>${fmtInt(t.Transferred)} / ${fmtInt(t.Total)} (${pct}%)</span></div>
      <div class="progress-bar"><div class="fill ${fillClass}" style="width:${pct}%"></div></div>
      ${t.Error ? `<div class="err">${esc(t.Error)}</div>` : ''}
    </div>
  `;
}

async function renderMigrationProgress(migId) {
  document.title = 'XferDB — Migration';
  app.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading migration…</div>`;
  let mig;
  try {
    mig = await api.getMigration(migId);
  } catch (err) {
    app.innerHTML = errorBanner('Migration not found: ' + err.message) + `<a class="btn" href="#/">Back to projects</a>`;
    return;
  }
  app.innerHTML = `
    <div class="breadcrumb"><a href="#/">Projects</a> / <a href="#/projects/${encodeURIComponent(mig.project_id)}">Project</a> / Migration</div>
    <div class="page-header">
      <div><h1 class="mono">${esc(migId.slice(0, 8))}</h1><div class="sub">Migration progress</div></div>
      <div class="actions-row" id="migActions"></div>
    </div>
    <div id="progressArea"></div>
  `;

  let stopped = false;
  let timer = null;
  let historyLoaded = false;
  let historyOpen = false;
  let cachedHistoryHTML = '<div class="loading-block"><span class="spinner"></span> Loading history…</div>';

  function clearTimer() {
    if (timer) { clearTimeout(timer); timer = null; }
  }

  function renderProgress(snap) {
    const actionsEl = document.getElementById('migActions');
    if (actionsEl) {
      mountMigActions(actionsEl, migId, snap.Phase, {
        onDone: () => { clearTimer(); tick(); },
        errorArea: document.getElementById('progressArea'),
      });
    }
    const rows = snap.Rows || {};
    const pct = rows.Total > 0 ? Math.round((rows.Transferred * 100) / rows.Total) : 0;
    const phaseBadge = snap.Phase === 'complete' ? 'ok' : snap.Phase === 'failed' ? 'fail' : snap.Phase === 'cancelled' ? 'cancelled' : snap.Phase === 'paused' ? 'warn' : 'progress';
    const area = document.getElementById('progressArea');
    if (!area) return;
    area.innerHTML = `
      ${snap.Phase === 'complete' ? '<div class="banner ok"><span>Migration complete.</span></div>' : ''}
      ${snap.Phase === 'failed' ? `<div class="banner error"><span>Migration failed.${snap.Errors && snap.Errors.length ? ' ' + esc(snap.Errors.join('; ')) : ''}</span></div>` : ''}
      ${snap.Phase === 'cancelled' ? '<div class="banner info"><span>Migration cancelled.</span></div>' : ''}
      <div class="stat-grid">
        <div class="stat-box"><div class="label">Phase</div><div class="value"><span class="badge ${phaseBadge}">${esc(snap.Phase)}</span></div></div>
        <div class="stat-box"><div class="label">Elapsed</div><div class="value">${fmtDuration(snap.ElapsedSeconds)}</div></div>
        <div class="stat-box"><div class="label">ETA</div><div class="value">${snap.ETASeconds ? fmtDuration(snap.ETASeconds) : '—'}</div></div>
        <div class="stat-box"><div class="label">Rate</div><div class="value">${Math.round(rows.RatePerSecond || 0)}/s</div></div>
      </div>
      <div class="card">
        <strong>Resource Usage</strong>
        <div class="stat-grid" style="margin-top:8px;">
          <div class="stat-box"><div class="label">CPU</div><div class="value">${((snap.resource?.CPUPercent) || 0).toFixed(1)}%</div></div>
          <div class="stat-box"><div class="label">Memory</div><div class="value">${((snap.resource?.MemAllocMB) || 0).toFixed(1)} MB</div></div>
          <div class="stat-box"><div class="label">Goroutines</div><div class="value">${snap.resource?.Goroutines || 0}</div></div>
          <div class="stat-box"><div class="label">Sys Memory</div><div class="value">${((snap.resource?.MemSysMB) || 0).toFixed(1)} MB</div></div>
        </div>
      </div>
      <div class="card">
        <strong>Rows: ${fmtInt(rows.Transferred)} / ${fmtInt(rows.Total)} (${pct}%)</strong>
        <div class="progress-bar"><div class="fill ${snap.Phase === 'complete' ? 'done' : snap.Phase === 'failed' ? 'failed' : snap.Phase === 'cancelled' ? 'cancelled' : ''}" style="width:${pct}%"></div></div>
      </div>
      <div class="card">
        <strong>Tables</strong>
        ${(snap.TableDetails || []).map(tableProgressRow).join('') || '<div class="hint">No tables yet.</div>'}
      </div>
      <details class="advanced" id="statsHistoryDetails" ${historyOpen ? 'open' : ''}>
        <summary>Stats History</summary>
        <div id="statsHistoryContent">${cachedHistoryHTML}</div>
      </details>
    `;
    // Set up event listener for stats history toggle
    const detailsEl = document.getElementById('statsHistoryDetails');
    if (detailsEl) {
      detailsEl.addEventListener('toggle', async () => {
        historyOpen = detailsEl.open;
        if (detailsEl.open && !historyLoaded) {
          historyLoaded = true;
          await loadStatsHistory();
        }
      });
    }
  }

  async function loadStatsHistory() {
    const container = document.getElementById('statsHistoryContent');
    if (!container) return;
    try {
      const records = await api.getStatsHistory(migId);
      if (!records || records.length === 0) {
        cachedHistoryHTML = '<div class="hint">No history recorded yet.</div>';
        container.innerHTML = cachedHistoryHTML;
        return;
      }
      // Build table showing key metrics over time
      const tableRows = records.map(r => {
        const pct = r.rows_total > 0 ? Math.round((r.rows_transferred * 100) / r.rows_total) : 0;
        const ts = new Date(r.timestamp).toLocaleTimeString();
        return `
          <tr>
            <td>${ts}</td>
            <td>${fmtDuration(r.elapsed_secs)}</td>
            <td><span class="badge ${r.phase === 'complete' ? 'ok' : r.phase === 'failed' ? 'fail' : 'progress'}">${esc(r.phase)}</span></td>
            <td>${fmtInt(r.rows_transferred)} / ${fmtInt(r.rows_total)} (${pct}%)</td>
            <td>${Math.round(r.rate_per_sec || 0)}/s</td>
            <td>${r.tables_done}/${r.tables_total}</td>
            <td>${(r.cpu_percent || 0).toFixed(1)}%</td>
            <td>${(r.mem_alloc_mb || 0).toFixed(1)} MB</td>
          </tr>
        `;
      }).join('');
      cachedHistoryHTML = `
        <div style="overflow-x:auto;margin-top:10px;">
          <table class="stats-history-table">
            <thead>
              <tr><th>Time</th><th>Elapsed</th><th>Phase</th><th>Rows</th><th>Rate</th><th>Tables</th><th>CPU</th><th>Memory</th></tr>
            </thead>
            <tbody>${tableRows}</tbody>
          </table>
        </div>
      `;
      container.innerHTML = cachedHistoryHTML;
    } catch (err) {
      cachedHistoryHTML = `<div class="banner error">Failed to load history: ${esc(err.message)}</div>`;
      container.innerHTML = cachedHistoryHTML;
    }
  }

  async function tick() {
    if (stopped) return;
    try {
      const snap = await api.getStats(migId);
      renderProgress(snap);
      if (snap.Phase === 'complete' || snap.Phase === 'failed' || snap.Phase === 'cancelled') {
        clearTimer();
        return;
      }
    } catch (err) {
      const area = document.getElementById('progressArea');
      if (area) area.insertAdjacentHTML('afterbegin', errorBanner('Stats fetch failed: ' + err.message));
    }
    if (!stopped) timer = setTimeout(tick, 1500);
  }

  setCleanup(() => {
    stopped = true;
    clearTimer();
  });

  // For completed/failed/cancelled migrations, load from history instead of live stats
  const finishedStatuses = ['completed', 'failed', 'cancelled'];
  if (finishedStatuses.includes(mig.status)) {
    // Load last stats from history
    try {
      const records = await api.getStatsHistory(migId);
      if (records && records.length > 0) {
        const last = records[records.length - 1];
        // Build a snap-like object from the history record
        const snap = {
          Phase: last.phase,
          ElapsedSeconds: last.elapsed_secs,
          ETASeconds: 0,
          Rows: {
            Total: last.rows_total,
            Transferred: last.rows_transferred,
            RatePerSecond: last.rate_per_sec,
          },
          resource: {
            CPUPercent: last.cpu_percent,
            MemAllocMB: last.mem_alloc_mb,
            MemSysMB: last.mem_sys_mb,
            Goroutines: last.goroutines,
          },
          TableDetails: [], // Not stored in history records
          Errors: mig.error ? [mig.error] : [],
        };
        renderProgress(snap);
      } else {
        // No history, show minimal info from migration record
        const snap = {
          Phase: mig.status === 'completed' ? 'complete' : mig.status,
          ElapsedSeconds: 0,
          ETASeconds: 0,
          Rows: { Total: 0, Transferred: 0, RatePerSecond: 0 },
          resource: {},
          TableDetails: [],
          Errors: mig.error ? [mig.error] : [],
        };
        renderProgress(snap);
      }
    } catch (err) {
      const area = document.getElementById('progressArea');
      if (area) area.innerHTML = errorBanner('Failed to load migration stats: ' + err.message);
    }
  } else {
    // Active migration - use live polling
    tick();
  }
}

// ---------------------------------------------------------------------------
// Not found
// ---------------------------------------------------------------------------

function render404() {
  document.title = 'XferDB — Not found';
  app.innerHTML = `
    <div class="empty-state">
      <div class="icon">🤷</div>
      <p>Nothing here.</p>
      <a class="btn primary" href="#/">Back to projects</a>
    </div>
  `;
}

// ---------------------------------------------------------------------------
// Health banner (independent of the router; reflects the API/engine process,
// not this xferdb-web process, which stays up on its own regardless)
// ---------------------------------------------------------------------------

function startHealthPoll() {
  const statusEl = document.getElementById('healthStatus');
  if (!statusEl) return;
  let backoff = 4000;
  async function check() {
    try {
      await api.health();
      statusEl.textContent = 'API connected';
      statusEl.className = 'status ok';
      backoff = 4000;
    } catch (err) {
      statusEl.textContent = 'API unreachable — retrying…';
      statusEl.className = 'status down';
      backoff = Math.min(backoff * 1.5, 15000);
    } finally {
      setTimeout(check, backoff);
    }
  }
  check();
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

function router() {
  if (cleanup) {
    try { cleanup(); } catch (e) { /* ignore */ }
    cleanup = null;
  }
  const raw = location.hash.slice(1) || '/';
  const parts = raw.split('/').filter(Boolean).map(decodeURIComponent);
  window.scrollTo(0, 0);

  if (parts.length === 0) return renderProjectsList();
  if (parts[0] === 'projects') {
    if (parts[1] === 'new') return renderProjectCreate();
    const id = parts[1];
    if (!id) return renderProjectsList();
    if (parts[2] === 'settings') return renderProjectSettings(id);
    if (parts[2] === 'tables') {
      if (parts[3]) return renderTableDetail(id, parts[3]);
      return renderTablesList(id); // kept as an alias of the bare project route below
    }
    return renderTablesList(id); // project's default landing page is now its table list
  }
  if (parts[0] === 'migrations' && parts[1]) return renderMigrationProgress(parts[1]);
  return render404();
}

const brand = document.getElementById('brand');
if (brand) brand.addEventListener('click', () => navigate('#/'));
window.addEventListener('hashchange', router);
router();
startHealthPoll();
