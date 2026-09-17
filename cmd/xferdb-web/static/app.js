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
        <div>
          <div class="name">${esc(p.name)}</div>
          <div class="meta">${esc(p.source_config.type)} → ${esc(p.target_config.type)}${p.description ? ' · ' + esc(p.description) : ''} · created ${esc(fmtDate(p.created_at))}</div>
        </div>
        <div class="right"><button class="btn small danger" data-del="${esc(p.id)}">Delete</button></div>
      </div>
    `).join('');
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
        <div><label>Batch size</label><input type="number" id="p-batch" value="1000"></div>
        <div><label>Table workers</label><input type="number" id="p-workers" value="1"></div>
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
async function loadTables(id, project) {
  const area = document.getElementById('tablesArea');
  const startedAt = Date.now();
  const elapsed = () => fmtDuration(Math.floor((Date.now() - startedAt) / 1000));

  if (project.source_config.type === 'mongodb') {
    let current = null; // { collection, scanned, target }
    const stopTicker = startProgressTicker(area, () => current
      ? `Sampling ${current.collection}: ${fmtInt(current.scanned)} / ${fmtInt(current.target)} — ${elapsed()} elapsed`
      : `Connecting… ${elapsed()} elapsed`);
    const collections = [];
    try {
      await analyzeMongoStream(id, {}, (evt) => {
        if (evt.type === 'progress') {
          current = { collection: evt.collection, scanned: evt.scanned, target: evt.target };
        } else if (evt.type === 'collection') {
          const fields = (evt.schema && evt.schema.fields) || [];
          collections.push({ name: evt.collection, fieldCount: fields.length, warnings: evt.warnings || [] });
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
      badgeText: c.warnings.length ? 'warning' : `${c.fieldCount} fields`,
    }));
  }

  const stopTicker = startProgressTicker(area, () => `Analyzing — ${elapsed()} elapsed`);
  let result;
  try {
    result = await api.analyze(id, {});
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

  async function refreshTables() {
    if (locked) return;
    try {
      cachedItems = await loadTables(id, project);
      tablesCache.set(id, cachedItems);
      renderRows();
    } catch (err) {
      area.innerHTML = errorBanner('Analyze failed: ' + err.message, { retry: refreshTables });
    }
  }

  document.getElementById('reanalyzeBtn').addEventListener('click', refreshTables);

  const stopMig = mountMigSection(document.getElementById('migCardSection'), id, (isLocked, status) => {
    locked = isLocked;
    document.getElementById('lockBanner').innerHTML = locked
      ? `<div class="banner info">Table changes are disabled while a migration is ${esc(status)}.</div>`
      : '';
    const btn = document.getElementById('reanalyzeBtn');
    if (btn) btn.disabled = locked;
    renderRows();
  });

  setCleanup(() => stopMig());

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
  if (!['complete', 'failed'].includes(phase)) buttons.push('<button class="btn danger" id="cancelBtn">Cancel</button>');
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
      body: 'This stops the migration. Rows already written are kept, but the run is marked failed and cannot be resumed.',
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
// onLockChange(locked, status) fires whenever table rows should lock/unlock.
function mountMigSection(container, projectId, onLockChange) {
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
      mountActiveCard(active.id);
    } else {
      onLockChange(false, '');
      mountStartForm(migs);
    }
  }

  function mountActiveCard(migId) {
    container.innerHTML = `
      <div class="page-header">
        <h2>Migration</h2>
        <div class="actions-row" id="migActions"></div>
      </div>
      <div id="migCardBody"></div>
      <div class="actions-row"><a class="btn small" href="#/migrations/${encodeURIComponent(migId)}">View full progress</a></div>
    `;

    async function tick() {
      if (stopped) return;
      try {
        const snap = await api.getStats(migId);
        if (stopped) return;
        const body = document.getElementById('migCardBody');
        const actionsEl = document.getElementById('migActions');
        if (!body || !actionsEl) return;
        const rows = snap.Rows || {};
        const pct = rows.Total > 0 ? Math.round((rows.Transferred * 100) / rows.Total) : 0;
        const phaseBadge = snap.Phase === 'complete' ? 'ok' : snap.Phase === 'failed' ? 'fail' : snap.Phase === 'paused' ? 'warn' : 'progress';
        body.innerHTML = `
          <div class="stat-grid">
            <div class="stat-box"><div class="label">Phase</div><div class="value"><span class="badge ${phaseBadge}">${esc(snap.Phase)}</span></div></div>
            <div class="stat-box"><div class="label">Rows</div><div class="value">${fmtInt(rows.Transferred)} / ${fmtInt(rows.Total)} (${pct}%)</div></div>
            <div class="stat-box"><div class="label">ETA</div><div class="value">${snap.ETASeconds ? fmtDuration(snap.ETASeconds) : '—'}</div></div>
          </div>
          <div class="progress-bar"><div class="fill ${snap.Phase === 'complete' ? 'done' : snap.Phase === 'failed' ? 'failed' : ''}" style="width:${pct}%"></div></div>
        `;
        mountMigActions(actionsEl, migId, snap.Phase, {
          onDone: () => { clearTimer(); tick(); },
          errorArea: body,
        });
        if (['complete', 'failed'].includes(snap.Phase)) {
          clearTimer();
          render();
          return;
        }
      } catch (err) {
        const body = document.getElementById('migCardBody');
        if (body) body.insertAdjacentHTML('afterbegin', errorBanner('Stats fetch failed: ' + err.message));
      }
      if (!stopped) timer = setTimeout(tick, 1500);
    }
    tick();
  }

  function mountStartForm(migs) {
    container.innerHTML = `
      <h2>Migration</h2>
      <details class="advanced" open>
        <summary>Run a migration</summary>
        <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeNone" value="none" checked><label for="modeNone">Keep existing target schema/data</label></div>
        <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeTruncate" value="truncate"><label for="modeTruncate">Truncate target tables first (keep schema)</label></div>
        <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeRecreate" value="recreate"><label for="modeRecreate">Drop &amp; recreate target tables</label></div>
        <div class="field-row">
          <div><label>Table workers</label><input type="number" id="m-workers" value="1"></div>
          <div><label>Batch size</label><input type="number" id="m-batch" value="1000"></div>
        </div>
      </details>
      <div id="startErr"></div>
      <button class="btn primary" id="startMigBtn">Run Migration</button>
      <div style="margin-top:18px;">
        <details class="advanced"><summary>History</summary><div id="migHistory"></div></details>
      </div>
    `;
    document.getElementById('startMigBtn').addEventListener('click', async () => {
      const mode = document.querySelector('input[name=schemaMode]:checked').value;
      const body = {
        table_workers: parseInt(document.getElementById('m-workers').value, 10) || 1,
        batch_size: parseInt(document.getElementById('m-batch').value, 10) || 1000,
        recreate_schema: mode === 'recreate',
        truncate: mode === 'truncate',
      };
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
  return () => { stopped = true; clearTimer(); };
}

// ---------------------------------------------------------------------------
// Migration progress
// ---------------------------------------------------------------------------

function tableProgressRow(t) {
  const pct = t.Total > 0 ? Math.round((t.Transferred * 100) / t.Total) : 0;
  const icons = { done: '✓', in_progress: '●', failed: '✗', pending: '·' };
  const icon = icons[t.Status] || '○';
  const fillClass = t.Status === 'done' ? 'done' : t.Status === 'failed' ? 'failed' : '';
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
    const phaseBadge = snap.Phase === 'complete' ? 'ok' : snap.Phase === 'failed' ? 'fail' : snap.Phase === 'paused' ? 'warn' : 'progress';
    const area = document.getElementById('progressArea');
    if (!area) return;
    area.innerHTML = `
      ${snap.Phase === 'complete' ? '<div class="banner ok"><span>Migration complete.</span></div>' : ''}
      ${snap.Phase === 'failed' ? `<div class="banner error"><span>Migration failed.${snap.Errors && snap.Errors.length ? ' ' + esc(snap.Errors.join('; ')) : ''}</span></div>` : ''}
      <div class="stat-grid">
        <div class="stat-box"><div class="label">Phase</div><div class="value"><span class="badge ${phaseBadge}">${esc(snap.Phase)}</span></div></div>
        <div class="stat-box"><div class="label">Elapsed</div><div class="value">${fmtDuration(snap.ElapsedSeconds)}</div></div>
        <div class="stat-box"><div class="label">ETA</div><div class="value">${snap.ETASeconds ? fmtDuration(snap.ETASeconds) : '—'}</div></div>
        <div class="stat-box"><div class="label">Rate</div><div class="value">${Math.round(rows.RatePerSecond || 0)}/s</div></div>
      </div>
      <div class="card">
        <strong>Rows: ${fmtInt(rows.Transferred)} / ${fmtInt(rows.Total)} (${pct}%)</strong>
        <div class="progress-bar"><div class="fill ${snap.Phase === 'complete' ? 'done' : snap.Phase === 'failed' ? 'failed' : ''}" style="width:${pct}%"></div></div>
      </div>
      <div class="card">
        <strong>Tables</strong>
        ${(snap.TableDetails || []).map(tableProgressRow).join('') || '<div class="hint">No tables yet.</div>'}
      </div>
    `;
  }

  async function tick() {
    if (stopped) return;
    try {
      const snap = await api.getStats(migId);
      renderProgress(snap);
      if (snap.Phase === 'complete' || snap.Phase === 'failed') {
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
  tick();
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
