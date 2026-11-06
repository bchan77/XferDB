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

// intField reads a whole-number input: def when it is blank or not a
// number, and never below min. A number box can be typed or scrolled into a
// negative value (a project once saved table_workers -75), which
// parseInt(...) || def lets through.
function intField(id, def, min) {
  const el = document.getElementById(id);
  const n = el ? parseInt(el.value, 10) : NaN;
  if (Number.isNaN(n)) return def;
  return Math.max(n, min);
}

// The mouse wheel over a focused number box changes its value, easily
// unnoticed while scrolling the page: let the page scroll instead.
document.addEventListener('wheel', (e) => {
  const el = e.target;
  if (el instanceof HTMLInputElement && el.type === 'number' && document.activeElement === el) el.blur();
}, { passive: true });

// isPgFamily: the types that speak PostgreSQL's SQL (YugabyteDB's YSQL runs
// the postgres adapter), so they take the same connection fields.
function isPgFamily(type) {
  return type === 'postgres' || type === 'yugabyte';
}

function defaultPort(type) {
  return { mysql: '3306', yugabyte: '5433' }[type] || '5432';
}

function connTypeOptions(isTarget, selected) {
  const types = isTarget ? ['postgres', 'yugabyte', 'mysql', 'sqlite'] : ['postgres', 'yugabyte', 'mysql', 'sqlite', 'mongodb'];
  return types.map((t) => `<option value="${t}" ${t === selected ? 'selected' : ''}>${t}</option>`).join('');
}

function shortTail(s) {
  if (!s) return '';
  return s.length > 16 ? '…' + s.slice(-16) : s;
}

function subFieldsHTML(prefix, type, cfg, isTarget) {
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
  // Server and login first, then the database: once those are filled in,
  // "Load databases" lists the server's databases to pick from.
  const hasPassword = !!cfg.password;
  return `
    <div class="field-row">
      <div><label>Host</label><input type="text" id="${prefix}-host" value="${esc(cfg.host || '')}" placeholder="localhost"></div>
      <div><label>Port</label><input type="number" id="${prefix}-port" value="${cfg.port || ''}" placeholder="${defaultPort(type)}"></div>
    </div>
    <div class="field-row">
      <div><label>Username</label><input type="text" id="${prefix}-username" value="${esc(cfg.username || '')}"></div>
      <div>
        <label>Password</label>
        <input type="password" id="${prefix}-password" placeholder="${hasPassword ? 'leave blank to keep current password' : ''}">
      </div>
    </div>
    ${isPgFamily(type) ? `
    <div class="field-row">
      <div>
        <label>SSL mode</label>
        <select id="${prefix}-sslmode">
          ${['', 'disable', 'require', 'prefer', 'verify-ca', 'verify-full']
            .map((m) => `<option value="${m}" ${cfg.ssl_mode === m ? 'selected' : ''}>${m || '(default)'}</option>`).join('')}
        </select>
      </div>
      <div></div>
    </div>` : ''}
    <label>Database</label>
    <div class="db-picker">
      <span id="${prefix}-dbslot"><input type="text" id="${prefix}-database" value="${esc(cfg.database || '')}" placeholder="type a name, or load the list"></span>
      <button type="button" class="btn" id="${prefix}-loaddbs">Load databases</button>
    </div>
    <div id="${prefix}-dbstatus"></div>
    ${isPgFamily(type) ? `
    <div class="field-row">
      <div>
        <label>Schema</label>
        <input type="text" id="${prefix}-schema" value="${esc(cfg.schema || '')}" placeholder="all schemas">
        <div class="hint">${isTarget
          ? 'Leave empty to keep each table in its source schema; set one to put tables from the source\'s default schema there'
          : 'Leave empty to migrate every schema; set one to migrate only that schema'}</div>
      </div>
      <div></div>
    </div>` : ''}
  `;
}

// Wires the "Load databases" button of a postgres/mysql editor: it sends the
// fields typed so far to the API, which connects and lists the server's
// databases, then swaps the Database text box for a dropdown of them. A
// connection failure is shown under the field so the user can fix the
// host/port/credentials and retry. `opts.projectId`/`opts.side` let the
// settings form, which never shows the stored password, use it. On a target
// whose typed database isn't on the server, it offers to create it.
function wireDbPicker(container, prefix, readCfg, opts) {
  const btn = container.querySelector(`#${prefix}-loaddbs`);
  if (!btn) return;
  const slot = container.querySelector(`#${prefix}-dbslot`);
  const status = container.querySelector(`#${prefix}-dbstatus`);
  const textInput = (value) =>
    `<input type="text" id="${prefix}-database" value="${esc(value)}" placeholder="type a name, or load the list">`;

  btn.addEventListener('click', async () => {
    const cfg = readCfg();
    if (!cfg.host) {
      status.innerHTML = errorBanner('Enter the host first.');
      return;
    }
    btn.disabled = true;
    status.innerHTML = '<div class="hint"><span class="spinner"></span> Connecting…</div>';
    try {
      const res = await api.listDatabases({ config: cfg, project_id: opts.projectId, side: opts.side });
      const dbs = res.databases || [];
      if (!dbs.length) {
        status.innerHTML = '<div class="hint">Connected, but this login sees no databases. Type the name instead.</div>';
        return;
      }
      const current = cfg.database;
      const options = current && !dbs.includes(current) ? [current, ...dbs] : dbs;
      slot.innerHTML = `<select id="${prefix}-database">
        ${current ? '' : '<option value="">— choose a database —</option>'}
        ${options.map((d) => `<option value="${esc(d)}" ${d === current ? 'selected' : ''}>${esc(d)}</option>`).join('')}
      </select>`;
      status.innerHTML = `<div class="hint">Connected · ${dbs.length} database${dbs.length === 1 ? '' : 's'} found. <a href="#" id="${prefix}-dbmanual">Type a name instead</a></div>`;
      status.querySelector(`#${prefix}-dbmanual`).addEventListener('click', (e) => {
        e.preventDefault();
        slot.innerHTML = textInput(container.querySelector(`#${prefix}-database`).value);
        status.innerHTML = '';
      });
      if (current && !dbs.includes(current)) offerCreate(current);
    } catch (err) {
      status.innerHTML = errorBanner('Could not connect: ' + err.message);
      // Listing falls back to the server's maintenance database, so this
      // only happens when that is missing too, but the fix is the same.
      if (cfg.database && /3D000|Error 1049/.test(err.message)) offerCreate(cfg.database);
    } finally {
      btn.disabled = false;
    }
  });

  // Appends a "database doesn't exist yet" note with a Create button to the
  // status line; once created, the list is reloaded so it shows as chosen.
  function offerCreate(name) {
    if (!opts.isTarget) {
      status.insertAdjacentHTML('beforeend',
        `<div class="hint">"${esc(name)}" is not on this server — pick one from the list.</div>`);
      return;
    }
    status.insertAdjacentHTML('beforeend', `
      <div class="banner info">
        <span>"${esc(name)}" does not exist on this server yet.</span>
        <button type="button" class="btn small primary" id="${prefix}-createdb">Create database</button>
      </div>`);
    const createBtn = status.querySelector(`#${prefix}-createdb`);
    createBtn.addEventListener('click', async () => {
      createBtn.disabled = true;
      createBtn.textContent = 'Creating…';
      try {
        await api.createDatabase({ config: { ...readCfg(), database: name }, project_id: opts.projectId, side: opts.side });
        btn.click();
      } catch (err) {
        status.innerHTML = errorBanner('Could not create the database: ' + err.message);
      }
    });
  }
}

// Mounts a connection-config editor into `container`. Attaches a .read()
// method to the container element that returns the current ConnectionConfig.
function mountConnEditor(container, prefix, cfg, isTarget, opts = {}) {
  cfg = cfg || {};
  opts = { ...opts, isTarget };
  const type = cfg.type || 'postgres';
  container.innerHTML = `
    <label>Type</label>
    <select id="${prefix}-type">${connTypeOptions(isTarget, type)}</select>
    <div id="${prefix}-sub">${subFieldsHTML(prefix, type, cfg, isTarget)}</div>
  `;
  const typeSel = container.querySelector(`#${prefix}-type`);
  typeSel.value = type;
  typeSel.addEventListener('change', () => {
    container.querySelector(`#${prefix}-sub`).innerHTML = subFieldsHTML(prefix, typeSel.value, {}, isTarget);
    wireDbPicker(container, prefix, container.read, opts);
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
      schema: isPgFamily(t) ? g(`${prefix}-schema`).trim() : '',
      dsn: '',
    };
  };
  wireDbPicker(container, prefix, container.read, opts);
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
        <div><label>Batch size</label><input type="number" id="p-batch" value="1000" min="1"><div class="hint">Rows per INSERT batch</div></div>
        <div><label>Table workers</label><input type="number" id="p-workers" value="1" min="1"><div class="hint">Parallel tables to migrate</div></div>
      </div>
      <div class="field-row">
        <div id="p-segment-row"><label>Segment workers</label><input type="number" id="p-segment" value="0" min="0"><div class="hint" id="p-segment-hint">Workers per table (0 = disabled)</div></div>
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
        batch_size: intField('p-batch', 1000, 1),
        table_workers: intField('p-workers', 1, 1),
        segment_workers: intField('p-segment', 0, 0),
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
    if (isPgFamily(cfg.type)) rows.push(['Schema', cfg.schema || '(all schemas)']);
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
  mountConnEditor(srcEditor, 'esrc', project.source_config, false, { projectId: project.id, side: 'source' });
  mountConnEditor(tgtEditor, 'etgt', project.target_config, true, { projectId: project.id, side: 'target' });

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

function permBlock(label, info, perm, missingDb) {
  const errs = (perm && perm.errors) || [];
  return `
    <div class="card">
      <strong>${esc(label)}</strong>
      ${info ? `
        <table class="kv">
          <tr><td>Type</td><td>${esc(info.type)} ${esc(info.version)}</td></tr>
          ${info.host ? `<tr><td>Host</td><td>${esc(info.host)}</td></tr>` : ''}
          ${info.database ? `<tr><td>Database</td><td>${esc(info.database)}</td></tr>` : ''}
          ${info.schema ? `<tr><td>Schema</td><td>${esc(info.schema)}</td></tr>` : ''}
          ${(info.schemas || []).length ? `<tr><td>Schemas</td><td>${info.schemas.map(esc).join(', ')}</td></tr>` : ''}
          <tr><td>Tables</td><td>${esc(info.tables)}</td></tr>
          ${info.size_human ? `<tr><td>Size</td><td>${esc(info.size_human)}</td></tr>` : ''}
          ${info.ssl ? `<tr><td>SSL</td><td>${esc(info.ssl)}</td></tr>` : ''}
        </table>` : '<div class="hint">No connection info available.</div>'}
      ${info && !info.tables && (info.other_schemas || []).length ? `
        <div class="banner info"><span>No tables in schema "${esc(info.schema || 'public')}". Tables were found in: ${info.other_schemas.map(esc).join(', ')}. Clear <strong>Schema</strong> under Edit Connection to migrate every schema, or set it to one of these.</span></div>` : ''}
      ${perm ? `<div class="hint">read: ${perm.can_read ? '✓' : '✗'} · write: ${perm.can_write ? '✓' : '✗'} · create table: ${perm.can_create_table ? '✓' : '✗'}</div>` : ''}
      ${errs.map((e) => `<div class="banner error"><span>${esc(e)}</span></div>`).join('')}
      ${missingDb ? `
        <div class="actions-row">
          <button class="btn primary" id="createTargetDbBtn">Create database "${esc(missingDb)}"</button>
        </div>
        <div id="createTargetDbErr"></div>` : ''}
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
      ${permBlock('Target', result.target_info, result.target, result.target_missing_database)}
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
    const createBtn = document.getElementById('createTargetDbBtn');
    if (createBtn) {
      createBtn.addEventListener('click', async () => {
        createBtn.disabled = true;
        createBtn.textContent = 'Creating…';
        try {
          await api.createTargetDatabase(id);
          await runPreflight(id);
        } catch (err) {
          createBtn.disabled = false;
          createBtn.textContent = `Create database "${result.target_missing_database}"`;
          document.getElementById('createTargetDbErr').innerHTML = errorBanner('Could not create the database: ' + err.message);
        }
      });
    }
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
      <h2>Live Migration ${betaBadge()}</h2>
      <p>A live migration copies the data and then keeps streaming the source's changes to the target until you cut over, so the application keeps running during the copy. This checks whether this project's source and target can support one; start it from the project's migration form. Live migration is in beta: try it on a copy of your data before relying on it in production.</p>
      <button class="btn" id="liveCheckBtn">Check live migration readiness</button>
      <div id="liveCheckArea"></div>
    </div>
    <div class="section">
      <h2>Validation</h2>
      <p>After a migration completes, XferDB compares the source and target. These are the project's defaults; each migration run can override them.</p>
      ${validationOptionsHTML('s', project.transfer_config)}
      <div id="validationSaveArea"></div>
      <button class="btn" id="saveValidationBtn">Save validation settings</button>
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

  wireValidationOptions('s');
  document.getElementById('saveValidationBtn').addEventListener('click', async () => {
    const btn = document.getElementById('saveValidationBtn');
    const area = document.getElementById('validationSaveArea');
    btn.disabled = true;
    try {
      // Re-read the project so a connection edited on this page since it
      // loaded isn't overwritten with the copy fetched at page load.
      const fresh = await api.getProject(id);
      await api.updateProject(id, {
        name: fresh.name,
        description: fresh.description,
        source_config: fresh.source_config,
        target_config: fresh.target_config,
        transfer_config: { ...(fresh.transfer_config || {}), ...readValidationOptions('s') },
      });
      area.innerHTML = `<div class="banner ok"><span>Validation settings saved.</span></div>`;
    } catch (err) {
      area.innerHTML = errorBanner('Save failed: ' + err.message);
    }
    btn.disabled = false;
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
  document.getElementById('liveCheckBtn').addEventListener('click', () => runLiveCheck(id));

  await runPreflight(id);
}

// renderLiveChecks shows a live preflight's checks (#102): what passed,
// what fails and how to fix it, and what is only worth knowing.
function renderLiveChecks(live) {
  const checks = live.checks || [];
  const failing = checks.filter((c) => !c.ok && !c.warning).length;
  const rows = checks.map((c) => {
    const cls = c.ok ? (c.warning ? 'warn' : 'ok') : (c.warning ? 'warn' : 'fail');
    const mark = c.ok && !c.warning ? '✓' : (c.warning ? '!' : '✗');
    const fix = c.fix && (!c.ok || c.warning) ? `<div class="hint">${esc(c.fix)}</div>` : '';
    return `<tr data-check="${esc(c.name)}">
      <td><span class="badge ${cls}">${mark}</span></td>
      <td><strong>${esc(c.name)}</strong>${c.detail ? `<div class="hint">${esc(c.detail)}</div>` : ''}${fix}</td>
    </tr>`;
  }).join('');
  return `
    <div class="banner ${live.ready ? 'ok' : 'error'}" id="liveCheckSummary">
      <span>${live.ready
        ? 'Ready for live migration.'
        : `Not ready for live migration: ${failing} check${failing === 1 ? '' : 's'} failing.`}</span>
    </div>
    <div class="card"><table class="kv live-checks">${rows}</table></div>
    ${renderLeftovers(live.leftovers)}
  `;
}

// renderLeftovers lists live migration streams on the source that no running
// migration owns, each with a Drop button. Never dropped automatically:
// another XferDB server using the same source would own streams this one
// doesn't know.
function renderLeftovers(leftovers) {
  if (!leftovers || !leftovers.length) return '';
  const rows = leftovers.map((s) => {
    const parts = [s.slot && 'replication slot', s.publication && 'publication', s.heartbeat && 'heartbeat row'].filter(Boolean);
    const held = s.retained_bytes != null ? ` · holds ${fmtBytes(s.retained_bytes)} of WAL` : '';
    const drop = s.active
      ? '<span class="hint">in use: can\'t drop</span>'
      : `<button class="btn danger" data-drop-stream="${esc(s.name)}">Drop</button>`;
    return `<tr data-leftover="${esc(s.name)}">
      <td><strong>${esc(s.name)}</strong>
        <div class="hint">${esc(parts.join(', '))}${esc(held)}</div>
        <div class="hint">${esc(s.reason)}${s.project_name ? ` (project ${esc(s.project_name)})` : ''}</div></td>
      <td style="text-align:right">${drop}</td>
    </tr>`;
  }).join('');
  return `
    <h3 style="margin-top:16px">Leftover streams on the source</h3>
    <p class="hint">The source keeps every change for a replication slot until it is dropped. Drop one only if no other XferDB server uses it.</p>
    <div class="card"><table class="kv live-leftovers">${rows}</table></div>
  `;
}

async function dropLeftover(id, name) {
  const ok = await confirmModal({
    title: `Drop ${name}?`,
    body: `This drops the replication slot, publication and heartbeat named ${name} from the source. A migration still using them (on another XferDB server) can't go on afterwards.`,
    confirmLabel: 'Drop',
    danger: true,
  });
  if (!ok) return;
  const area = document.getElementById('liveCheckArea');
  area.querySelectorAll('[data-drop-stream]').forEach((b) => { b.disabled = true; });
  try {
    await api.dropLiveStream(id, name);
  } catch (err) {
    area.insertAdjacentHTML('afterbegin', errorBanner(`Couldn't drop ${name}: ${err.message}`));
    area.querySelectorAll('[data-drop-stream]').forEach((b) => { b.disabled = false; });
    return;
  }
  runLiveCheck(id);
}

async function runLiveCheck(id) {
  const area = document.getElementById('liveCheckArea');
  const btn = document.getElementById('liveCheckBtn');
  btn.disabled = true;
  area.innerHTML = `<div class="loading-block"><span class="spinner"></span> Checking…</div>`;
  try {
    const result = await api.preflight(id, 'live');
    area.innerHTML = result.live
      ? renderLiveChecks(result.live)
      : errorBanner('The server did not return live migration checks; it may be older than this page.');
    area.querySelectorAll('[data-drop-stream]').forEach((b) => {
      b.addEventListener('click', () => dropLeftover(id, b.dataset.dropStream));
    });
  } catch (err) {
    area.innerHTML = errorBanner('Live migration check failed: ' + err.message, { retry: () => runLiveCheck(id) });
  }
  btn.disabled = false;
}

// ---------------------------------------------------------------------------
// Validation (#103): compare a migration's source and target after the fact
// ---------------------------------------------------------------------------

// validationBadge describes a table's latest validation outcome for the table
// list. status is one entry of /validation-status "tables", or undefined when
// no validation has checked the table yet.
// progress, when given, is the table's entry of "in_progress": it is being
// validated right now, which outranks any earlier result.
function validationBadge(status, progress) {
  if (progress) {
    const pct = progress.rows_total > 0 ? ` · ${Math.floor((progress.rows_done / progress.rows_total) * 100)}%` : '';
    return { cls: 'progress', text: `validating${pct}`, title: validationProgressText(progress) };
  }
  if (!status) return { cls: 'cancelled', text: 'not validated', title: 'No validation has checked this table yet.' };
  if (status.stale) return { cls: 'warn', text: 'validation stale', title: 'Migrated again since it was last validated.' };
  if (status.result === 'pass') {
    return { cls: 'ok', text: `validated · ${status.level_run || 'L2'}`, title: 'Source and target match.' };
  }
  if (status.result === 'expected_lossy') {
    return { cls: 'warn', text: 'validated · lossy (plan)', title: status.detail || "Matches once the plan's lossy type overrides are applied." };
  }
  return { cls: 'fail', text: 'validation failed', title: status.detail || 'Source and target differ.' };
}

// validationOptionsHTML renders the Validation / Level selects used both on
// the migration start form (per-run override) and in project settings
// (project default). cfg is a transfer_config; empty fields mean the defaults
// (after, L2, every row).
function validationOptionsHTML(prefix, cfg) {
  const mode = (cfg && cfg.validation) || 'after';
  const level = (cfg && cfg.validation_level) || 'L2';
  const sample = (cfg && cfg.validation_sample) || 0;
  const sel = (cond) => (cond ? 'selected' : '');
  return `
    <div class="field-row">
      <div>
        <label for="${prefix}-validation">Validation</label>
        <select id="${prefix}-validation">
          <option value="after" ${sel(mode === 'after')}>After migration</option>
          <option value="per_table" ${sel(mode === 'per_table')}>As each table finishes</option>
          <option value="off" ${sel(mode === 'off')}>Skip</option>
        </select>
        <div class="hint">Compare source and target once the migration completes, or each table as soon as it is copied</div>
      </div>
      <div>
        <label for="${prefix}-validation-level">Validation level</label>
        <select id="${prefix}-validation-level">
          <option value="L2" ${sel(level === 'L2')}>L2 · compare every row</option>
          <option value="L1" ${sel(level === 'L1')}>L1 · row counts only</option>
          <option value="L3" ${sel(level === 'L3')}>L3 · L2, then list differing rows</option>
        </select>
        <div class="hint">L2 reads every row of both databases again; L3 also diffs a failing table by primary key</div>
      </div>
      <div>
        <label for="${prefix}-validation-sample">L3 sample (%)</label>
        <input id="${prefix}-validation-sample" type="number" min="0" max="100" step="1" value="${esc(sample)}">
        <div class="hint">Share of a failing table the L3 diff checks; 0 checks every row</div>
      </div>
    </div>`;
}

// wireValidationOptions disables the level select while validation is set
// to Skip, and the sample unless the level is L3, since neither would have
// any effect.
function wireValidationOptions(prefix) {
  const mode = document.getElementById(`${prefix}-validation`);
  const level = document.getElementById(`${prefix}-validation-level`);
  const sample = document.getElementById(`${prefix}-validation-sample`);
  if (!mode || !level) return;
  const sync = () => {
    level.disabled = mode.value === 'off';
    if (sample) sample.disabled = level.disabled || level.value !== 'L3';
  };
  mode.addEventListener('change', sync);
  level.addEventListener('change', sync);
  sync();
}

// readSample returns the sample input's percentage, clamped to 0-100.
function readSample(id) {
  const n = parseInt(document.getElementById(id)?.value, 10);
  return Number.isFinite(n) ? Math.min(100, Math.max(0, n)) : 0;
}

function readValidationOptions(prefix) {
  return {
    validation: document.getElementById(`${prefix}-validation`).value,
    validation_level: document.getElementById(`${prefix}-validation-level`).value,
    validation_sample: readSample(`${prefix}-validation-sample`),
  };
}

// validationResultsHTML renders a run's per-table results. partial (a
// per_table run still going) leaves out the overall verdict banner.
function validationResultsHTML(v, { partial = false } = {}) {
  const tables = v.tables || [];
  const failed = tables.filter((t) => t.result === 'fail').length;
  const lossy = tables.filter((t) => t.result === 'expected_lossy').length;
  const checked = tables.filter((t) => ['pass', 'fail', 'expected_lossy'].includes(t.result)).length;
  let banner = '';
  if (partial) {
    banner = '';
  } else if (v.status === 'failed') {
    banner = `<div class="banner error"><span>Validation could not finish: ${esc(v.error || 'unknown error')}</span></div>`;
  } else if (v.status === 'cancelled') {
    banner = `<div class="banner info"><span>Validation cancelled.</span></div>`;
  } else if (failed > 0) {
    banner = `<div class="banner error"><span>${failed} of ${checked} table(s) differ between source and target.</span></div>`;
  } else if (lossy > 0) {
    banner = `<div class="banner ok"><span>All ${checked} validated table(s) match. In ${lossy} of them, the plan's lossy type overrides changed some values, as expected.</span></div>`;
  } else {
    banner = `<div class="banner ok"><span>All ${checked} validated table(s) match.</span></div>`;
  }
  const badgeFor = (t) => {
    const map = {
      pass: ['ok', 'pass'], fail: ['fail', 'FAIL'], skipped: ['cancelled', 'skipped'],
      expected_lossy: ['warn', 'lossy (plan)'],
    };
    const [cls, text] = map[t.result] || ['pending', t.result];
    return `<span class="badge ${cls}">${esc(text)}${t.stale ? ' (stale)' : ''}</span>`;
  };
  const count = (n) => (n === undefined || n === null ? '—' : fmtInt(n));
  const hasDiffs = (t) => (t.diff_missing || 0) + (t.diff_extra || 0) + (t.diff_mismatch || 0) + (t.lossy_rows || 0) > 0;
  return `
    ${banner}
    <div style="overflow-x:auto;margin-top:10px;">
      <table class="stats-history-table">
        <thead><tr><th>Table</th><th>Result</th><th>Source rows</th><th>Target rows</th><th>Detail</th><th></th></tr></thead>
        <tbody>
          ${tables.map((t) => `
            <tr data-table="${esc(t.table_name)}">
              <td class="mono">${esc(t.table_name)}</td>
              <td>${badgeFor(t)}${t.level_run ? ` <span class="hint">${esc(t.level_run)}</span>` : ''}</td>
              <td>${count(t.source_rows)}</td>
              <td>${count(t.target_rows)}</td>
              <td>${esc(t.detail || '')}${t.level_note ? `<div class="level-note">${esc(t.level_note)}</div>` : ''}</td>
              <td>${hasDiffs(t) ? `<button class="btn small" data-diffs="${esc(t.table_name)}">View rows</button>` : ''}</td>
            </tr>`).join('')}
        </tbody>
      </table>
    </div>`;
}

// wireDiffButtons opens the diff drawer from each "View rows" button that
// validationResultsHTML rendered inside container.
function wireDiffButtons(container, v) {
  container.querySelectorAll('[data-diffs]').forEach((btn) => {
    btn.addEventListener('click', () => {
      const t = (v.tables || []).find((x) => x.table_name === btn.dataset.diffs);
      if (t) openDiffDrawer(v.id, t);
    });
  });
}

const DIFF_KINDS = {
  missing_in_target: { label: 'Missing in target', cls: 'fail' },
  extra_in_target: { label: 'Only in target', cls: 'warn' },
  value_mismatch: { label: 'Values differ', cls: 'fail' },
  lossy_by_plan: { label: 'Changed by plan', cls: 'warn' },
};

function diffValueHTML(v) {
  return v === null || v === undefined
    ? '<span class="diff-value null">NULL</span>'
    : `<span class="diff-value">${esc(v)}</span>`;
}

function diffKeyHTML(key) {
  return Object.keys(key || {}).sort()
    .map((k) => `<span class="mono">${esc(k)}</span>=${diffValueHTML(key[k])}`).join(' ');
}

// diffRowHTML renders one example: the differing columns of a mismatch side
// by side, or the whole row for a row on only one side.
function diffRowHTML(d) {
  const kind = DIFF_KINDS[d.kind] || { label: d.kind, cls: 'pending' };
  let body;
  if (d.kind === 'value_mismatch' || d.kind === 'lossy_by_plan') {
    const rowCls = d.kind === 'value_mismatch' ? 'diff-changed' : 'diff-planned';
    body = `<table class="stats-history-table"><thead><tr><th>Column</th><th>Source</th><th>Target</th></tr></thead><tbody>
      ${(d.columns || []).map((c) => `<tr class="${rowCls}"><td class="mono">${esc(c)}</td><td>${diffValueHTML(d.source?.[c])}</td><td>${diffValueHTML(d.target?.[c])}</td></tr>`).join('')}
    </tbody></table>`;
  } else {
    const row = d.kind === 'missing_in_target' ? d.source : d.target;
    body = Object.keys(row || {}).sort()
      .map((c) => `<span class="mono">${esc(c)}</span>: ${diffValueHTML(row[c])}`).join(' · ');
  }
  return `<tr data-kind="${esc(d.kind)}">
    <td class="diff-kind"><span class="badge ${kind.cls}">${esc(kind.label)}</span></td>
    <td>${diffKeyHTML(d.key)}</td>
    <td>${body}</td>
  </tr>`;
}

// openDiffDrawer lists the example rows an L3 diff recorded for table t of
// validation vid.
async function openDiffDrawer(vid, t) {
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  const totals = [
    t.diff_missing ? `${fmtInt(t.diff_missing)} missing in target` : '',
    t.diff_extra ? `${fmtInt(t.diff_extra)} only in target` : '',
    t.diff_mismatch ? `${fmtInt(t.diff_mismatch)} with different values` : '',
    t.lossy_rows ? `${fmtInt(t.lossy_rows)} changed by the plan's type overrides, as expected` : '',
  ].filter(Boolean).join(' · ');
  overlay.innerHTML = `
    <div class="modal drawer" role="dialog" aria-modal="true" aria-label="Differing rows" id="diffDrawer">
      <h3>Differing rows · <span class="mono">${esc(t.table_name)}</span></h3>
      <div class="hint">${esc(totals)}${t.sample ? ` · in a ${esc(t.sample)}% sample` : ''}. Values are shown as compared, after normalising each database's representation.</div>
      <div class="drawer-body" style="margin-top:10px;"><div class="loading-block"><span class="spinner"></span> Loading…</div></div>
      <div class="actions" style="margin-top:12px;"><button class="btn" data-act="close">Close</button></div>
    </div>`;
  document.body.appendChild(overlay);
  const close = () => { document.removeEventListener('keydown', onKey); overlay.remove(); };
  const onKey = (e) => { if (e.key === 'Escape') close(); };
  overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });
  overlay.querySelector('[data-act=close]').addEventListener('click', close);
  document.addEventListener('keydown', onKey);

  const body = overlay.querySelector('.drawer-body');
  try {
    const diffs = await api.getValidationDiffs(vid, t.table_name);
    const total = (t.diff_missing || 0) + (t.diff_extra || 0) + (t.diff_mismatch || 0) + (t.lossy_rows || 0);
    body.innerHTML = `
      ${diffs.length < total ? `<div class="hint">Showing the first ${fmtInt(diffs.length)} of ${fmtInt(total)}.</div>` : ''}
      <table class="stats-history-table" id="diffTable">
        <thead><tr><th>Kind</th><th>Key</th><th>Detail</th></tr></thead>
        <tbody>${diffs.map(diffRowHTML).join('')}</tbody>
      </table>`;
  } catch (err) {
    body.innerHTML = errorBanner('Could not load the differing rows: ' + err.message);
  }
}

const VALIDATION_STEPS = {
  counting: 'Counting rows',
  reading_source: 'Reading source',
  reading_target: 'Reading target',
  row_diff: 'Diffing rows by key',
};

// validationProgressText is one line on how far a table's validation has got.
function validationProgressText(p) {
  const step = VALIDATION_STEPS[p.step] || p.step;
  if (!(p.rows_total > 0)) return `${step}…`;
  return `${step} · ${fmtInt(p.rows_done)} / ${fmtInt(p.rows_total)} rows (${Math.floor((p.rows_done / p.rows_total) * 100)}%)`;
}

// validationProgressHTML shows each table being validated right now: its
// step, and while reading both sides (L2), rows read of rows to read with a
// bar and an ETA from the table's own read rate so far.
function validationProgressHTML(items) {
  if (!items || !items.length) return '';
  return `<div class="validation-progress">${items.map((p) => {
    const step = VALIDATION_STEPS[p.step] || p.step;
    const elapsed = Math.max(0, (Date.now() - new Date(p.started_at).getTime()) / 1000);
    let detail = `${esc(step)}… · ${esc(fmtDuration(elapsed))}`;
    let bar = '';
    if (p.rows_total > 0) {
      const pct = Math.min(100, (p.rows_done / p.rows_total) * 100);
      const rate = elapsed > 0 ? p.rows_done / elapsed : 0;
      const eta = rate > 0 && p.rows_done < p.rows_total ? (p.rows_total - p.rows_done) / rate : 0;
      detail = `${esc(step)} · ${fmtInt(p.rows_done)} / ${fmtInt(p.rows_total)} rows (${pct.toFixed(0)}%)` +
        `${rate > 0 ? ` · ${fmtInt(Math.round(rate))}/s` : ''}${eta > 0 ? ` · about ${esc(fmtDuration(eta))} left` : ''}`;
      bar = `<div class="progress-bar"><div class="fill" style="width:${pct.toFixed(1)}%"></div></div>`;
    }
    return `<div class="validation-progress-row" data-table="${esc(p.table_name)}">
      <div><span class="mono">${esc(p.table_name)}</span> <span class="hint">${detail}</span></div>
      ${bar}
    </div>`;
  }).join('')}</div>`;
}

// mountValidationCard shows a migration's latest validation and lets the user
// start one. waitForAuto keeps polling briefly for the validation the server
// starts on its own right after a migration completes, instead of reporting
// "not validated" in the gap before it appears. Returns a stop function.
function mountValidationCard(container, migId, { waitForAuto = false } = {}) {
  let stopped = false;
  let timer = null;
  const autoDeadline = waitForAuto ? Date.now() + 15000 : 0;

  function card(body) {
    container.innerHTML = `<div class="card" id="validationCard"><strong>Validation</strong><div style="margin-top:8px;">${body}</div></div>`;
  }

  function startControlsHTML(label) {
    return `
      <div class="actions-row" style="margin-top:10px;">
        <select id="v-level" style="width:auto;">
          <option value="">Project default level</option>
          <option value="L2">L2 · compare every row</option>
          <option value="L1">L1 · row counts only</option>
          <option value="L3">L3 · L2, then list differing rows</option>
        </select>
        <label for="v-sample" class="hint" style="align-self:center;">L3 sample %</label>
        <input id="v-sample" type="number" min="0" max="100" step="1" value="0" style="width:80px;" disabled>
        <button class="btn" id="validateBtn">${esc(label)}</button>
      </div>
      <div id="validateErr"></div>`;
  }

  function wireStart() {
    const btn = document.getElementById('validateBtn');
    if (!btn) return;
    const levelSel = document.getElementById('v-level');
    const sampleIn = document.getElementById('v-sample');
    levelSel.addEventListener('change', () => { sampleIn.disabled = levelSel.value !== 'L3'; });
    btn.addEventListener('click', async () => {
      const level = levelSel.value;
      const body = level ? { level } : {};
      if (level === 'L3') body.sample = readSample('v-sample');
      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Starting…';
      try {
        await api.startValidation(migId, body);
        poll();
      } catch (err) {
        document.getElementById('validateErr').innerHTML = errorBanner('Could not start validation: ' + err.message);
        btn.disabled = false;
        btn.textContent = 'Validate now';
      }
    });
  }

  async function poll() {
    if (stopped) return;
    clearTimeout(timer);
    let latest;
    try {
      const vs = await api.listValidations(migId);
      latest = vs && vs.length ? await api.getValidation(vs[0].id) : null;
    } catch (err) {
      card(errorBanner('Could not load validation: ' + err.message, { retry: poll }));
      return;
    }
    if (stopped) return;

    if (!latest) {
      if (Date.now() < autoDeadline) {
        card('<div class="loading-block"><span class="spinner"></span> Waiting for validation to start…</div>');
        timer = setTimeout(poll, 1500);
        return;
      }
      card(`<div class="hint">This migration has not been validated (validation was skipped, or the migration did not complete).</div>${startControlsHTML('Validate now')}`);
      wireStart();
      return;
    }

    if (latest.status === 'pending' || latest.status === 'in_progress') {
      const done = (latest.tables || []).filter((t) => t.result !== 'skipped').length;
      const perTable = latest.options && latest.options.mode === 'per_table';
      card(`
        <div class="loading-block"><span class="spinner"></span> ${perTable
          ? `Validating each table as it finishes (${esc(latest.level)})… ${done} table(s) checked so far`
          : `Validating (${esc(latest.level)})… ${done} table(s) checked`}</div>
        ${validationProgressHTML(latest.in_progress)}
        ${done ? validationResultsHTML(latest, { partial: true }) : ''}
        <div class="actions-row"><button class="btn small" id="cancelValidationBtn">Cancel validation</button></div>`);
      wireDiffButtons(container, latest);
      document.getElementById('cancelValidationBtn').addEventListener('click', async () => {
        try { await api.cancelValidation(latest.id); } catch (err) { /* finished meanwhile; next poll shows it */ }
        poll();
      });
      timer = setTimeout(poll, 2000);
      return;
    }

    card(`
      <div class="hint">Level ${esc(latest.level)}${latest.options && latest.options.mode === 'per_table' ? ' · each table as it finished' : ''} · ${esc(latest.status)} ${latest.completed_at ? '· ' + esc(fmtDate(latest.completed_at)) : ''}</div>
      ${validationResultsHTML(latest)}
      ${startControlsHTML('Validate again')}`);
    wireDiffButtons(container, latest);
    wireStart();
  }

  poll();
  return () => { stopped = true; clearTimeout(timer); };
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
      <div class="right">
        <span class="badge ${t.badge}">${esc(t.badgeText)}</span>
        ${t.validation ? `<span class="badge ${t.validation.cls}" title="${esc(t.validation.title)}">${esc(t.validation.text)}</span>` : ''}
      </div>
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
      <div class="page-header">
        <h2>${label}</h2>
        <div class="actions-row">
          <button class="btn" id="validateNowBtn" title="Compare the latest migration's source and target">Validate now</button>
          <button class="btn" id="reanalyzeBtn">Re-analyze</button>
        </div>
      </div>
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
      <div id="validationBanner"></div>
      <div id="tablesArea"></div>
    </div>
  `;

  const area = document.getElementById('tablesArea');
  let cachedItems = tablesCache.get(id) || [];
  let locked = false;

  // Latest validation result per table (from /validation-status), and the
  // project's running validation, if any.
  let validationByTable = new Map();
  let progressByTable = new Map();
  let activeValidation = null;
  let validationTimer = null;

  function renderRows() {
    const items = cachedItems.map((t) => ({
      ...t, validation: validationBadge(validationByTable.get(t.name), progressByTable.get(t.name)),
    }));
    renderTableCards(area, id, items, locked);
  }

  function syncValidateButton() {
    const btn = document.getElementById('validateNowBtn');
    if (btn) btn.disabled = locked || !!activeValidation;
  }

  async function loadValidationStatus() {
    clearTimeout(validationTimer);
    let status;
    try {
      status = await api.getValidationStatus(id);
    } catch (err) {
      return; // badges are secondary; keep the last known ones
    }
    validationByTable = new Map((status.tables || []).map((t) => [t.table_name, t]));
    progressByTable = new Map((status.in_progress || []).map((p) => [p.table_name, p]));
    activeValidation = status.active || null;
    const banner = document.getElementById('validationBanner');
    if (banner) {
      const now = (status.in_progress || []).map((p) => `<span class="mono">${esc(p.table_name)}</span>: ${esc(validationProgressText(p))}`).join(' · ');
      banner.innerHTML = activeValidation
        ? `<div class="banner info"><span><span class="spinner"></span> Validating (${esc(activeValidation.level)})… ${now || 'results appear here as each table finishes.'} <a href="#/migrations/${encodeURIComponent(activeValidation.migration_id)}">View</a></span></div>`
        : '';
    }
    syncValidateButton();
    renderRows();
    // Keep checking while a validation runs, and (slower) while a migration
    // runs, since a per_table validation starts partway through.
    if (activeValidation || locked) validationTimer = setTimeout(loadValidationStatus, activeValidation ? 3000 : 5000);
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
    const wasLocked = locked;
    locked = isLocked;
    syncValidateButton();
    // A migration started: a per_table validation may begin mid-run. Or one
    // just finished: the server may have started validating it.
    if (wasLocked !== isLocked) loadValidationStatus();
    document.getElementById('lockBanner').innerHTML = locked
      ? `<div class="banner info">Table changes are disabled while a migration is ${esc(status)}.</div>`
      : '';
    const btn = document.getElementById('reanalyzeBtn');
    if (btn) btn.disabled = locked;
    renderRows();
  }, project.transfer_config);

  async function refreshTables() {
    if (locked) return;
    try {
      const analyzeOpts = readAnalyzeOpts();
      cachedItems = await loadTables(id, project, analyzeOpts);
      tablesCache.set(id, cachedItems);
      renderRows();
      // Fill in the table checkboxes. Deliberately not a full refresh(): that
      // rebuilds the form and would wipe anything already entered in it.
      migSection.updateTables();
    } catch (err) {
      area.innerHTML = errorBanner('Analyze failed: ' + err.message, { retry: refreshTables });
    }
  }

  document.getElementById('reanalyzeBtn').addEventListener('click', refreshTables);

  document.getElementById('validateNowBtn').addEventListener('click', async () => {
    const banner = document.getElementById('validationBanner');
    const btn = document.getElementById('validateNowBtn');
    btn.disabled = true;
    try {
      const migs = await api.listMigrations(id);
      if (!migs || migs.length === 0) {
        banner.innerHTML = `<div class="banner info"><span>Run a migration first; validation compares what it copied.</span></div>`;
        syncValidateButton();
        return;
      }
      await api.startValidation(migs[0].id, {});
      await loadValidationStatus();
    } catch (err) {
      banner.innerHTML = errorBanner('Could not start validation: ' + err.message);
      syncValidateButton();
    }
  });

  setCleanup(() => { migSection.stop(); clearTimeout(validationTimer); });
  loadValidationStatus();

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
function mountMigActions(actionsEl, migId, phase, { onDone, errorArea, live = false } = {}) {
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
      title: live ? 'Cancel live migration?' : 'Cancel migration?',
      body: live
        ? 'This stops streaming changes and drops the replication slot on the source. Rows already written to the target are kept, but the run is marked cancelled and cannot be resumed.'
        : 'This stops the migration. Rows already written are kept, but the run is marked cancelled and cannot be resumed.',
      confirmLabel: 'Cancel Migration',
      danger: true,
    });
    if (!ok) return;
    await doAction('cancel');
  });
}

// ---------------------------------------------------------------------------
// Live migration (#102)
// ---------------------------------------------------------------------------

const LIVE_PHASES = [
  ['setup', 'Setup'],
  ['snapshot', 'Snapshot'],
  ['catch_up', 'Catch-up'],
  ['streaming', 'Streaming'],
  ['cutover', 'Cutover'],
];

function fmtBytes(n) {
  n = n || 0;
  if (n < 1024) return `${n} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v.toFixed(1)} ${units[i]}`;
}

// betaBadge marks live migration, which is in beta, wherever it is offered
// or shown.
function betaBadge() {
  return '<span class="badge beta" title="Live migration is in beta: try it on a copy of your data before relying on it in production.">Beta</span>';
}

// livePhaseStrip shows where a live migration is: the phases it passed,
// the current one, and those still to come.
function livePhaseStrip(phase) {
  // Completed: every phase, cutover included, is behind it.
  const at = phase === 'completed' ? LIVE_PHASES.length : LIVE_PHASES.findIndex(([key]) => key === phase);
  return `<ol class="phase-strip" data-phase="${esc(phase || '')}">${LIVE_PHASES.map(([key, label], i) => {
    const cls = i < at ? 'done' : i === at ? 'current' : 'todo';
    return `<li class="${cls}" data-phase="${key}">${esc(label)}</li>`;
  }).join('')}</ol>`;
}

// liveStatusHTML is a live migration's stream: phase strip, lag and the
// workers' state, from GET /migrations/:id.
function liveStatusHTML(mig) {
  const l = mig.live;
  const phase = mig.phase || 'setup';
  let body = livePhaseStrip(phase);
  if (l && l.schema_change) {
    body += schemaChangeHTML(l.schema_change);
  } else if (mig.status === 'paused' && ['catch_up', 'streaming', 'cutover'].includes(phase)) {
    body += `<div class="banner info"><span>Paused: changes are still captured from the source and queued here; the target catches up when you resume.</span></div>`;
  }
  for (const w of (l && l.warnings) || []) {
    body += `<div class="banner warn" data-warning><span>${esc(w)}</span></div>`;
  }
  if (!l) {
    if (['setup', 'snapshot'].includes(phase)) {
      body += `<div class="hint">The change stream starts before the copy: every change made while the tables are copied is replayed once the copy is done.</div>`;
    }
    return body;
  }
  const secs = l.lag_seconds == null ? '—' : (l.lag_seconds < 1 ? '< 1s' : fmtDuration(l.lag_seconds));
  const applied = l.apply ? fmtInt(l.apply.changes_applied) : '—';
  body += `
    <div class="stat-grid live-lag">
      <div class="stat-box" data-stat="lag-changes"><div class="label">Changes to apply</div><div class="value">${fmtInt(l.lag_changes)}</div></div>
      <div class="stat-box" data-stat="lag-seconds"><div class="label">Apply lag</div><div class="value">${esc(secs)}</div></div>
      <div class="stat-box" data-stat="lag-bytes"><div class="label">Behind source</div><div class="value">${esc(l.behind_seconds != null ? (l.behind_seconds < 1 ? '< 1s' : fmtDuration(l.behind_seconds)) : fmtBytes(l.lag_bytes))}</div></div>
      <div class="stat-box" data-stat="applied"><div class="label">Applied</div><div class="value">${applied}</div></div>
      <div class="stat-box" data-stat="queue"><div class="label">Queue on disk</div><div class="value">${esc(fmtBytes(l.queue_bytes))}</div></div>
      ${l.retention ? `<div class="stat-box" data-stat="retained" title="WAL the source keeps for this migration's replication slot"><div class="label">WAL held on source</div><div class="value">${esc(fmtBytes(l.retention.retained_bytes))}</div></div>` : ''}
    </div>`;
  if (phase === 'catch_up') {
    body += `<div class="hint">Catching up: replaying what was written during the copy, up to ${esc(l.catch_up_pos || 'the position recorded when it ended')}. Unique indexes are added once it is done.</div>`;
  } else if (phase === 'streaming' && mig.status !== 'paused' && !(l.cutover && l.cutover.running)) {
    body += `<div class="hint">The target follows the source. To switch over, stop every application writing to the source, then cut over: the rest of the changes are applied, sequences set, foreign keys and checks added, and the replication slot dropped.</div>`;
  }
  if (l.cutover) body += cutoverHTML(l.cutover);
  if (l.capture_error) body += errorBanner('Capture worker: ' + l.capture_error + ' (restarting)');
  if (l.apply_error) body += errorBanner('Apply worker: ' + l.apply_error + ' (restarting)');
  return body;
}

// schemaChangeHTML explains a pause at a schema change on the source.
function schemaChangeHTML(sc) {
  const when = sc.commit_at ? ` at ${esc(new Date(sc.commit_at).toLocaleString())}` : '';
  return `<div class="schema-change" data-table="${esc(sc.table)}">
    <strong>Paused at a schema change on the source</strong>
    <div>Table <code>${esc(sc.table)}</code> changed${when}:</div>
    <ul>${(sc.summary || []).map((line) => `<li>${esc(line)}</li>`).join('')}</ul>
    <div class="hint">Every change before it is applied; none after it, for any table, until you resume. Make the same change on the target first, then resume: the changes after it are written to the target as it is then. Cut over is not possible meanwhile.</div>
  </div>`;
}

// cutoverHTML shows a cutover's steps, and why it stopped if it did.
function cutoverHTML(c) {
  const icons = { done: '✓', skipped: '–', failed: '✗', running: '●', pending: '·' };
  let html = `<div class="cutover" data-running="${c.running ? 'true' : 'false'}">
    <strong>${c.running ? 'Cutting over' : c.error ? 'Cutover stopped' : 'Cutover'}</strong>
    <ol class="cutover-steps">${(c.steps || []).map((s) => `
      <li class="${esc(s.state)}" data-step="${esc(s.name)}"><span class="status-icon">${icons[s.state] || '·'}</span>${esc(s.label)}${s.detail ? `<div class="hint">${esc(s.detail)}</div>` : ''}</li>`).join('')}
    </ol>`;
  if (!c.running && c.error) {
    // The failed step shows the reason; repeat it only if no step does.
    if (!(c.steps || []).some((s) => s.state === 'failed' && s.detail)) html += errorBanner(c.error);
    if (c.writes_after > 0) {
      html += `<div class="actions-row"><button class="btn" id="cutoverRetryBtn">Cut over again</button><button class="btn danger" id="cutoverAnywayBtn">Cut over anyway</button></div>`;
    }
  }
  return html + '</div>';
}

// mountLiveActions puts a live migration's controls in actionsEl: pause,
// resume and cancel as for any migration, plus Cut over once it streams,
// and wires the cutover card's retry buttons in area.
function mountLiveActions(actionsEl, area, migId, mig, onDone) {
  const phase = mig.phase || '';
  const cutting = phase === 'cutover' || !!(mig.live && mig.live.cutover && mig.live.cutover.running);
  // A running cutover can be cancelled but not paused.
  const state = mig.status === 'paused' ? 'paused' : cutting ? 'cutting_over' : 'in_progress';
  mountMigActions(actionsEl, migId, state, { onDone, errorArea: area, live: true });
  // Resuming past a schema change accepts it: ask first.
  const sc = mig.live && mig.live.schema_change;
  const resume = actionsEl.querySelector('#resumeBtn');
  if (sc && resume) {
    const fresh = resume.cloneNode(true);
    resume.replaceWith(fresh);
    fresh.addEventListener('click', async () => {
      const ok = await confirmModal({
        title: 'Resume past the schema change?',
        body: `Make sure ${sc.table} on the target has the same change (${(sc.summary || []).join('; ')}). Changes after it are then applied to the target as it is.`,
        confirmLabel: 'Resume',
      });
      if (!ok) return;
      try {
        await api.patchMigration(migId, 'resume');
        onDone();
      } catch (err) {
        area.insertAdjacentHTML('afterbegin', errorBanner('resume failed: ' + err.message));
      }
    });
  }
  const doCutover = async (ignoreWrites) => {
    try {
      await api.cutover(migId, ignoreWrites);
      onDone();
    } catch (err) {
      area.insertAdjacentHTML('afterbegin', errorBanner('Cutover failed to start: ' + err.message));
    }
  };
  if (phase === 'streaming' && mig.status === 'in_progress' && !cutting) {
    actionsEl.insertAdjacentHTML('afterbegin', '<button class="btn primary" id="cutoverBtn">Cut over</button>');
    actionsEl.querySelector('#cutoverBtn').addEventListener('click', async () => {
      const ok = await confirmModal({
        title: 'Cut over to the target?',
        body: 'Stop every application writing to the source first. The cutover applies the remaining changes, checks for a few seconds that no more arrive, sets the target\'s sequences, adds foreign keys and checks, drops the replication slot and completes the migration, which is then validated.',
        confirmLabel: 'Cut over',
      });
      if (ok) doCutover(false);
    });
  }
  const retry = area && area.querySelector('#cutoverRetryBtn');
  if (retry) retry.addEventListener('click', () => doCutover(false));
  const anyway = area && area.querySelector('#cutoverAnywayBtn');
  if (anyway) anyway.addEventListener('click', async () => {
    const ok = await confirmModal({
      title: 'Cut over while writes continue?',
      body: 'Changes still arriving are applied once more, then the stream stops: anything written to the source after that is not migrated.',
      confirmLabel: 'Cut over anyway',
      danger: true,
    });
    if (ok) doCutover(true);
  });
}

// mountMigSection renders the migration status/controls card on a project's
// tables screen: a live-polling progress card with Pause/Resume/Cancel while
// a migration is pending/in_progress/paused, or a "Run Migration" form plus
// history otherwise. Returns a cleanup function that stops polling.
// getTables() returns the current list of tables for selection.
// onLockChange(locked, status) fires whenever table rows should lock/unlock.
// transferCfg is the project's transfer_config; it supplies the defaults for
// the start form's validation selects.
function mountMigSection(container, projectId, sourceType, targetType, getTables, onLockChange, transferCfg) {
  let stopped = false;
  let timer = null;
  // Set while the start form is mounted; refills just its table-selection block.
  let updateTableSelect = null;
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
    updateTableSelect = null;
    if (active) {
      onLockChange(true, active.status);
      if (active.mode === 'live') mountLiveCard(active.id);
      else mountActiveCard(active.id, active.status);
    } else {
      onLockChange(false, '');
      mountStartForm(migs, sourceType, targetType);
    }
  }

  // mountLiveCard is the active card of a live migration: its phase, lag
  // and controls, polled from the migration record, which, unlike the
  // stats collector, survives a server restart.
  function mountLiveCard(migId) {
    container.innerHTML = `
      <div class="page-header">
        <h2>Live migration ${betaBadge()}</h2>
        <div class="actions-row" id="migActions"></div>
      </div>
      <div id="migCardBody"></div>
      <div class="actions-row"><a class="btn small" href="#/migrations/${encodeURIComponent(migId)}">View full progress</a></div>
    `;
    async function tick() {
      if (stopped) return;
      try {
        const mig = await api.getMigration(migId);
        if (stopped) return;
        const body = document.getElementById('migCardBody');
        const actionsEl = document.getElementById('migActions');
        if (!body || !actionsEl) return;
        if (!ACTIVE_STATUSES.includes(mig.status)) {
          clearTimer();
          render();
          return;
        }
        body.innerHTML = liveStatusHTML(mig);
        mountLiveActions(actionsEl, body, migId, mig, () => { clearTimer(); tick(); });
      } catch (err) {
        const body = document.getElementById('migCardBody');
        if (body) body.innerHTML = errorBanner('Could not load the live migration: ' + err.message);
      }
      if (!stopped) timer = setTimeout(tick, 1500);
    }
    tick();
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
          <div class="progress-bar"><div class="fill ${snap.Phase === 'complete' || snap.Phase === 'snapshot_done' ? 'done' : snap.Phase === 'failed' ? 'failed' : snap.Phase === 'cancelled' ? 'cancelled' : ''}" style="width:${pct}%"></div></div>
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

  // saveRunOptions stores a started run's tuning and validation choices as
  // the project's defaults, so the form opens with them next time. It
  // re-reads the project so a concurrent settings edit isn't overwritten. A
  // failure only costs the remembered values, so it doesn't block the run.
  async function saveRunOptions(body) {
    try {
      const fresh = await api.getProject(projectId);
      const merged = {
        ...(fresh.transfer_config || {}),
        table_workers: body.table_workers,
        segment_workers: body.segment_workers,
        batch_size: body.batch_size,
        async_pipeline: body.async_pipeline,
        accurate_counts: body.accurate_counts,
        offset_fallback: body.offset_fallback,
        validation: body.validation,
        validation_level: body.validation_level,
        validation_sample: body.validation_sample,
        run_options_saved: true,
      };
      await api.updateProject(projectId, {
        name: fresh.name,
        description: fresh.description,
        source_config: fresh.source_config,
        target_config: fresh.target_config,
        transfer_config: merged,
      });
      transferCfg = merged;
    } catch (err) {
      console.warn('could not save run options as project defaults', err);
    }
  }

  function mountStartForm(migs, sourceType, targetType) {
    const segmentDisabled = sourceType === 'mongodb';
    // The form starts from the project's saved defaults, which the last run
    // from this form wrote (see saveRunOptions). Schema mode, bulk copy and
    // table selection aren't remembered: a stale "drop & recreate" left
    // selected is too easy to run by accident.
    const tc = transferCfg || {};
    const asyncOn = tc.run_options_saved ? !!tc.async_pipeline : true;
    const checked = (on) => (on ? 'checked' : '');
    container.innerHTML = `
      <h2>Migration</h2>
      <details class="advanced" open>
        <summary>Run a migration</summary>
        <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeNone" value="none" checked><label for="modeNone">Keep existing target schema/data</label></div>
        <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeTruncate" value="truncate"><label for="modeTruncate">Truncate target tables first (keep schema)</label></div>
        <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeRecreate" value="recreate"><label for="modeRecreate">Drop &amp; recreate target tables</label></div>
        ${isPgFamily(sourceType) ? `
        <div class="checkbox-row" id="m-live-row"><input type="checkbox" id="m-live"><label for="m-live" style="white-space:nowrap;">Live migration ${betaBadge()}</label><span class="hint" style="margin-left:8px;">Copy the tables, then keep streaming the source's changes to the target. Run the live readiness check in Settings first. In beta: try it on a copy of your data first.</span></div>` : ''}
        <div class="field-row">
          <div><label>Table workers</label><input type="number" id="m-workers" min="1" value="${esc(tc.table_workers > 0 ? tc.table_workers : 1)}"><div class="hint">Parallel tables to migrate</div></div>
          <div><label>Batch size</label><input type="number" id="m-batch" min="1" value="${esc(tc.batch_size > 0 ? tc.batch_size : 1000)}"><div class="hint">Rows per INSERT batch</div></div>
        </div>
        <div class="field-row">
          <div><label>Segment workers</label><input type="number" id="m-segment" min="0" value="${esc(tc.segment_workers > 0 ? tc.segment_workers : 0)}" ${segmentDisabled ? 'disabled' : ''}><div class="hint">${segmentDisabled ? 'Not supported for MongoDB sources' : 'Workers per table (0 = disabled)'}</div></div>
          <div></div>
        </div>
        ${validationOptionsHTML('m', transferCfg)}
        <details class="advanced" style="margin-top:12px;">
          <summary>Performance options</summary>
          <div class="checkbox-row"><input type="checkbox" id="m-async" ${checked(asyncOn)}><label for="m-async">Async pipeline</label><span class="hint" style="margin-left:8px;">Overlap reading and writing for faster throughput</span></div>
          <div class="checkbox-row"><input type="checkbox" id="m-bulk"><label for="m-bulk">Bulk copy</label><span class="hint" style="margin-left:8px;">Faster writes (postgres: COPY; mysql: LOAD DATA; sqlite: batched inserts); only available with truncate or drop &amp; recreate above</span></div>
          <div class="checkbox-row"><input type="checkbox" id="m-accurate" ${checked(tc.accurate_counts)}><label for="m-accurate">Accurate row counts</label><span class="hint" style="margin-left:8px;">Use exact counts instead of estimates (slower)</span></div>
          <div class="checkbox-row" id="m-offset-row" style="display:none;"><input type="checkbox" id="m-offset" ${checked(tc.offset_fallback)}><label for="m-offset">Force OFFSET segments</label><span class="hint" style="margin-left:8px;">Use OFFSET-based splitting instead of PK range</span></div>
        </details>
        <div id="tableSelectBlock"></div>
      </details>
      <div id="startErr"></div>
      <button class="btn primary" id="startMigBtn">Run Migration</button>
      <div style="margin-top:18px;">
        <details class="advanced"><summary>History</summary><div id="migHistory"></div></details>
      </div>
    `;

    wireValidationOptions('m');

    // A live migration creates the target schema itself (to hold back the
    // constraints replay could break), so it can't truncate an existing one.
    const liveInput = document.getElementById('m-live');
    if (liveInput) {
      liveInput.addEventListener('change', () => {
        const truncate = document.getElementById('modeTruncate');
        truncate.disabled = liveInput.checked;
        if (liveInput.checked && truncate.checked) {
          document.getElementById('modeNone').checked = true;
          truncate.dispatchEvent(new Event('change', { bubbles: true }));
        }
        document.getElementById('startMigBtn').textContent = liveInput.checked ? 'Start Live Migration' : 'Run Migration';
      });
    }

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

    // The table list arrives after this form is mounted (analyze is async), so
    // it renders into its own block that can be refilled on its own. Re-running
    // the whole form instead would discard anything already entered here.
    updateTableSelect = () => {
      const block = document.getElementById('tableSelectBlock');
      if (!block) return;
      const names = (getTables() || []).map(t => t.name);
      if (names.length === 0) {
        block.innerHTML = '';
        return;
      }
      block.innerHTML = `
        <div style="margin-top:12px;">
          <label style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
            Tables to migrate
            <span style="font-weight:normal;color:var(--muted);">(<span id="tableCount">${names.length}</span> of ${names.length} selected)</span>
          </label>
          <div class="checkbox-row" style="margin-bottom:6px;">
            <input type="checkbox" id="selectAllTables" checked>
            <label for="selectAllTables">Select all</label>
          </div>
          <div id="tableCheckboxes" style="max-height:200px;overflow-y:auto;border:1px solid var(--border);border-radius:6px;padding:8px;">
            ${names.map(name => `
              <div class="checkbox-row" style="margin:4px 0;">
                <input type="checkbox" class="table-cb" id="tbl-${esc(name)}" value="${esc(name)}" checked>
                <label for="tbl-${esc(name)}" class="mono" style="font-size:12px;">${esc(name)}</label>
              </div>
            `).join('')}
          </div>
        </div>
      `;
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
          const allChecked = container.querySelectorAll('.table-cb:checked').length === names.length;
          document.getElementById('selectAllTables').checked = allChecked;
          updateCount();
        });
      });
    };
    updateTableSelect();
    document.getElementById('startMigBtn').addEventListener('click', async () => {
      const mode = document.querySelector('input[name=schemaMode]:checked').value;
      const body = {
        table_workers: intField('m-workers', 1, 1),
        segment_workers: intField('m-segment', 0, 0),
        batch_size: intField('m-batch', 1000, 1),
        recreate_schema: mode === 'recreate',
        truncate: mode === 'truncate',
        async_pipeline: document.getElementById('m-async')?.checked || false,
        bulk_copy: document.getElementById('m-bulk')?.checked || false,
        accurate_counts: document.getElementById('m-accurate')?.checked || false,
        offset_fallback: document.getElementById('m-offset')?.checked || false,
        ...readValidationOptions('m'),
      };
      const isLive = !!document.getElementById('m-live')?.checked;
      if (isLive) body.mode = 'live';
      // Add selected tables if not all are selected
      const tableBoxes = container.querySelectorAll('.table-cb');
      if (tableBoxes.length > 0) {
        const selectedTables = Array.from(container.querySelectorAll('.table-cb:checked')).map(cb => cb.value);
        if (selectedTables.length === 0) {
          document.getElementById('startErr').innerHTML = errorBanner('Please select at least one table to migrate.');
          return;
        }
        if (selectedTables.length < tableBoxes.length) {
          body.tables = selectedTables;
        }
      }
      const btn = document.getElementById('startMigBtn');
      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> Starting…';
      try {
        const mig = await api.startMigration(projectId, body);
        await saveRunOptions(body);
        navigate(`#/migrations/${encodeURIComponent(mig.id)}`);
      } catch (err) {
        document.getElementById('startErr').innerHTML = errorBanner('Could not start migration: ' + err.message);
        btn.disabled = false;
        btn.textContent = isLive ? 'Start Live Migration' : 'Run Migration';
      }
    });

    const histEl = document.getElementById('migHistory');
    if (!migs || migs.length === 0) {
      histEl.innerHTML = `<div class="hint">No migrations run yet.</div>`;
    } else {
      histEl.innerHTML = migs.map((m) => `
        <div class="list-row" data-id="${esc(m.id)}">
          <div>
            <div class="name mono">${esc(m.id.slice(0, 8))}${m.mode === 'live' ? ' <span class="badge info">live</span> ' + betaBadge() : ''}</div>
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
    // Refills the table-selection block in place. No-op if the start form
    // isn't mounted yet -- it reads getTables() when it mounts anyway.
    updateTables: () => { if (updateTableSelect) updateTableSelect(); },
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
    ${mig.mode === 'live' ? '<div class="card" id="liveArea"></div>' : ''}
    <div id="progressArea"></div>
    <div id="validationArea"></div>
  `;
  const isLive = mig.mode === 'live';

  let stopped = false;
  let timer = null;
  // Set once the validation card is mounted; it lives outside progressArea
  // so the progress re-render on every tick doesn't wipe it.
  let stopValidationCard = null;
  function showValidation(waitForAuto) {
    if (stopValidationCard || stopped) return;
    const el = document.getElementById('validationArea');
    if (el) stopValidationCard = mountValidationCard(el, migId, { waitForAuto });
  }
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
    const phaseBadge = snap.Phase === 'complete' || snap.Phase === 'snapshot_done' ? 'ok' : snap.Phase === 'failed' ? 'fail' : snap.Phase === 'cancelled' ? 'cancelled' : snap.Phase === 'paused' ? 'warn' : 'progress';
    // A live migration's copy ends in snapshot_done; the stream goes on.
    const phaseLabel = snap.Phase === 'snapshot_done' ? 'copy done' : snap.Phase;
    const area = document.getElementById('progressArea');
    if (!area) return;
    area.innerHTML = `
      ${snap.Phase === 'complete' ? '<div class="banner ok"><span>Migration complete.</span></div>' : ''}
      ${snap.Phase === 'failed' ? `<div class="banner error"><span>Migration failed.${snap.Errors && snap.Errors.length ? ' ' + esc(snap.Errors.join('; ')) : ''}</span></div>` : ''}
      ${snap.Phase === 'cancelled' ? '<div class="banner info"><span>Migration cancelled.</span></div>' : ''}
      <div class="stat-grid">
        <div class="stat-box"><div class="label">${isLive ? 'Copy' : 'Phase'}</div><div class="value"><span class="badge ${phaseBadge}">${esc(phaseLabel)}</span></div></div>
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
        <div class="progress-bar"><div class="fill ${snap.Phase === 'complete' || snap.Phase === 'snapshot_done' ? 'done' : snap.Phase === 'failed' ? 'failed' : snap.Phase === 'cancelled' ? 'cancelled' : ''}" style="width:${pct}%"></div></div>
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

  // A per_table validation starts while the migration is still copying;
  // show its card as soon as it exists, checking every few ticks.
  let ticks = 0;
  async function checkEarlyValidation() {
    if (stopValidationCard || ++ticks % 3 !== 1) return;
    try {
      const vs = await api.listValidations(migId);
      if (vs && vs.length) showValidation(false);
    } catch (err) { /* secondary; the card mounts when the migration ends */ }
  }

  // A live migration's stream, refreshed on every tick. Once the copy is
  // done its controls act on the stream, so they replace the copy's.
  let liveMig = mig;
  function renderLive() {
    const el = document.getElementById('liveArea');
    if (el) el.innerHTML = `<strong>Live migration</strong> ${betaBadge()}${liveStatusHTML(liveMig)}`;
    const actionsEl = document.getElementById('migActions');
    if (!actionsEl || ['setup', 'snapshot'].includes(liveMig.phase)) return;
    if (!ACTIVE_STATUSES.includes(liveMig.status)) {
      actionsEl.innerHTML = '';
      return;
    }
    mountLiveActions(actionsEl, el, migId, liveMig, () => { clearTimer(); tick(); });
  }

  async function tick() {
    if (stopped) return;
    if (isLive) {
      try {
        liveMig = await api.getMigration(migId);
      } catch (err) { /* keep the last state; the next tick retries */ }
      if (stopped) return;
      // Past its snapshot, the copy's stats are history (gone after a
      // server restart); the stream is what moves.
      if (!['setup', 'snapshot'].includes(liveMig.phase) || !ACTIVE_STATUSES.includes(liveMig.status)) {
        try {
          renderProgress(await api.getStats(migId));
        } catch (err) { /* no copy stats after a restart */ }
        renderLive();
        if (!ACTIVE_STATUSES.includes(liveMig.status)) {
          clearTimer();
          // A cutover that completes is followed by its validation.
          showValidation(liveMig.status === 'completed');
          return;
        }
        if (!stopped) timer = setTimeout(tick, 1500);
        return;
      }
      renderLive();
    }
    try {
      const snap = await api.getStats(migId);
      renderProgress(snap);
      checkEarlyValidation();
      if (snap.Phase === 'complete' || snap.Phase === 'failed' || snap.Phase === 'cancelled') {
        clearTimer();
        // The server starts the automatic validation right after a
        // migration completes; wait for it rather than report "not validated".
        showValidation(snap.Phase === 'complete');
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
    if (stopValidationCard) stopValidationCard();
  });

  // A finished migration's tables, from the migration record (stats history
  // has no per-table rows), in the shape of a stats snapshot's TableDetails.
  const STATUS_TO_DETAIL = { completed: 'done', in_progress: 'in_progress', failed: 'failed', cancelled: 'cancelled', pending: 'pending', paused: 'pending' };
  const finishedTableDetails = (mig.tables || [])
    .map((t) => ({ Name: t.table_name, Status: STATUS_TO_DETAIL[t.status] || t.status, Transferred: t.rows_transferred, Total: t.rows_total }))
    .sort((a, b) => a.Name.localeCompare(b.Name));

  // For completed/failed/cancelled migrations, load from history instead of live stats
  const finishedStatuses = ['completed', 'failed', 'cancelled'];
  if (finishedStatuses.includes(mig.status)) {
    showValidation(false);
    if (isLive) renderLive();
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
          TableDetails: finishedTableDetails,
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
          TableDetails: finishedTableDetails,
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
