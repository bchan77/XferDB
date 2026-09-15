import { api, analyzeMongoStream } from './api.js';
import { esc, confirmModal, errorBanner, fmtInt, fmtDate, fmtDuration, redactMongoURI } from './ui.js';

const app = document.getElementById('app');
let cleanup = null;

function setCleanup(fn) { cleanup = fn; }
function navigate(hash) { location.hash = hash; }

const PG_TYPES = ['text', 'varchar(255)', 'integer', 'bigint', 'smallint', 'boolean', 'numeric',
  'real', 'double precision', 'date', 'timestamp', 'timestamptz', 'jsonb', 'uuid', 'bytea'];
const STRATEGIES = ['direct', 'as_jsonb', 'flatten', 'skip'];

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

async function loadMigSection(projectId) {
  const sec = document.getElementById('migSection');
  sec.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading migrations…</div>`;
  try {
    const migs = await api.listMigrations(projectId);
    const active = migs.find((m) => m.status === 'pending' || m.status === 'in_progress' || m.status === 'paused');
    let startHTML;
    if (active) {
      startHTML = `
        <div class="banner info">
          <span>A migration is already ${esc(active.status)} for this project.</span>
          <button class="btn small" id="goActiveBtn">View Progress</button>
        </div>`;
    } else {
      startHTML = `
        <details class="advanced">
          <summary>Migration options</summary>
          <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeNone" value="none" checked><label for="modeNone">Keep existing target schema/data</label></div>
          <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeTruncate" value="truncate"><label for="modeTruncate">Truncate target tables first (keep schema)</label></div>
          <div class="checkbox-row"><input type="radio" name="schemaMode" id="modeRecreate" value="recreate"><label for="modeRecreate">Drop &amp; recreate target tables</label></div>
          <div class="field-row">
            <div><label>Table workers</label><input type="number" id="m-workers" value="1"></div>
            <div><label>Batch size</label><input type="number" id="m-batch" value="1000"></div>
          </div>
        </details>
        <div id="startErr"></div>
        <button class="btn primary" id="startMigBtn">Start Migration</button>
      `;
    }
    sec.innerHTML = startHTML + '<div style="margin-top:22px;"><h2 style="font-size:13px;">History</h2><div id="migHistory"></div></div>';

    if (active) {
      document.getElementById('goActiveBtn').addEventListener('click', () => navigate(`#/migrations/${encodeURIComponent(active.id)}`));
    } else {
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
          btn.textContent = 'Start Migration';
        }
      });
    }

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
  } catch (err) {
    sec.innerHTML = errorBanner('Could not load migrations: ' + err.message, { retry: () => loadMigSection(projectId) });
  }
}

async function renderProjectDetail(id) {
  document.title = 'XferDB — Project';
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
  document.title = `XferDB — ${project.name}`;

  app.innerHTML = `
    <div class="breadcrumb"><a href="#/">Projects</a> / ${esc(project.name)}</div>
    <div class="page-header">
      <div><h1>${esc(project.name)}</h1><div class="sub">${esc(project.description || '')}</div></div>
      <div class="actions-row">
        <button class="btn" id="viewTablesBtn">View Tables</button>
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
      <h2>Migrations</h2>
      <div id="migSection"></div>
    </div>
  `;

  document.getElementById('viewTablesBtn').addEventListener('click', () => navigate(`#/projects/${encodeURIComponent(id)}/tables`));
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

  await loadMigSection(id);
  await runPreflight(id);
}

// ---------------------------------------------------------------------------
// Tables / collections list
// ---------------------------------------------------------------------------

function renderTableCards(area, projectId, items) {
  if (items.length === 0) {
    area.innerHTML = `<div class="empty-state"><div class="icon">📂</div><p>No tables found.</p></div>`;
    return;
  }
  area.innerHTML = items.map((t) => `
    <div class="list-row" data-name="${esc(t.name)}">
      <div class="name mono">${esc(t.name)}</div>
      <span class="badge ${t.badge}">${esc(t.badgeText)}</span>
    </div>
  `).join('');
  area.querySelectorAll('.list-row').forEach((row) => {
    row.addEventListener('click', () => navigate(`#/projects/${encodeURIComponent(projectId)}/tables/${encodeURIComponent(row.dataset.name)}`));
  });
}

async function loadTables(id, project) {
  const area = document.getElementById('tablesArea');
  area.innerHTML = `<div class="loading-block"><span class="spinner"></span> Analyzing…</div>`;
  try {
    if (project.source_config.type === 'mongodb') {
      area.innerHTML = '<div class="hint" id="progressLine">Starting…</div>';
      const progressLine = document.getElementById('progressLine');
      const collections = [];
      await analyzeMongoStream(id, {}, (evt) => {
        if (evt.type === 'progress') {
          progressLine.textContent = `Sampling ${evt.collection}: ${fmtInt(evt.scanned)} / ${fmtInt(evt.target)}…`;
        } else if (evt.type === 'collection') {
          const fields = (evt.schema && evt.schema.fields) || [];
          collections.push({ name: evt.collection, fieldCount: fields.length, warnings: evt.warnings || [] });
        } else if (evt.type === 'error') {
          throw new Error(evt.error);
        }
      });
      renderTableCards(area, id, collections.map((c) => ({
        name: c.name,
        badge: c.warnings.length ? 'warn' : 'ok',
        badgeText: c.warnings.length ? 'warning' : `${c.fieldCount} fields`,
      })));
    } else {
      const result = await api.analyze(id, {});
      const tables = result.Tables || [];
      renderTableCards(area, id, tables.map((t) => ({
        name: t.Name,
        badge: t.Status === 'compatible' ? 'ok' : 'warn',
        badgeText: `${t.Status}${t.Issues && t.Issues.length ? ' · ' + t.Issues.length + ' issue(s)' : ''}`,
      })));
    }
  } catch (err) {
    area.innerHTML = errorBanner('Analyze failed: ' + err.message, { retry: () => loadTables(id, project) });
  }
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
  document.title = `XferDB — ${project.name} ${label.toLowerCase()}`;
  app.innerHTML = `
    <div class="breadcrumb"><a href="#/">Projects</a> / <a href="#/projects/${encodeURIComponent(id)}">${esc(project.name)}</a> / ${label}</div>
    <div class="page-header"><h1>${label}</h1><button class="btn" id="reanalyzeBtn">Re-analyze</button></div>
    <div id="tablesArea"></div>
  `;
  document.getElementById('reanalyzeBtn').addEventListener('click', () => loadTables(id, project));
  await loadTables(id, project);
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

async function renderRelationalSplit(area, id, tableName) {
  area.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading schema…</div>`;
  try {
    const { source, target } = await api.getTableSchema(id, tableName);
    const srcCols = (source && source.columns) || [];
    const tgtCols = (target && target.columns) || [];
    const tgtExists = tgtCols.length > 0;
    const tgtByName = {};
    tgtCols.forEach((c) => { tgtByName[c.name] = c; });

    area.innerHTML = `
      ${!tgtExists ? '<div class="banner info">This table doesn’t exist on the target yet — it will be created automatically when the migration runs.</div>' : ''}
      <div class="banner info">Editable type suggestions are currently only available for MongoDB→PostgreSQL projects. This view is read-only.</div>
      <div class="split">
        <div><h3>Source</h3>${srcCols.map((c) => colRowReadOnly(c)).join('') || '<div class="hint">No columns.</div>'}</div>
        <div><h3>Target ${tgtExists ? '' : '(not yet created)'}</h3>
          ${srcCols.map((c) => colRowReadOnly(tgtByName[c.name] || c, !tgtByName[c.name])).join('') || '<div class="hint">No columns.</div>'}
        </div>
      </div>
    `;
  } catch (err) {
    area.innerHTML = errorBanner('Could not load table schema: ' + err.message, { retry: () => renderRelationalSplit(area, id, tableName) });
  }
}

function rowId(fieldName) {
  return fieldName.replace(/[^a-zA-Z0-9_-]/g, '_');
}

function editableRow(r) {
  const rid = rowId(r.field_name);
  const knownType = PG_TYPES.includes(r.pg_type);
  return `
    <div class="col-row" id="row-${rid}">
      <div class="edit-grid">
        <input type="text" id="col-${rid}" value="${esc(r.pg_column)}" placeholder="column name">
        <select id="type-${rid}">
          ${PG_TYPES.map((t) => `<option value="${t}" ${t === r.pg_type ? 'selected' : ''}>${t}</option>`).join('')}
          ${!knownType ? `<option value="${esc(r.pg_type)}" selected>${esc(r.pg_type)}</option>` : ''}
        </select>
        <select id="strat-${rid}">
          ${STRATEGIES.map((s) => `<option value="${s}" ${s === r.strategy ? 'selected' : ''}>${s}</option>`).join('')}
        </select>
        <span></span>
      </div>
      <div class="checkbox-row">
        <input type="checkbox" id="null-${rid}" ${r.nullable ? 'checked' : ''}><label for="null-${rid}">Nullable</label>
        <input type="checkbox" id="pk-${rid}" ${r.is_pk ? 'checked' : ''}><label for="pk-${rid}">Primary key</label>
      </div>
    </div>
  `;
}

async function renderMongoSplit(area, id, tableName) {
  area.innerHTML = `<div class="loading-block"><span class="spinner"></span> Loading schema plan…</div>`;
  let plan;
  try {
    plan = await api.getSchemaPlan(id);
  } catch (err) {
    area.innerHTML = errorBanner('Could not load schema plan: ' + err.message, { retry: () => renderMongoSplit(area, id, tableName) });
    return;
  }
  const rows = plan[tableName];
  if (!rows || rows.length === 0) {
    area.innerHTML = `
      <div class="banner info">No schema plan found for this collection yet.</div>
      <a class="btn" href="#/projects/${encodeURIComponent(id)}/tables">Back to collections (Re-analyze there)</a>
    `;
    return;
  }

  const original = {};
  rows.forEach((r) => { original[r.field_name] = { ...r }; });

  area.innerHTML = `
    <div class="split">
      <div><h3>Source (MongoDB field)</h3><div id="srcCols">
        ${rows.map((r) => `
          <div class="col-row">
            <div class="col-name">${esc(r.field_name)} ${r.is_pk ? '<span class="badge ok">PK</span>' : ''}</div>
            ${r.overridden ? '<div class="hint">previously overridden</div>' : ''}
          </div>
        `).join('')}
      </div></div>
      <div><h3>Suggested target (PostgreSQL)</h3><div id="tgtCols">${rows.map((r) => editableRow(r)).join('')}</div></div>
    </div>
    <div id="saveArea"></div>
  `;

  function readRow(fieldName) {
    const rid = rowId(fieldName);
    return {
      field_name: fieldName,
      pg_column: document.getElementById(`col-${rid}`).value.trim(),
      pg_type: document.getElementById(`type-${rid}`).value,
      strategy: document.getElementById(`strat-${rid}`).value,
      nullable: document.getElementById(`null-${rid}`).checked,
      is_pk: document.getElementById(`pk-${rid}`).checked,
    };
  }

  function isDirty(cur, orig) {
    return cur.pg_column !== orig.pg_column || cur.pg_type !== orig.pg_type ||
      cur.strategy !== orig.strategy || cur.nullable !== orig.nullable || cur.is_pk !== orig.is_pk;
  }

  function refreshDirty() {
    let anyDirty = false;
    rows.forEach((r) => {
      const dirty = isDirty(readRow(r.field_name), original[r.field_name]);
      const rowEl = document.getElementById('row-' + rowId(r.field_name));
      if (rowEl) rowEl.classList.toggle('dirty', dirty);
      if (dirty) anyDirty = true;
    });
    const saveArea = document.getElementById('saveArea');
    if (anyDirty) {
      saveArea.innerHTML = `<div class="actions-row"><button class="btn primary" id="saveSchemaBtn">Save Changes</button></div>`;
      document.getElementById('saveSchemaBtn').addEventListener('click', onSave);
    } else {
      saveArea.innerHTML = '';
    }
  }

  area.querySelectorAll('#tgtCols input, #tgtCols select').forEach((el) => {
    el.addEventListener('input', refreshDirty);
    el.addEventListener('change', refreshDirty);
  });

  async function onSave() {
    const changed = [];
    const diffLines = [];
    rows.forEach((r) => {
      const cur = readRow(r.field_name);
      const orig = original[r.field_name];
      if (isDirty(cur, orig)) {
        changed.push({ collection: tableName, ...cur });
        diffLines.push(`${r.field_name}: ${orig.pg_type} → ${cur.pg_type}${orig.strategy !== cur.strategy ? ` (strategy ${orig.strategy} → ${cur.strategy})` : ''}`);
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
      await api.putSchemaPlanOverrides(id, changed);
      await renderMongoSplit(area, id, tableName);
    } catch (err) {
      document.getElementById('saveArea').insertAdjacentHTML('beforeend', errorBanner('Save failed: ' + err.message));
      btn.disabled = false;
      btn.textContent = 'Save Changes';
    }
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
  const backHash = `#/projects/${encodeURIComponent(id)}/tables`;
  const label = project.source_config.type === 'mongodb' ? 'Collections' : 'Tables';
  app.innerHTML = `
    <div class="breadcrumb">
      <a href="#/">Projects</a> / <a href="#/projects/${encodeURIComponent(id)}">${esc(project.name)}</a> /
      <a href="${backHash}">${label}</a> / ${esc(tableName)}
    </div>
    <div class="page-header"><h1 class="mono">${esc(tableName)}</h1></div>
    <div id="detailArea"></div>
  `;
  const area = document.getElementById('detailArea');
  if (project.source_config.type === 'mongodb') {
    await renderMongoSplit(area, id, tableName);
  } else {
    await renderRelationalSplit(area, id, tableName);
  }
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

  function renderActions(phase) {
    const actionsEl = document.getElementById('migActions');
    if (!actionsEl) return;
    const buttons = [];
    if (['in_progress', 'schema', 'post_schema'].includes(phase)) buttons.push('<button class="btn" id="pauseBtn">Pause</button>');
    if (phase === 'paused') buttons.push('<button class="btn primary" id="resumeBtn">Resume</button>');
    if (!['complete', 'failed'].includes(phase)) buttons.push('<button class="btn danger" id="cancelBtn">Cancel</button>');
    actionsEl.innerHTML = buttons.join(' ');
    const pauseBtn = document.getElementById('pauseBtn');
    if (pauseBtn) pauseBtn.addEventListener('click', () => doAction('pause'));
    const resumeBtn = document.getElementById('resumeBtn');
    if (resumeBtn) resumeBtn.addEventListener('click', () => doAction('resume'));
    const cancelBtn = document.getElementById('cancelBtn');
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

  async function doAction(action) {
    try {
      await api.patchMigration(migId, action);
      clearTimer();
      tick();
    } catch (err) {
      const area = document.getElementById('progressArea');
      if (area) area.insertAdjacentHTML('afterbegin', errorBanner(action + ' failed: ' + err.message));
    }
  }

  function renderProgress(snap) {
    renderActions(snap.Phase);
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
    if (parts[2] === 'tables') {
      if (parts[3]) return renderTableDetail(id, parts[3]);
      return renderTablesList(id);
    }
    return renderProjectDetail(id);
  }
  if (parts[0] === 'migrations' && parts[1]) return renderMigrationProgress(parts[1]);
  return render404();
}

const brand = document.getElementById('brand');
if (brand) brand.addEventListener('click', () => navigate('#/'));
window.addEventListener('hashchange', router);
router();
startHealthPoll();
