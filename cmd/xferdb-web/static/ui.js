// Small reusable UI helpers shared across views.

export function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// confirmModal shows a modal with a message and Confirm/Cancel buttons.
// Resolves true if confirmed, false otherwise (cancel, backdrop click, Esc).
export function confirmModal({ title = 'Are you sure?', body = '', confirmLabel = 'Confirm', danger = false } = {}) {
  return new Promise((resolve) => {
    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
      <div class="modal" role="dialog" aria-modal="true">
        <h3>${esc(title)}</h3>
        <div class="body">${esc(body)}</div>
        <div class="actions">
          <button class="btn" data-act="cancel">Cancel</button>
          <button class="btn ${danger ? 'danger' : 'primary'}" data-act="ok">${esc(confirmLabel)}</button>
        </div>
      </div>`;
    document.body.appendChild(overlay);

    function done(result) {
      document.removeEventListener('keydown', onKey);
      overlay.remove();
      resolve(result);
    }
    function onKey(e) {
      if (e.key === 'Escape') done(false);
    }
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) done(false);
    });
    overlay.querySelector('[data-act=cancel]').addEventListener('click', () => done(false));
    overlay.querySelector('[data-act=ok]').addEventListener('click', () => done(true));
    document.addEventListener('keydown', onKey);
    overlay.querySelector('[data-act=ok]').focus();
  });
}

export function errorBanner(message, { retry } = {}) {
  const id = 'b' + Math.random().toString(36).slice(2);
  setTimeout(() => {
    if (retry) {
      const btn = document.getElementById(id);
      if (btn) btn.addEventListener('click', retry);
    }
  });
  return `<div class="banner error"><span>${esc(message)}</span>${
    retry ? `<button class="btn small" id="${id}">Retry</button>` : ''
  }</div>`;
}

export function fmtInt(n) {
  if (n === null || n === undefined) return '0';
  return Number(n).toLocaleString('en-US');
}

export function fmtDate(s) {
  if (!s) return '—';
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  return d.toLocaleString();
}

export function fmtDuration(seconds) {
  const s = Math.floor(seconds || 0);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}

// redactMongoURI hides the userinfo (user:password@) portion of a mongo URI
// for display only. Never send the result anywhere — it's for rendering.
export function redactMongoURI(uri) {
  if (!uri) return '';
  try {
    return uri.replace(/\/\/([^@/]+)@/, '//***:***@');
  } catch (e) {
    return '(hidden)';
  }
}

// inferTypeFromDSN mirrors registry.InferTypeFromDSN for the four supported schemes.
export function inferTypeFromDSN(dsn) {
  if (!dsn) return '';
  const lower = dsn.toLowerCase();
  if (lower.startsWith('mongodb://') || lower.startsWith('mongodb+srv://')) return 'mongodb';
  const m = lower.match(/^([a-z0-9+.-]+):\/\//);
  if (!m) return '';
  const scheme = m[1];
  if (['postgres', 'mysql', 'sqlite'].includes(scheme)) return scheme;
  return '';
}
