// Thin fetch wrapper around the XferDB REST API. All calls are relative
// (/api/v1/...) so they go through xferdb-web's own reverse proxy — the
// browser never talks to the API server directly.

class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.status = status;
  }
}

async function req(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  let res;
  try {
    res = await fetch(path, opts);
  } catch (e) {
    throw new ApiError('Could not reach the API server. It may be down or restarting.', 0);
  }
  if (res.status === 204) return null;
  let data = null;
  const text = await res.text();
  if (text) {
    try { data = JSON.parse(text); } catch (e) { data = text; }
  }
  if (!res.ok) {
    const msg = (data && data.error) ? data.error : `Server returned ${res.status}`;
    throw new ApiError(msg, res.status);
  }
  return data;
}

export const api = {
  health: () => req('GET', '/api/v1/health'),

  listProjects: () => req('GET', '/api/v1/projects'),
  createProject: (body) => req('POST', '/api/v1/projects', body),
  getProject: (id) => req('GET', `/api/v1/projects/${encodeURIComponent(id)}`),
  updateProject: (id, body) => req('PUT', `/api/v1/projects/${encodeURIComponent(id)}`, body),
  deleteProject: (id) => req('DELETE', `/api/v1/projects/${encodeURIComponent(id)}`),

  preflight: (id) => req('POST', `/api/v1/projects/${encodeURIComponent(id)}/preflight`),
  analyze: (id, body) => req('POST', `/api/v1/projects/${encodeURIComponent(id)}/analyze`, body || {}),
  getTableSchema: (id, table) =>
    req('GET', `/api/v1/projects/${encodeURIComponent(id)}/tables/${encodeURIComponent(table)}`),

  getSchemaPlan: (id) => req('GET', `/api/v1/projects/${encodeURIComponent(id)}/schema-plan`),
  putSchemaPlanOverrides: (id, overrides) =>
    req('PUT', `/api/v1/projects/${encodeURIComponent(id)}/schema-plan`, overrides),
  resetSchemaPlan: (id, collection) => {
    let path = `/api/v1/projects/${encodeURIComponent(id)}/schema-plan`;
    if (collection) path += `?collection=${encodeURIComponent(collection)}`;
    return req('DELETE', path);
  },

  startMigration: (projectId, body) =>
    req('POST', `/api/v1/projects/${encodeURIComponent(projectId)}/migrations`, body || {}),
  listMigrations: (projectId) =>
    req('GET', `/api/v1/projects/${encodeURIComponent(projectId)}/migrations`),
  getMigration: (id) => req('GET', `/api/v1/migrations/${encodeURIComponent(id)}`),
  patchMigration: (id, action) =>
    req('PATCH', `/api/v1/migrations/${encodeURIComponent(id)}`, { action }),
  deleteMigration: (id) => req('DELETE', `/api/v1/migrations/${encodeURIComponent(id)}`),
  getStats: (id) => req('GET', `/api/v1/migrations/${encodeURIComponent(id)}/stats`),
  getStatsHistory: (id) => req('GET', `/api/v1/migrations/${encodeURIComponent(id)}/stats/history`),
};

// analyzeMongoStream POSTs to /analyze and reads the NDJSON stream the server
// sends for MongoDB sources, invoking onEvent(obj) for each decoded line.
// Resolves when the stream ends (after a "done" or "error" event).
export async function analyzeMongoStream(projectId, body, onEvent) {
  const res = await fetch(`/api/v1/projects/${encodeURIComponent(projectId)}/analyze`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok || !res.body) {
    let data = null;
    try { data = await res.json(); } catch (e) { /* ignore */ }
    throw new ApiError((data && data.error) || `Server returned ${res.status}`, res.status);
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let idx;
    while ((idx = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;
      try { onEvent(JSON.parse(line)); } catch (e) { /* ignore malformed line */ }
    }
  }
  if (buf.trim()) {
    try { onEvent(JSON.parse(buf.trim())); } catch (e) { /* ignore */ }
  }
}

export { ApiError };
