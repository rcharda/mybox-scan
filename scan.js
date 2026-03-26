// ════════════════════════════════════════════════════════
//  SCAN AUTO — VERSION SERVEUR (GitHub Actions)
//  🚀 MODE TURBO SEUL (Sans filtre intelligent)
// ════════════════════════════════════════════════════════

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("❌ Variables d'environnement manquantes : SUPABASE_URL et SUPABASE_KEY");
  process.exit(1);
}

const SB_HEADERS_R = {
  'apikey': SUPABASE_KEY,
  'Authorization': 'Bearer ' + SUPABASE_KEY,
};
const SB_HEADERS_W = {
  ...SB_HEADERS_R,
  'Content-Type': 'application/json',
  'Prefer': 'return=representation',
};

// ── 🚀 CONFIGURATION TURBO ─────────────────────────────
const TIMEOUT_MS   = 5_000;  // 5 secondes max par chaîne
const CONCURRENCY  = 50;     // 50 chaînes testées en même temps
const RETRY_COUNT  = 0;      // 0 retry pour aller au plus vite

function log(type, msg) {
  const icons = { ok: '✅', fail: '❌', info: 'ℹ️', warn: '⚠️', sys: '🔵' };
  console.log(`${icons[type] || '·'} ${msg}`);
}

async function sbGet(table, params = '') {
  const q = params ? '?' + params : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}${q}`, {
    headers: SB_HEADERS_R,
  });
  if (!r.ok) throw new Error(`sbGet ${table}: ${await r.text()}`);
  return r.json();
}

async function sbPatch(table, filter, body) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${filter}`, {
    method: 'PATCH',
    headers: SB_HEADERS_W,
    body: JSON.stringify(body),
  });
  return r;
}

async function sbPost(table, body) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: { ...SB_HEADERS_W, 'Prefer': 'return=minimal' },
    body: JSON.stringify(body),
  });
  return r;
}

async function testChannel(ch, attempt = 0) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
    const res = await fetch(ch.url, {
      method: 'GET',
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; IPTVScanner/1.0)',
        'Accept': '*/*',
      },
      redirect: 'follow',
    });
    clearTimeout(timer);

    if (res.status < 400) return true;
    if (res.status === 401 || res.status === 403) return true;

    return false;
  } catch (err) {
    clearTimeout(timer);
    if (attempt < RETRY_COUNT) {
      await new Promise(r => setTimeout(r, 1000));
      return testChannel(ch, attempt + 1);
    }
    return false;
  }
}

async function scanAllChannels(channels) {
  const okChannels   = [];
  const failChannels = [];
  let done = 0;
  const total = channels.length;

  for (let i = 0; i < total; i += CONCURRENCY) {
    const batch = channels.slice(i, i + CONCURRENCY);

    const results = await Promise.all(
      batch.map(ch => testChannel(ch).then(ok => ({ ch, ok })))
    );

    for (const { ch, ok } of results) {
      done++;
      if (ok) {
        okChannels.push(ch);
      } else {
        failChannels.push(ch);
      }
    }

    const pct = Math.round((done / total) * 100);
    if (i % (CONCURRENCY * 5) === 0 || done === total) {
        log('sys', `Progression : ${pct}% (${done}/${total}) — ✅ ${okChannels.length} | ❌ ${failChannels.length}`);
    }
  }

  return { okChannels, failChannels };
}

async function publishToSupabase(okChannels, failChannels, currentVersion) {
  const orderedChannels = [...okChannels, ...failChannels];
  const prioURLs = okChannels.map(c => c.url);

  const now = new Date();
  const base = now.getFullYear() + '.' + String(now.getMonth() + 1).padStart(2, '0') + '.' + String(now.getDate()).padStart(2, '0');

  const parts = (currentVersion || '1.0').split('.');
  let newVersion = base;
  if (parts.slice(0, 3).join('.') === base && parts.length > 3) {
    newVersion = base + '.' + (parseInt(parts[3] || '0') + 1);
  } else if (parts.slice(0, 3).join('.') === base) {
    newVersion = base + '.1';
  }

  log('sys', `📦 Publication channels_data v${newVersion} (${orderedChannels.length} chaînes)...`);

  const payload = { version: newVersion, count: orderedChannels.length, published_at: now.toISOString(), data: orderedChannels };
  const r1 = await sbPatch('channels_data', 'id=eq.1', payload);
  if (!r1.ok) await sbPost('channels_data', { id: 1, ...payload });

  log('sys', `⭐ Mise à jour channel_priorities (${prioURLs.length} URLs)...`);
  const r2 = await sbPatch('channel_priorities', 'id=eq.1', { priorities: prioURLs, saved_at: now.toISOString() });
  if (!r2.ok) await sbPost('channel_priorities', { id: 1, priorities: prioURLs, saved_at: now.toISOString() });

  return newVersion;
}

async function main() {
  log('sys', '🤖 SCAN AUTO — DÉMARRAGE (Mode Turbo)');
  const rows = await sbGet('channels_data', 'select=version,count,data&order=published_at.desc&limit=1');
  if (!rows || !rows.length) process.exit(1);

  const allChannels = rows[0].data.filter(c => c.url);
  const currentVersion = rows[0].version || '1.0';
  const { okChannels, failChannels } = await scanAllChannels(allChannels);
  const newVersion = await publishToSupabase(okChannels, failChannels, currentVersion);
  log('sys', `🚀 PUBLICATION RÉUSSIE — v${newVersion}`);
}

main().catch(err => { console.error('Erreur:', err.message); process.exit(1); });
