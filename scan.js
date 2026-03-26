// ════════════════════════════════════════════════════════
//  SCAN AUTO — VERSION SERVEUR (GitHub Actions)
//  Reproduit la logique de onglet_scan_auto.html
//  sans navigateur, via des requêtes HTTP simples
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

// ── CONFIG ────────────────────────────────────────────
const TIMEOUT_MS   = 10_000; // 10 secondes par chaîne
const CONCURRENCY  = 10;     // 10 chaînes testées en parallèle
const RETRY_COUNT  = 1;      // 1 retry si timeout

// ════════════════════════════════════════════════════════
//  HELPERS
// ════════════════════════════════════════════════════════
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

// ════════════════════════════════════════════════════════
//  TEST D'UNE CHAÎNE (équivalent de testChannel() du HTML)
//  On fait un GET HTTP sur l'URL .m3u8 ou le stream.
//  Si la réponse est 2xx ou 3xx → chaîne vivante.
// ════════════════════════════════════════════════════════
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

    // 2xx ou 3xx = le flux répond
    if (res.status < 400) return true;

    // 401/403 = protégé mais existe
    if (res.status === 401 || res.status === 403) return true;

    return false;
  } catch (err) {
    clearTimeout(timer);
    // Retry une fois en cas de timeout réseau
    if (attempt < RETRY_COUNT) {
      await new Promise(r => setTimeout(r, 1000));
      return testChannel(ch, attempt + 1);
    }
    return false;
  }
}

// ════════════════════════════════════════════════════════
//  SCAN EN PARALLÈLE (pool de CONCURRENCY)
// ════════════════════════════════════════════════════════
async function scanAllChannels(channels) {
  const okChannels   = [];
  const failChannels = [];
  let done = 0;
  const total = channels.length;

  // Découper en batches de CONCURRENCY
  for (let i = 0; i < total; i += CONCURRENCY) {
    const batch = channels.slice(i, i + CONCURRENCY);

    const results = await Promise.all(
      batch.map(ch => testChannel(ch).then(ok => ({ ch, ok })))
    );

    for (const { ch, ok } of results) {
      done++;
      if (ok) {
        okChannels.push(ch);
        log('ok', `[${done}/${total}] MARCHE  → ${ch.name || ch.url}`);
      } else {
        failChannels.push(ch);
        log('fail', `[${done}/${total}] MORTE   → ${ch.name || ch.url}`);
      }
    }

    const pct = Math.round((done / total) * 100);
    log('sys', `Progression : ${pct}% (${done}/${total}) — ✅ ${okChannels.length} | ❌ ${failChannels.length}`);
  }

  return { okChannels, failChannels };
}

// ════════════════════════════════════════════════════════
//  PUBLICATION SUR SUPABASE (identique au HTML)
// ════════════════════════════════════════════════════════
async function publishToSupabase(okChannels, failChannels, currentVersion) {
  const orderedChannels = [...okChannels, ...failChannels];
  const prioURLs = okChannels.map(c => c.url);

  // Calcul de la nouvelle version
  const now = new Date();
  const base = now.getFullYear() + '.' +
    String(now.getMonth() + 1).padStart(2, '0') + '.' +
    String(now.getDate()).padStart(2, '0');

  const parts = (currentVersion || '1.0').split('.');
  let newVersion = base;
  if (parts.slice(0, 3).join('.') === base && parts.length > 3) {
    newVersion = base + '.' + (parseInt(parts[3] || '0') + 1);
  } else if (parts.slice(0, 3).join('.') === base) {
    newVersion = base + '.1';
  }

  log('sys', `📦 Publication channels_data v${newVersion} (${orderedChannels.length} chaînes)...`);

  // 1. Publier channels_data
  const payload = {
    version: newVersion,
    count: orderedChannels.length,
    published_at: now.toISOString(),
    data: orderedChannels,
  };

  const r1 = await sbPatch('channels_data', 'id=eq.1', payload);
  if (!r1.ok) {
    log('warn', 'Pas de ligne id=1, création...');
    await sbPost('channels_data', { id: 1, ...payload });
  }
  log('ok', `✅ channels_data publié (v${newVersion})`);

  // 2. Publier channel_priorities
  log('sys', `⭐ Mise à jour channel_priorities (${prioURLs.length} URLs)...`);
  const r2 = await sbPatch('channel_priorities', 'id=eq.1', {
    priorities: prioURLs,
    saved_at: now.toISOString(),
  });
  if (!r2.ok) {
    log('warn', 'Pas de ligne id=1, création...');
    await sbPost('channel_priorities', {
      id: 1,
      priorities: prioURLs,
      saved_at: now.toISOString(),
    });
  }
  log('ok', `✅ channel_priorities mis à jour (${prioURLs.length} prioritaires)`);

  return newVersion;
}

// ════════════════════════════════════════════════════════
//  POINT D'ENTRÉE
// ════════════════════════════════════════════════════════
async function main() {
  log('sys', '══════════════════════════════════════════════');
  log('sys', '🤖 SCAN AUTO — DÉMARRAGE (version serveur)');
  log('sys', `⏱  Timeout : ${TIMEOUT_MS / 1000}s | Concurrence : ${CONCURRENCY}`);
  log('sys', '══════════════════════════════════════════════');

  // 1. Charger les chaînes depuis Supabase
  log('sys', '🔄 Chargement des chaînes depuis Supabase...');
  const rows = await sbGet(
    'channels_data',
    'select=version,count,data&order=published_at.desc&limit=1'
  );

  if (!rows || !rows.length || !Array.isArray(rows[0].data) || !rows[0].data.length) {
    log('fail', 'Aucune chaîne dans Supabase (channels_data vide)');
    process.exit(1);
  }

  const allChannels    = rows[0].data.filter(c => c.url);
  const currentVersion = rows[0].version || '1.0';
  log('ok', `${allChannels.length} chaînes chargées (v${currentVersion})`);

  // 2. Scanner toutes les chaînes
  log('sys', '🤖 Démarrage du scan...');
  const startTime = Date.now();
  const { okChannels, failChannels } = await scanAllChannels(allChannels);
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  log('sys', '══════════════════════════════════════════════');
  log('sys', `🎉 SCAN TERMINÉ en ${elapsed}s`);
  log('ok',  `✅ Chaînes qui marchent : ${okChannels.length}`);
  log('fail',`❌ Chaînes mortes       : ${failChannels.length}`);
  log('sys', `📊 Taux de réussite     : ${Math.round(okChannels.length / allChannels.length * 100)}%`);
  log('sys', '══════════════════════════════════════════════');

  // 3. Publier sur Supabase
  const newVersion = await publishToSupabase(okChannels, failChannels, currentVersion);

  log('sys', '');
  log('sys', `🚀 PUBLICATION RÉUSSIE — v${newVersion}`);
  log('sys', `📦 ${allChannels.length} chaînes publiées`);
  log('ok',  `⭐ ${okChannels.length} chaînes prioritaires en haut`);
  log('fail',`🔻 ${failChannels.length} chaînes mortes en bas`);
}

main().catch(err => {
  console.error('❌ Erreur fatale :', err.message);
  process.exit(1);
});
