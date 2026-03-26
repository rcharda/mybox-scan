// ════════════════════════════════════════════════════════
//  SCAN AUTO — VERSION SERVEUR (GitHub Actions)
//  TRUE WORKER POOL — optimisé pour 31 000+ chaînes
//  Problème résolu : batch → worker pool
//  Chaque worker prend la tâche suivante immédiatement
// ════════════════════════════════════════════════════════

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("❌ Variables manquantes : SUPABASE_URL et SUPABASE_KEY");
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

// ── CONFIG WORKER POOL ────────────────────────────────
const TIMEOUT_MS  = 3_000;  // 3s (HEAD répond vite)
const CONCURRENCY = 300;    // 300 workers en parallèle
//
// Estimation : 31 000 chaînes à 96% mortes
//   300 workers × (1/3s) = ~100 ch/s
//   31 000 / 100 = ~5 min ✅
// ─────────────────────────────────────────────────────

function log(type, msg) {
  const icons = { ok: '✅', fail: '❌', info: 'ℹ️', warn: '⚠️', sys: '🔵' };
  const ts = new Date().toLocaleTimeString('fr-FR');
  console.log(`[${ts}] ${icons[type] || '·'} ${msg}`);
}

async function sbGet(table, params = '') {
  const q = params ? '?' + params : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}${q}`, { headers: SB_HEADERS_R });
  if (!r.ok) throw new Error(`sbGet ${table}: ${await r.text()}`);
  return r.json();
}

async function sbPatch(table, filter, body) {
  return fetch(`${SUPABASE_URL}/rest/v1/${table}?${filter}`, {
    method: 'PATCH',
    headers: SB_HEADERS_W,
    body: JSON.stringify(body),
  });
}

async function sbPost(table, body) {
  return fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: { ...SB_HEADERS_W, 'Prefer': 'return=minimal' },
    body: JSON.stringify(body),
  });
}

// ════════════════════════════════════════════════════════
//  TEST D'UNE CHAÎNE
//  HEAD d'abord (rapide), GET en fallback
//  2xx / 3xx / 401 / 403 = vivante
// ════════════════════════════════════════════════════════
async function testChannel(ch) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
    // HEAD = pas de body téléchargé → plus rapide
    const res = await fetch(ch.url, {
      method: 'HEAD',
      signal: controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; IPTVScanner/1.0)',
        'Accept': '*/*',
      },
      redirect: 'follow',
    });
    clearTimeout(timer);
    // 2xx, 3xx, 401, 403 = le serveur répond = chaîne vivante
    return res.status < 400 || res.status === 401 || res.status === 403;
  } catch {
    clearTimeout(timer);
    return false;
  }
}

// ════════════════════════════════════════════════════════
//  TRUE WORKER POOL
//  Contrairement aux batches, chaque worker prend
//  la tâche suivante dès qu'il finit — pas d'attente
// ════════════════════════════════════════════════════════
async function workerPool(channels, concurrency) {
  const okChannels   = [];
  const failChannels = [];
  const total        = channels.length;
  let   index        = 0;   // prochain index à traiter
  let   done         = 0;   // compteur total traités
  const startTime    = Date.now();

  // Un worker : prend une tâche, la traite, prend la suivante
  async function worker() {
    while (true) {
      const i = index++;          // réserver atomiquement le prochain index
      if (i >= total) return;     // plus rien à faire

      const ch = channels[i];
      const ok = await testChannel(ch);

      if (ok) okChannels.push(ch);
      else    failChannels.push(ch);

      done++;

      // Log toutes les 2000 chaînes
      if (done % 2000 === 0 || done === total) {
        const elapsed  = ((Date.now() - startTime) / 1000).toFixed(0);
        const pct      = Math.round((done / total) * 100);
        const vitesse  = Math.round(done / (elapsed || 1));
        const restant  = vitesse > 0 ? Math.round((total - done) / vitesse) : '?';
        log('sys', `${pct}% — ${done}/${total} | ✅ ${okChannels.length} | ❌ ${failChannels.length} | ${vitesse} ch/s | ~${restant}s restantes`);
      }
    }
  }

  // Lancer CONCURRENCY workers en parallèle
  // Chacun tourne en boucle jusqu'à épuisement de la file
  await Promise.all(
    Array.from({ length: concurrency }, () => worker())
  );

  return { okChannels, failChannels };
}

// ════════════════════════════════════════════════════════
//  PUBLICATION SUR SUPABASE
// ════════════════════════════════════════════════════════
async function publishToSupabase(okChannels, failChannels, currentVersion) {
  const orderedChannels = [...okChannels, ...failChannels];
  const prioURLs        = okChannels.map(c => c.url);

  const now  = new Date();
  const base = now.getFullYear() + '.' +
    String(now.getMonth() + 1).padStart(2, '0') + '.' +
    String(now.getDate()).padStart(2, '0');

  const parts = (currentVersion || '1.0').split('.');
  let newVersion = base;
  if (parts.slice(0, 3).join('.') === base) {
    newVersion = base + '.' + (parseInt(parts[3] || '0') + 1);
  }

  log('sys', `📦 Publication v${newVersion} — ${orderedChannels.length} chaînes...`);

  const payload = {
    version:      newVersion,
    count:        orderedChannels.length,
    published_at: now.toISOString(),
    data:         orderedChannels,
  };

  // 1. channels_data
  const r1 = await sbPatch('channels_data', 'id=eq.1', payload);
  if (!r1.ok) {
    log('warn', 'Pas de ligne id=1 → création...');
    await sbPost('channels_data', { id: 1, ...payload });
  }
  log('ok', `✅ channels_data publié (v${newVersion})`);

  // 2. channel_priorities
  log('sys', `⭐ channel_priorities — ${prioURLs.length} URLs...`);
  const r2 = await sbPatch('channel_priorities', 'id=eq.1', {
    priorities: prioURLs,
    saved_at:   now.toISOString(),
  });
  if (!r2.ok) {
    log('warn', 'Pas de ligne id=1 → création...');
    await sbPost('channel_priorities', {
      id:         1,
      priorities: prioURLs,
      saved_at:   now.toISOString(),
    });
  }
  log('ok', `✅ channel_priorities mis à jour (${prioURLs.length} prioritaires)`);

  return newVersion;
}

// ════════════════════════════════════════════════════════
//  POINT D'ENTRÉE
// ════════════════════════════════════════════════════════
async function main() {
  log('sys', '══════════════════════════════════════════════════');
  log('sys', '🤖 SCAN AUTO — TRUE WORKER POOL');
  log('sys', `⚙️  Timeout: ${TIMEOUT_MS}ms | Workers: ${CONCURRENCY} | Méthode: HEAD`);
  log('sys', '══════════════════════════════════════════════════');

  // 1. Charger les chaînes
  log('sys', '🔄 Chargement depuis Supabase...');
  const rows = await sbGet(
    'channels_data',
    'select=version,count,data&order=published_at.desc&limit=1'
  );

  if (!rows?.length || !Array.isArray(rows[0].data) || !rows[0].data.length) {
    log('fail', 'Aucune chaîne dans Supabase (channels_data vide)');
    process.exit(1);
  }

  const allChannels    = rows[0].data.filter(c => c.url);
  const currentVersion = rows[0].version || '1.0';
  log('ok',   `${allChannels.length} chaînes chargées (v${currentVersion})`);
  log('info', `Estimation : ~${Math.ceil(allChannels.length / CONCURRENCY * TIMEOUT_MS / 1000 / 60)} min max`);

  // 2. Scanner avec le worker pool
  log('sys', `🚀 Démarrage — ${CONCURRENCY} workers en parallèle...`);
  const startTime = Date.now();
  const { okChannels, failChannels } = await workerPool(allChannels, CONCURRENCY);
  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

  log('sys', '══════════════════════════════════════════════════');
  log('sys', `🎉 SCAN TERMINÉ en ${elapsed} min`);
  log('ok',  `✅ Vivantes  : ${okChannels.length}`);
  log('fail',`❌ Mortes    : ${failChannels.length}`);
  log('sys', `📊 Réussite  : ${Math.round(okChannels.length / allChannels.length * 100)}%`);
  log('sys', '══════════════════════════════════════════════════');

  // 3. Publier
  const newVersion = await publishToSupabase(okChannels, failChannels, currentVersion);

  log('sys', '');
  log('sys', `🚀 PUBLICATION RÉUSSIE — v${newVersion}`);
  log('ok',  `⭐ ${okChannels.length} prioritaires en haut`);
  log('fail',`🔻 ${failChannels.length} en bas`);
}

main().catch(err => {
  console.error('❌ Erreur fatale :', err.message);
  process.exit(1);
});
