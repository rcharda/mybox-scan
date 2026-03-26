// ════════════════════════════════════════════════════════
//  SCAN AUTO — VERSION SERVEUR (GitHub Actions)
//  TRUE WORKER POOL + SUPABASE STORAGE
//  Résout : payload 31 000 chaînes trop grand pour REST API
//  Solution : JSON → Supabase Storage, URL → channels_data
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
const TIMEOUT_MS  = 3_000;   // 3s par chaîne
const CONCURRENCY = 300;     // 300 workers en parallèle
// Estimation : ~5 min pour 31 000 chaînes

// ════════════════════════════════════════════════════════
//  HELPERS LOG
// ════════════════════════════════════════════════════════
function log(type, msg) {
  const icons = { ok: '✅', fail: '❌', info: 'ℹ️', warn: '⚠️', sys: '🔵' };
  const ts = new Date().toLocaleTimeString('fr-FR');
  console.log(`[${ts}] ${icons[type] || '·'} ${msg}`);
}

// ════════════════════════════════════════════════════════
//  HELPERS SUPABASE REST (petits payloads)
// ════════════════════════════════════════════════════════
async function sbGet(table, params = '') {
  const q = params ? '?' + params : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}${q}`, { headers: SB_HEADERS_R });
  if (!r.ok) throw new Error(`sbGet ${table}: ${await r.text()}`);
  return r.json();
}

async function sbPatch(table, filter, body) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${filter}`, {
    method: 'PATCH',
    headers: SB_HEADERS_W,
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const err = await r.text();
    throw new Error(`sbPatch ${table}: HTTP ${r.status} — ${err}`);
  }
  return r;
}

async function sbPost(table, body) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: { ...SB_HEADERS_W, 'Prefer': 'return=minimal' },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const err = await r.text();
    throw new Error(`sbPost ${table}: HTTP ${r.status} — ${err}`);
  }
  return r;
}

// ════════════════════════════════════════════════════════
//  SUPABASE STORAGE — upload du gros JSON
//  Bucket : iptv-data (public)
//  Fichier : channels.json (écrasé à chaque scan)
// ════════════════════════════════════════════════════════
async function uploadToStorage(jsonString) {
  const BUCKET = 'iptv-data';
  const FILE   = 'channels.json';
  const url    = `${SUPABASE_URL}/storage/v1/object/${BUCKET}/${FILE}`;

  log('sys', `☁️  Upload channels.json → Storage (${(jsonString.length / 1024 / 1024).toFixed(1)} MB)...`);

  // Tentative UPSERT (update si existe déjà)
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      'apikey':          SUPABASE_KEY,
      'Authorization':   'Bearer ' + SUPABASE_KEY,
      'Content-Type':    'application/json',
      'x-upsert':        'true',       // écrase si le fichier existe
      'Cache-Control':   'no-cache',
    },
    body: jsonString,
  });

  if (!r.ok) {
    const err = await r.text();
    throw new Error(`Storage upload: HTTP ${r.status} — ${err}`);
  }

  // URL publique du fichier
  const publicUrl = `${SUPABASE_URL}/storage/v1/object/public/${BUCKET}/${FILE}`;
  log('ok', `✅ Fichier uploadé → ${publicUrl}`);
  return publicUrl;
}

// ════════════════════════════════════════════════════════
//  TEST D'UNE CHAÎNE — HEAD HTTP (rapide, pas de body)
// ════════════════════════════════════════════════════════
async function testChannel(ch) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
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
    return res.status < 400 || res.status === 401 || res.status === 403;
  } catch {
    clearTimeout(timer);
    return false;
  }
}

// ════════════════════════════════════════════════════════
//  TRUE WORKER POOL
//  Chaque worker prend la tâche suivante dès qu'il finit
// ════════════════════════════════════════════════════════
async function workerPool(channels, concurrency) {
  const okChannels   = [];
  const failChannels = [];
  const total        = channels.length;
  let   index        = 0;
  let   done         = 0;
  const startTime    = Date.now();

  async function worker() {
    while (true) {
      const i = index++;
      if (i >= total) return;

      const ch = channels[i];
      const ok = await testChannel(ch);

      if (ok) okChannels.push(ch);
      else    failChannels.push(ch);

      done++;

      if (done % 2000 === 0 || done === total) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        const pct     = Math.round((done / total) * 100);
        const vitesse = Math.round(done / (elapsed || 1));
        const restant = vitesse > 0 ? Math.round((total - done) / vitesse) : '?';
        log('sys', `${pct}% — ${done}/${total} | ✅ ${okChannels.length} | ❌ ${failChannels.length} | ${vitesse} ch/s | ~${restant}s restantes`);
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return { okChannels, failChannels };
}

// ════════════════════════════════════════════════════════
//  PUBLICATION — 2 étapes :
//  1. Gros JSON → Supabase Storage (pas de limite taille)
//  2. Petits metadata + URL → channels_data (REST API)
// ════════════════════════════════════════════════════════
async function publishToSupabase(okChannels, failChannels, currentVersion) {
  const orderedChannels = [...okChannels, ...failChannels];
  const prioURLs        = okChannels.map(c => c.url);

  // Calcul version
  const now  = new Date();
  const base = `${now.getFullYear()}.${String(now.getMonth()+1).padStart(2,'0')}.${String(now.getDate()).padStart(2,'0')}`;
  const parts = (currentVersion || '1.0').split('.');
  let newVersion = base;
  if (parts.slice(0,3).join('.') === base) {
    newVersion = `${base}.${parseInt(parts[3] || '0') + 1}`;
  }

  // ── ÉTAPE 1 : Upload JSON dans Storage ──────────────
  const jsonPayload = JSON.stringify({
    version:      newVersion,
    count:        orderedChannels.length,
    published_at: now.toISOString(),
    channels:     orderedChannels,       // ← gros tableau ici
  });

  const dataUrl = await uploadToStorage(jsonPayload);

  // ── ÉTAPE 2 : Metadata légers dans channels_data ────
  // Seulement version + count + url → très petit payload
  log('sys', `📦 Mise à jour channels_data (metadata uniquement)...`);

  const metadata = {
    version:      newVersion,
    count:        orderedChannels.length,
    published_at: now.toISOString(),
    data_url:     dataUrl,               // ← URL du fichier Storage
    data:         [],                    // ← tableau vide (plus de gros payload)
  };

  try {
    await sbPatch('channels_data', 'id=eq.1', metadata);
    log('ok', `✅ channels_data mis à jour (v${newVersion})`);
  } catch (e) {
    log('warn', `PATCH échoué : ${e.message} → tentative INSERT...`);
    try {
      await sbPost('channels_data', { id: 1, ...metadata });
      log('ok', `✅ channels_data créé (v${newVersion})`);
    } catch (e2) {
      log('fail', `❌ Impossible d'écrire channels_data : ${e2.message}`);
      throw e2;
    }
  }

  // ── ÉTAPE 3 : channel_priorities ────────────────────
  log('sys', `⭐ channel_priorities (${prioURLs.length} URLs)...`);
  const prioPayload = { priorities: prioURLs, saved_at: now.toISOString() };

  try {
    await sbPatch('channel_priorities', 'id=eq.1', prioPayload);
    log('ok', `✅ channel_priorities mis à jour`);
  } catch (e) {
    log('warn', `PATCH échoué : ${e.message} → tentative INSERT...`);
    try {
      await sbPost('channel_priorities', { id: 1, ...prioPayload });
      log('ok', `✅ channel_priorities créé`);
    } catch (e2) {
      log('fail', `❌ Impossible d'écrire channel_priorities : ${e2.message}`);
      throw e2;
    }
  }

  return { newVersion, dataUrl };
}

// ════════════════════════════════════════════════════════
//  POINT D'ENTRÉE
// ════════════════════════════════════════════════════════
async function main() {
  log('sys', '══════════════════════════════════════════════════');
  log('sys', '🤖 SCAN AUTO — WORKER POOL + STORAGE');
  log('sys', `⚙️  Timeout: ${TIMEOUT_MS}ms | Workers: ${CONCURRENCY}`);
  log('sys', '══════════════════════════════════════════════════');

  // 1. Charger les chaînes depuis Storage ou channels_data
  log('sys', '🔄 Chargement des chaînes depuis Supabase...');

  let allChannels    = [];
  let currentVersion = '1.0';

  try {
    const rows = await sbGet('channels_data', 'select=version,count,data,data_url&order=published_at.desc&limit=1');

    if (rows?.length) {
      currentVersion = rows[0].version || '1.0';

      // Essayer de charger depuis Storage (nouveau format)
      if (rows[0].data_url) {
        log('info', `📂 Chargement depuis Storage : ${rows[0].data_url}`);
        const r = await fetch(rows[0].data_url + '?t=' + Date.now());
        if (r.ok) {
          const json = await r.json();
          allChannels = (json.channels || json.data || []).filter(c => c.url);
          log('ok', `${allChannels.length} chaînes chargées depuis Storage`);
        }
      }

      // Fallback : ancien format (data directement dans la table)
      if (!allChannels.length && Array.isArray(rows[0].data) && rows[0].data.length) {
        allChannels = rows[0].data.filter(c => c.url);
        log('ok', `${allChannels.length} chaînes chargées depuis channels_data.data`);
      }
    }
  } catch (e) {
    log('fail', `Erreur chargement : ${e.message}`);
    process.exit(1);
  }

  if (!allChannels.length) {
    log('fail', 'Aucune chaîne trouvée ! Vérifiez channels_data ou le bucket Storage.');
    process.exit(1);
  }

  log('info', `Estimation : ~${Math.ceil(allChannels.length / CONCURRENCY * TIMEOUT_MS / 1000 / 60)} min max`);

  // 2. Scanner
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
  const { newVersion, dataUrl } = await publishToSupabase(okChannels, failChannels, currentVersion);

  log('sys', '');
  log('sys', `🚀 PUBLICATION RÉUSSIE — v${newVersion}`);
  log('ok',  `☁️  Storage  : ${dataUrl}`);
  log('ok',  `⭐ Prioritaires : ${okChannels.length}`);
  log('fail',`🔻 En bas       : ${failChannels.length}`);
}

main().catch(err => {
  console.error('❌ Erreur fatale :', err.message);
  process.exit(1);
});
