// ════════════════════════════════════════════════════════
//  SCAN AUTO — VERSION GITHUB ACTIONS
//  Reproduit exactement ce que le superadmin fait manuellement
//  dans l'onglet "Priorités" :
//    1. Lit les priorités manuelles (channel_priorities)
//    2. Scanne toutes les chaînes
//    3. Ordre final : prio-vivant → autre-vivant → prio-mort → autre-mort
//    4. INSERT nouvelle ligne dans channels_data (comme prioSaveAndPublish)
//    5. UPSERT channel_priorities
// ════════════════════════════════════════════════════════

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('❌ Variables manquantes : SUPABASE_URL et SUPABASE_KEY');
  process.exit(1);
}

const SB_HEADERS_R = {
  'apikey':        SUPABASE_KEY,
  'Authorization': 'Bearer ' + SUPABASE_KEY,
};
const SB_HEADERS_W = {
  ...SB_HEADERS_R,
  'Content-Type': 'application/json',
  'Prefer':       'return=minimal',
};

// ── CONFIG ────────────────────────────────────────────
const TIMEOUT_MS  = 3_000;   // 3 s par chaîne
const CONCURRENCY = 300;     // workers parallèles

// ════════════════════════════════════════════════════════
//  HELPERS LOG
// ════════════════════════════════════════════════════════
function log(type, msg) {
  const icons = { ok:'✅', fail:'❌', info:'ℹ️', warn:'⚠️', sys:'🔵' };
  const ts = new Date().toLocaleTimeString('fr-FR');
  console.log(`[${ts}] ${icons[type] || '·'} ${msg}`);
}

// ════════════════════════════════════════════════════════
//  HELPERS SUPABASE REST
// ════════════════════════════════════════════════════════
async function sbGet(table, params = '') {
  const q = params ? '?' + params : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}${q}`, { headers: SB_HEADERS_R });
  if (!r.ok) throw new Error(`sbGet ${table}: ${await r.text()}`);
  return r.json();
}

// INSERT — ajoute une nouvelle ligne (comme le superadmin)
async function sbInsert(table, body) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method:  'POST',
    headers: SB_HEADERS_W,
    body:    JSON.stringify(body),
  });
  if (!r.ok) {
    const err = await r.text();
    throw new Error(`sbInsert ${table}: HTTP ${r.status} — ${err}`);
  }
  return r;
}

// UPSERT sur id=1 (channel_priorities)
async function sbUpsert(table, body) {
  const r = await fetch(
    `${SUPABASE_URL}/rest/v1/${table}?on_conflict=id`,
    {
      method:  'POST',
      headers: {
        ...SB_HEADERS_W,
        'Prefer': 'resolution=merge-duplicates,return=minimal',
      },
      body: JSON.stringify(body),
    }
  );
  if (!r.ok) {
    const err = await r.text();
    throw new Error(`sbUpsert ${table}: HTTP ${r.status} — ${err}`);
  }
  return r;
}

// ════════════════════════════════════════════════════════
//  TEST D'UNE CHAÎNE
// ════════════════════════════════════════════════════════
async function testChannel(ch) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(ch.url, {
      method:  'HEAD',
      signal:  controller.signal,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; IPTVScanner/1.0)',
        'Accept':     '*/*',
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
//  WORKER POOL
// ════════════════════════════════════════════════════════
async function workerPool(channels, concurrency) {
  const results  = new Map();   // url → true/false
  const total    = channels.length;
  let   index    = 0;
  let   done     = 0;
  const t0       = Date.now();

  async function worker() {
    while (true) {
      const i = index++;
      if (i >= total) return;
      const ch = channels[i];
      results.set(ch.url, await testChannel(ch));
      done++;
      if (done % 2000 === 0 || done === total) {
        const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
        const pct     = Math.round(done / total * 100);
        const vit     = Math.round(done / (elapsed || 1));
        const rest    = vit > 0 ? Math.round((total - done) / vit) : '?';
        const ok      = [...results.values()].filter(Boolean).length;
        log('sys', `${pct}% — ${done}/${total} | ✅ ${ok} | ❌ ${done - ok} | ${vit} ch/s | ~${rest}s`);
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return results;
}

// ════════════════════════════════════════════════════════
//  PUBLICATION — reproduit prioSaveAndPublish() du superadmin
//
//  Ordre final :
//    1. chaînes prioritaires vivantes  (ordre de priorité)
//    2. autres chaînes vivantes
//    3. chaînes prioritaires mortes    (ordre de priorité)
//    4. autres chaînes mortes
// ════════════════════════════════════════════════════════
async function publishToSupabase(allChannels, scanResults, priorityURLs) {
  const prioSet = new Set(priorityURLs);

  // Séparer en 4 groupes
  const prioOk    = [];
  const prioFail  = [];
  const otherOk   = [];
  const otherFail = [];

  for (const ch of allChannels) {
    const alive = scanResults.get(ch.url) === true;
    if (prioSet.has(ch.url)) {
      alive ? prioOk.push(ch) : prioFail.push(ch);
    } else {
      alive ? otherOk.push(ch) : otherFail.push(ch);
    }
  }

  // Respecter l'ordre de priorité manuel dans prioOk et prioFail
  const prioIndex = new Map(priorityURLs.map((url, i) => [url, i]));
  prioOk.sort(  (a, b) => (prioIndex.get(a.url) ?? 0) - (prioIndex.get(b.url) ?? 0));
  prioFail.sort((a, b) => (prioIndex.get(a.url) ?? 0) - (prioIndex.get(b.url) ?? 0));

  const newData = [...prioOk, ...otherOk, ...prioFail, ...otherFail];

  // Version identique au format superadmin : YYYY.MM.DD-HHMM
  const now = new Date();
  const newVersion =
    now.getFullYear() + '.' +
    String(now.getMonth() + 1).padStart(2, '0') + '.' +
    String(now.getDate()).padStart(2, '0') + '-' +
    String(now.getHours()).padStart(2, '0') +
    String(now.getMinutes()).padStart(2, '0');

  const kb = (new Blob([JSON.stringify(newData)]).size / 1024).toFixed(1);

  log('sys', `📦 INSERT channels_data — v${newVersion} — ${newData.length} chaînes — ${kb} KB...`);

  // ── ÉTAPE 1 : INSERT nouvelle ligne dans channels_data ──
  await sbInsert('channels_data', {
    version:      newVersion,
    data:         newData,
    note:         `${prioOk.length} prioritaires en tête — scan auto GitHub Actions`,
    size_kb:      parseFloat(kb),
    published_at: now.toISOString(),
  });
  log('ok', `✅ channels_data inséré (v${newVersion})`);

  // ── ÉTAPE 2 : UPSERT channel_priorities ──
  //    On garde uniquement les URLs prioritaires encore vivantes en tête,
  //    mais on conserve aussi les mortes pour que l'admin les retrouve.
  const aliveprioURLs = priorityURLs.filter(u => scanResults.get(u) === true);

  log('sys', `⭐ UPSERT channel_priorities — ${aliveprioURLs.length} vivantes / ${priorityURLs.length} totales...`);
  await sbUpsert('channel_priorities', {
    id:         1,
    priorities: aliveprioURLs,    // seulement les vivantes restent en tête
    count:      aliveprioURLs.length,
    saved_at:   now.toISOString(),
  });
  log('ok', `✅ channel_priorities mis à jour`);

  return { newVersion, prioOk, prioFail, otherOk, otherFail };
}

// ════════════════════════════════════════════════════════
//  POINT D'ENTRÉE
// ════════════════════════════════════════════════════════
async function main() {
  log('sys', '══════════════════════════════════════════════════');
  log('sys', '🤖 SCAN AUTO — WORKER POOL (mode superadmin)');
  log('sys', `⚙️  Timeout: ${TIMEOUT_MS}ms | Workers: ${CONCURRENCY}`);
  log('sys', '══════════════════════════════════════════════════');

  // 1. Charger la dernière version des chaînes
  log('sys', '🔄 Chargement des chaînes depuis channels_data...');
  let allChannels = [];

  try {
    const rows = await sbGet(
      'channels_data',
      'select=version,count,data&order=published_at.desc&limit=1'
    );
    if (!rows?.length || !Array.isArray(rows[0].data) || !rows[0].data.length) {
      log('fail', 'channels_data vide ou introuvable !');
      process.exit(1);
    }
    allChannels = rows[0].data.filter(c => c?.url);
    log('ok', `${allChannels.length} chaînes chargées (v${rows[0].version})`);
  } catch (e) {
    log('fail', `Erreur chargement : ${e.message}`);
    process.exit(1);
  }

  if (!allChannels.length) {
    log('fail', 'Aucune chaîne trouvée !');
    process.exit(1);
  }

  // 2. Charger les priorités manuelles
  log('sys', '⭐ Chargement des priorités manuelles...');
  let priorityURLs = [];

  try {
    const prows = await sbGet(
      'channel_priorities',
      'select=priorities&id=eq.1&limit=1'
    );
    if (prows?.length && Array.isArray(prows[0].priorities)) {
      priorityURLs = prows[0].priorities.filter(u =>
        allChannels.some(c => c.url === u)
      );
      log('ok', `${priorityURLs.length} URLs prioritaires chargées`);
    } else {
      log('warn', 'Aucune priorité manuelle trouvée — scan sans priorité');
    }
  } catch (e) {
    log('warn', `Impossible de charger les priorités : ${e.message} — on continue sans`);
  }

  log('info', `Estimation : ~${Math.ceil(allChannels.length / CONCURRENCY * TIMEOUT_MS / 1000 / 60)} min max`);

  // 3. Scanner
  log('sys', `🚀 Démarrage — ${CONCURRENCY} workers...`);
  const t0 = Date.now();
  const scanResults = await workerPool(allChannels, CONCURRENCY);
  const elapsed = ((Date.now() - t0) / 1000 / 60).toFixed(1);

  const totalOk   = [...scanResults.values()].filter(Boolean).length;
  const totalFail = allChannels.length - totalOk;

  log('sys', '══════════════════════════════════════════════════');
  log('sys', `🎉 SCAN TERMINÉ en ${elapsed} min`);
  log('ok',  `✅ Vivantes  : ${totalOk}`);
  log('fail',`❌ Mortes    : ${totalFail}`);
  log('sys', `📊 Réussite  : ${Math.round(totalOk / allChannels.length * 100)}%`);
  log('sys', '══════════════════════════════════════════════════');

  // 4. Publier
  const { newVersion, prioOk, prioFail, otherOk, otherFail } =
    await publishToSupabase(allChannels, scanResults, priorityURLs);

  log('sys', '');
  log('sys', `🚀 PUBLICATION RÉUSSIE — v${newVersion}`);
  log('ok',  `⭐ Prio vivantes   : ${prioOk.length}`);
  log('ok',  `📺 Autres vivantes : ${otherOk.length}`);
  log('fail',`💀 Prio mortes     : ${prioFail.length}`);
  log('fail',`🔻 Autres mortes   : ${otherFail.length}`);
}

main().catch(err => {
  console.error('❌ Erreur fatale :', err.message);
  process.exit(1);
});
