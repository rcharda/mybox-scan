// ════════════════════════════════════════════════════════
//  SCAN AUTO — VERSION GITHUB ACTIONS
//  ✅ Fix chaînes beIN Sports / IPTV mal détectées :
//    - HEAD d'abord (pas de téléchargement du flux)
//    - Fallback GET avec Range: bytes=0-1023 si HEAD échoue
//    - Fallback GET classique si Range refusé
//    - 4 User-Agents réalistes rotatifs (VLC, Kodi, Chrome...)
//    - Timeout 8s au lieu de 5s
//    - 401/403/405/500+ gérés correctement
// ════════════════════════════════════════════════════════

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("❌ Variables manquantes : SUPABASE_URL et SUPABASE_KEY");
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
const TIMEOUT_MS  = 8_000;  // ✅ 8s (était 5s — trop court pour beIN/OSN)
const CONCURRENCY = 50;     // 50 workers en parallèle

// ✅ User-Agents réalistes — évite le blocage "IPTVScanner"
const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'VLC/3.0.20 LibVLC/3.0.20 (Windows 10; x64)',
  'Kodi/20.2 (Windows; x86_64) App_Bitness/64 Version/20.2-Git:20230726-ce9ab3be6f',
  'stagefright/1.2 (Linux;Android 11)',
  'okhttp/4.11.0',
  'ExoPlayerLib/2.18.1 (Linux;Android 13) ExoPlayerLib/2.18.1',
];

function randomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

// ════════════════════════════════════════════════════════
//  LOG
// ════════════════════════════════════════════════════════
function log(type, msg) {
  const icons = { ok: '✅', fail: '❌', info: 'ℹ️', warn: '⚠️', sys: '🔵' };
  const ts = new Date().toLocaleTimeString('fr-FR');
  console.log(`[${ts}] ${icons[type] || '·'} ${msg}`);
}

// ════════════════════════════════════════════════════════
//  HELPERS SUPABASE
// ════════════════════════════════════════════════════════
async function sbGet(table, params = '') {
  const q = params ? '?' + params : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}${q}`, {
    headers: SB_HEADERS_R,
  });
  if (!r.ok) throw new Error(`sbGet ${table}: ${await r.text()}`);
  return r.json();
}

// ✅ INSERT sans id — laisse bigserial générer l'id automatiquement
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

// ✅ UPSERT sur id=1 (channel_priorities a une vraie PK fixe)
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
//  TEST D'UNE CHAÎNE — 3 tentatives en cascade
//
//  Tentative 1 : HEAD  → rapide, pas de téléchargement
//  Tentative 2 : GET Range:0-1023 → si HEAD = 405
//  Tentative 3 : GET classique    → dernier recours
//
//  Codes considérés VIVANTS :
//    < 500         → serveur répond (200, 206, 302, 401, 403, 404...)
//    401 / 403     → protégé mais le serveur EXISTE et répond
//    405           → method not allowed → HEAD refusé → on retente en GET
//
//  Codes considérés MORTS :
//    >= 500        → erreur serveur
//    timeout/abort → flux injoignable
// ════════════════════════════════════════════════════════
async function testChannel(ch) {
  const ua = randomUA();

  // ── Tentative 1 : HEAD ────────────────────────────────
  try {
    const ctrl1 = new AbortController();
    const t1 = setTimeout(() => ctrl1.abort(), TIMEOUT_MS);

    const res = await fetch(ch.url, {
      method:   'HEAD',
      signal:   ctrl1.signal,
      headers:  {
        'User-Agent': ua,
        'Accept':     '*/*',
        'Connection': 'keep-alive',
        'Icy-MetaData': '1',
      },
      redirect: 'follow',
    });
    clearTimeout(t1);

    // 405 = server refuses HEAD → fallback GET
    if (res.status === 405) throw new Error('HEAD_NOT_ALLOWED');

    // Tout ce qui est < 500 = le serveur répond = flux existe
    return res.status < 500;

  } catch (e1) {

    // ── Tentative 2 : GET avec Range (lit seulement 1 KB) ──
    if (e1.message !== 'HEAD_NOT_ALLOWED' && e1.name !== 'AbortError') {
      // Vrai timeout ou erreur réseau → on retente quand même
    }

    try {
      const ctrl2 = new AbortController();
      const t2 = setTimeout(() => ctrl2.abort(), TIMEOUT_MS);

      const res2 = await fetch(ch.url, {
        method:   'GET',
        signal:   ctrl2.signal,
        headers:  {
          'User-Agent': ua,
          'Accept':     '*/*',
          'Range':      'bytes=0-1023',   // lire seulement 1 KB
          'Connection': 'keep-alive',
        },
        redirect: 'follow',
      });
      clearTimeout(t2);

      // Annuler le body pour ne pas consommer le flux entier
      try { await res2.body?.cancel(); } catch {}

      // 206 = Partial Content = parfait, 200 = aussi bon
      // 416 = Range not satisfiable mais serveur répond
      if (res2.status < 500) return true;

      // ── Tentative 3 : GET classique sans Range ──────────
      const ctrl3 = new AbortController();
      const t3 = setTimeout(() => ctrl3.abort(), TIMEOUT_MS);

      const res3 = await fetch(ch.url, {
        method:   'GET',
        signal:   ctrl3.signal,
        headers:  {
          'User-Agent': ua,
          'Accept':     '*/*',
        },
        redirect: 'follow',
      });
      clearTimeout(t3);
      try { await res3.body?.cancel(); } catch {}

      return res3.status < 500;

    } catch {
      return false;
    }
  }
}

// ════════════════════════════════════════════════════════
//  SCAN — batch de 50 en parallèle
// ════════════════════════════════════════════════════════
async function scanAllChannels(channels) {
  const okChannels   = [];
  const failChannels = [];
  let done  = 0;
  const total = channels.length;

  for (let i = 0; i < total; i += CONCURRENCY) {
    const batch   = channels.slice(i, i + CONCURRENCY);
    const results = await Promise.all(
      batch.map(ch => testChannel(ch).then(ok => ({ ch, ok })))
    );

    for (const { ch, ok } of results) {
      done++;
      ok ? okChannels.push(ch) : failChannels.push(ch);
    }

    const pct = Math.round(done / total * 100);
    if (i % (CONCURRENCY * 5) === 0 || done === total) {
      log('sys', `${pct}% (${done}/${total}) — ✅ ${okChannels.length} | ❌ ${failChannels.length}`);
    }
  }

  return { okChannels, failChannels };
}

// ════════════════════════════════════════════════════════
//  PUBLICATION
//
//  Ordre final (comme superadmin onglet Priorités) :
//    1. prioritaires vivantes  (ordre manuel)
//    2. autres vivantes
//    3. prioritaires mortes    (ordre manuel)
//    4. autres mortes
// ════════════════════════════════════════════════════════
async function publishToSupabase(allChannels, okChannels, failChannels, priorityURLs) {

  // ── Trier en respectant les priorités manuelles ──────
  const prioSet   = new Set(priorityURLs);
  const prioIndex = new Map(priorityURLs.map((url, i) => [url, i]));

  const prioOk    = okChannels  .filter(c =>  prioSet.has(c.url));
  const otherOk   = okChannels  .filter(c => !prioSet.has(c.url));
  const prioFail  = failChannels.filter(c =>  prioSet.has(c.url));
  const otherFail = failChannels.filter(c => !prioSet.has(c.url));

  prioOk  .sort((a, b) => (prioIndex.get(a.url) ?? 0) - (prioIndex.get(b.url) ?? 0));
  prioFail.sort((a, b) => (prioIndex.get(a.url) ?? 0) - (prioIndex.get(b.url) ?? 0));

  const newData = [...prioOk, ...otherOk, ...prioFail, ...otherFail];

  // ── Version au format superadmin : YYYY.MM.DD-HHMM ──
  const now = new Date();
  const newVersion =
    now.getFullYear() + '.' +
    String(now.getMonth() + 1).padStart(2, '0') + '.' +
    String(now.getDate()).padStart(2, '0') + '-' +
    String(now.getHours()).padStart(2, '0') +
    String(now.getMinutes()).padStart(2, '0');

  const kb = (new Blob([JSON.stringify(newData)]).size / 1024).toFixed(1);

  // ── ÉTAPE 1 : INSERT dans channels_data ─────────────
  log('sys', `📦 INSERT channels_data v${newVersion} — ${newData.length} chaînes — ${kb} KB...`);
  try {
    await sbInsert('channels_data', {
      version:      newVersion,
      data:         newData,
      note:         `${prioOk.length} prioritaires en tête — scan GitHub Actions`,
      size_kb:      parseFloat(kb),
      published_at: now.toISOString(),
    });
    log('ok', `✅ channels_data publié (v${newVersion})`);
  } catch (e) {
    log('fail', `❌ channels_data ÉCHEC : ${e.message}`);
    throw e;
  }

  // ── ÉTAPE 2 : UPSERT channel_priorities ─────────────
  const alivePrioURLs = priorityURLs.filter(u => okChannels.some(c => c.url === u));
  log('sys', `⭐ UPSERT channel_priorities — ${alivePrioURLs.length}/${priorityURLs.length} vivantes...`);
  try {
    await sbUpsert('channel_priorities', {
      id:         1,
      priorities: alivePrioURLs,
      count:      alivePrioURLs.length,
      saved_at:   now.toISOString(),
    });
    log('ok', `✅ channel_priorities mis à jour`);
  } catch (e) {
    log('fail', `❌ channel_priorities ÉCHEC : ${e.message}`);
    throw e;
  }

  return { newVersion, prioOk, prioFail, otherOk, otherFail };
}

// ════════════════════════════════════════════════════════
//  MAIN
// ════════════════════════════════════════════════════════
async function main() {
  log('sys', '══════════════════════════════════════════════════');
  log('sys', '🤖 SCAN AUTO — DÉMARRAGE');
  log('sys', `⚙️  Timeout: ${TIMEOUT_MS}ms | Workers: ${CONCURRENCY}`);
  log('sys', '══════════════════════════════════════════════════');

  // 1. Charger les chaînes
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
      log('ok', `${priorityURLs.length} priorités chargées`);
    } else {
      log('warn', 'Aucune priorité manuelle — ordre par défaut');
    }
  } catch (e) {
    log('warn', `Priorités non chargées : ${e.message} — on continue sans`);
  }

  const estMin = Math.ceil(allChannels.length / CONCURRENCY * TIMEOUT_MS / 1000 / 60);
  log('info', `~${estMin} min estimées pour ${allChannels.length} chaînes`);

  // 3. Scanner
  log('sys', `🚀 Scan — ${CONCURRENCY} workers...`);
  const t0 = Date.now();
  const { okChannels, failChannels } = await scanAllChannels(allChannels);
  const elapsed = ((Date.now() - t0) / 1000 / 60).toFixed(1);

  log('sys', '══════════════════════════════════════════════════');
  log('sys', `🎉 SCAN TERMINÉ en ${elapsed} min`);
  log('ok',  `✅ Vivantes : ${okChannels.length}`);
  log('fail',`❌ Mortes   : ${failChannels.length}`);
  log('sys', `📊 Réussite : ${Math.round(okChannels.length / allChannels.length * 100)}%`);
  log('sys', '══════════════════════════════════════════════════');

  // 4. Publier
  const { newVersion, prioOk, prioFail, otherOk, otherFail } =
    await publishToSupabase(allChannels, okChannels, failChannels, priorityURLs);

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
