// ════════════════════════════════════════════════════════
//  SCAN AUTO — GitHub Actions + FFmpeg
//  ✅ FFmpeg teste vraiment si la vidéo se décode
//  ✅ channel_priorities n'est JAMAIS modifié
//  ✅ Ton classement est préservé à chaque scan
//  ✅ 50 workers en parallèle (true worker pool)
// ════════════════════════════════════════════════════════

import { spawn }  from 'child_process';
import { resolve } from 'path';

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
const CONCURRENCY   = 50;     // 50 workers en parallèle
const FFPROBE_SECS  = 6;      // durée max de lecture FFmpeg par chaîne
const FFPROBE_PATH  = 'ffprobe'; // installé par le workflow yml

// ════════════════════════════════════════════════════════
//  LOG
// ════════════════════════════════════════════════════════
function log(type, msg) {
  const icons = { ok: '✅', fail: '❌', info: 'ℹ️', warn: '⚠️', sys: '🔵' };
  const ts = new Date().toLocaleTimeString('fr-FR');
  console.log(`[${ts}] ${icons[type] || '·'} ${msg}`);
}

// ════════════════════════════════════════════════════════
//  TEST FFmpeg — vraie lecture vidéo
//
//  ffprobe essaie de lire FFPROBE_SECS secondes du flux
//  Si des paquets vidéo/audio sont reçus → VIVANTE ✅
//  Si timeout / erreur → MORTE ❌
//
//  User-agents réalistes (évite blocage serveurs IPTV)
// ════════════════════════════════════════════════════════
const USER_AGENTS = [
  'VLC/3.0.20 LibVLC/3.0.20',
  'Kodi/20.2 (Linux)',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
  'stagefright/1.2 (Linux;Android 11)',
  'lavf/58.76.100',
];

function randomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function testChannel(ch) {
  return new Promise(resolve => {
    const ua   = randomUA();
    const args = [
      '-v',         'error',
      '-user_agent', ua,
      '-timeout',   String(FFPROBE_SECS * 1_000_000), // en microsecondes
      '-i',          ch.url,
      '-t',         String(FFPROBE_SECS),              // lire max N secondes
      '-select_streams', 'v:0',                        // stream vidéo principal
      '-show_entries', 'packet=pts_time',              // chercher des paquets
      '-of',        'csv=p=0',
    ];

    let stdout  = '';
    let stderr  = '';
    let done    = false;

    const proc = spawn(FFPROBE_PATH, args, { stdio: ['ignore', 'pipe', 'pipe'] });

    // Tuer après FFPROBE_SECS + 3s de marge
    const killer = setTimeout(() => {
      if (!done) { proc.kill('SIGKILL'); }
    }, (FFPROBE_SECS + 3) * 1000);

    proc.stdout.on('data', d => { stdout += d.toString(); });
    proc.stderr.on('data', d => { stderr += d.toString(); });

    proc.on('close', code => {
      if (done) return;
      done = true;
      clearTimeout(killer);

      // Si ffprobe a reçu des paquets vidéo → stdout non vide
      // OU si stderr contient des infos de stream (audio seul OK aussi)
      const hasVideo  = stdout.trim().length > 0;
      const hasStream = stderr.includes('Stream #') &&
                        !stderr.includes('Connection refused') &&
                        !stderr.includes('No such file') &&
                        !stderr.includes('Failed to open');

      resolve(hasVideo || hasStream);
    });

    proc.on('error', () => {
      if (!done) { done = true; clearTimeout(killer); resolve(false); }
    });
  });
}

// ════════════════════════════════════════════════════════
//  TRUE WORKER POOL — 50 workers indépendants
//  Chaque worker prend la tâche suivante dès qu'il finit
//  Pas de batch → pas d'attente inutile
// ════════════════════════════════════════════════════════
async function workerPool(channels) {
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

      // Log toutes les 500 chaînes
      if (done % 500 === 0 || done === total) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
        const pct     = Math.round(done / total * 100);
        const spd     = Math.round(done / (elapsed || 1));
        const eta     = spd > 0 ? Math.round((total - done) / spd) : '?';
        log('sys', `${pct}% — ${done}/${total} | ✅ ${okChannels.length} | ❌ ${failChannels.length} | ${spd} ch/s | ~${eta}s`);
      }
    }
  }

  // Lancer les 50 workers en même temps
  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

  return { okChannels, failChannels };
}

// ════════════════════════════════════════════════════════
//  SUPABASE — helpers
// ════════════════════════════════════════════════════════
async function sbGet(table, params = '') {
  const q = params ? '?' + params : '';
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}${q}`, { headers: SB_HEADERS_R });
  if (!r.ok) throw new Error(`sbGet ${table}: ${await r.text()}`);
  return r.json();
}

async function sbInsert(table, body) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method:  'POST',
    headers: SB_HEADERS_W,
    body:    JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`sbInsert ${table}: HTTP ${r.status} — ${await r.text()}`);
  return r;
}

// ════════════════════════════════════════════════════════
//  PUBLICATION
//
//  Ordre final :
//    1. Tes priorités VIVANTES  (ton ordre — intact) ⭐
//    2. Autres vivantes
//    3. Tes priorités MORTES    (ton ordre — intact)
//    4. Autres mortes
//
//  ⚠️  channel_priorities n'est JAMAIS modifié ici
// ════════════════════════════════════════════════════════
async function publishToSupabase(okChannels, failChannels, priorityURLs) {
  const prioSet   = new Set(priorityURLs);
  const prioIndex = new Map(priorityURLs.map((url, i) => [url, i]));

  // Séparer vivantes / mortes selon le classement manuel
  const prioOk    = okChannels  .filter(c =>  prioSet.has(c.url));
  const otherOk   = okChannels  .filter(c => !prioSet.has(c.url));
  const prioFail  = failChannels.filter(c =>  prioSet.has(c.url));
  const otherFail = failChannels.filter(c => !prioSet.has(c.url));

  // Respecter TON ordre dans chaque groupe
  const byPrio = (a, b) => (prioIndex.get(a.url) ?? 0) - (prioIndex.get(b.url) ?? 0);
  prioOk  .sort(byPrio);
  prioFail.sort(byPrio);

  const newData = [...prioOk, ...otherOk, ...prioFail, ...otherFail];

  // Version horodatée
  const now = new Date();
  const newVersion =
    now.getFullYear() + '.' +
    String(now.getMonth() + 1).padStart(2, '0') + '.' +
    String(now.getDate()).padStart(2, '0') + '-' +
    String(now.getHours()).padStart(2, '0') +
    String(now.getMinutes()).padStart(2, '0');

  const kb = (new Blob([JSON.stringify(newData)]).size / 1024).toFixed(1);

  log('sys', `📦 INSERT channels_data v${newVersion} — ${newData.length} chaînes — ${kb} KB`);
  log('info', `   ⭐ ${prioOk.length} prioritaires vivantes en tête`);
  log('info', `   📺 ${otherOk.length} autres vivantes`);
  log('info', `   💀 ${prioFail.length} prioritaires mortes`);
  log('info', `   🔻 ${otherFail.length} autres mortes en bas`);

  await sbInsert('channels_data', {
    version:      newVersion,
    data:         newData,
    note:         `FFmpeg scan — ${prioOk.length} prio vivantes / ${okChannels.length} total vivantes`,
    size_kb:      parseFloat(kb),
    published_at: now.toISOString(),
  });

  log('ok', `✅ channels_data publié (v${newVersion})`);
  log('warn', `⚠️  channel_priorities NON modifié — ton classement est préservé ✅`);

  return { newVersion, prioOk, otherOk, prioFail, otherFail };
}

// ════════════════════════════════════════════════════════
//  MAIN
// ════════════════════════════════════════════════════════
async function main() {
  log('sys', '══════════════════════════════════════════════════');
  log('sys', '🤖 SCAN AUTO — FFmpeg + Worker Pool');
  log('sys', `⚙️  Workers: ${CONCURRENCY} | Durée FFmpeg: ${FFPROBE_SECS}s/chaîne`);
  log('sys', '══════════════════════════════════════════════════');

  // 1. Charger les chaînes depuis le dernier channels_data
  log('sys', '🔄 Chargement des chaînes depuis Supabase...');
  let allChannels = [];
  try {
    const rows = await sbGet(
      'channels_data',
      'select=version,count,data&order=published_at.desc&limit=1'
    );
    if (!rows?.length || !Array.isArray(rows[0].data) || !rows[0].data.length) {
      log('fail', 'channels_data vide — importe d\'abord un fichier JSON');
      process.exit(1);
    }
    allChannels = rows[0].data.filter(c => c?.url);
    log('ok', `${allChannels.length} chaînes chargées (v${rows[0].version})`);
  } catch (e) {
    log('fail', `Erreur chargement chaînes : ${e.message}`);
    process.exit(1);
  }

  // 2. Charger TON classement (lecture seule — jamais modifié)
  log('sys', '⭐ Chargement du classement depuis channel_priorities...');
  let priorityURLs = [];
  try {
    const prows = await sbGet('channel_priorities', 'select=priorities&id=eq.1&limit=1');
    if (prows?.length && Array.isArray(prows[0].priorities)) {
      priorityURLs = prows[0].priorities.filter(u => allChannels.some(c => c.url === u));
      log('ok', `${priorityURLs.length} chaînes dans ton classement`);
    } else {
      log('warn', 'Aucun classement encore — classe les chaînes depuis le HTML après ce scan');
    }
  } catch (e) {
    log('warn', `Classement non chargé : ${e.message} — on continue sans`);
  }

  // 3. Estimation
  const estMin = Math.ceil(allChannels.length / CONCURRENCY * (FFPROBE_SECS + 2) / 60);
  log('info', `Estimation : ~${estMin} min pour ${allChannels.length} chaînes`);

  // 4. Scanner avec FFmpeg
  log('sys', `🚀 Démarrage — ${CONCURRENCY} workers FFmpeg en parallèle...`);
  const t0 = Date.now();
  const { okChannels, failChannels } = await workerPool(allChannels);
  const elapsed = ((Date.now() - t0) / 1000 / 60).toFixed(1);

  log('sys', '══════════════════════════════════════════════════');
  log('sys', `🎉 SCAN TERMINÉ en ${elapsed} min`);
  log('ok',  `✅ Vivantes : ${okChannels.length}`);
  log('fail',`❌ Mortes   : ${failChannels.length}`);
  log('sys', `📊 Réussite : ${Math.round(okChannels.length / allChannels.length * 100)}%`);
  log('sys', '══════════════════════════════════════════════════');

  // 5. Publier (channel_priorities n'est pas touché)
  const result = await publishToSupabase(okChannels, failChannels, priorityURLs);

  log('sys', '');
  log('sys', `🚀 PUBLICATION RÉUSSIE — v${result.newVersion}`);
  log('ok',  `⭐ Prioritaires vivantes  : ${result.prioOk.length}`);
  log('ok',  `📺 Autres vivantes        : ${result.otherOk.length}`);
  log('fail',`💀 Prioritaires mortes    : ${result.prioFail.length}`);
  log('fail',`🔻 Autres mortes          : ${result.otherFail.length}`);
  log('sys', '');
  log('sys', '💡 Classement préservé — channel_priorities intact ✅');
}

main().catch(err => {
  console.error('❌ Erreur fatale :', err.message);
  process.exit(1);
});
