// ════════════════════════════════════════════════════════
//  PUBLICATION — MÉTHODE SYNCHRONISÉE (SuperAdmin & App)
// ════════════════════════════════════════════════════════
async function publishToSupabase(okChannels, failChannels, currentVersion) {
  
  // 1. Préparation de la liste ordonnée (Vivantes en haut, Mortes en bas)
  const orderedChannels = [...okChannels, ...failChannels];
  const prioURLs = okChannels.map(c => c.url);

  // 2. Génération de la version (Format: AAAA.MM.JJ-HHmm)
  const now = new Date();
  const newVersion = now.getFullYear() + '.' + 
    String(now.getMonth() + 1).padStart(2, '0') + '.' + 
    String(now.getDate()).padStart(2, '0') + '-' +
    String(now.getHours()).padStart(2, '0') + String(now.getMinutes()).padStart(2, '0');

  // 3. Calcul de la taille (comme dans le SuperAdmin)
  const kb = (new Blob([JSON.stringify(orderedChannels)]).size / 1024).toFixed(1);

  log('sys', `📦 Publication synchronisée v${newVersion} (${kb} Ko)...`);

  // --- ACTION A : CRÉER UNE NOUVELLE LIGNE DANS L'HISTORIQUE ---
  // Cela permet à l'app de détecter la mise à jour via "published_at" DESC
  const payloadData = {
    version: newVersion,
    count: orderedChannels.length,
    data: orderedChannels,
    note: `Scan Auto Turbo : ${okChannels.length} OK / ${failChannels.length} Mortes`,
    size_kb: parseFloat(kb),
    published_at: now.toISOString()
  };

  const res1 = await sbPost('channels_data', payloadData);
  if (res1.ok) {
    log('ok', `✅ Nouvelle entrée ajoutée à channels_data`);
  }

  // --- ACTION B : METTRE À JOUR LA LIGNE FIXE DES PRIORITÉS ---
  // L'application utilise cette table pour savoir quelles URLs charger en priorité
  const res2 = await sbPatch('channel_priorities', 'id=eq.1', {
    priorities: prioURLs,
    count: prioURLs.length,
    saved_at: now.toISOString()
  });

  if (res2.ok) {
    log('ok', `✅ Table channel_priorities (id=1) mise à jour`);
  }

  return newVersion;
}
