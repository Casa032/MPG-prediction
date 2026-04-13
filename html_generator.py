"""
html_generator.py
=================
Génère un fichier dashboard.html autonome avec toutes les données intégrées.
Aucun serveur requis — double-clic pour ouvrir dans le navigateur.

Pipeline complet :
    excel_parser.py → storage.py → html_generator.py → dashboard.html

Usage :
    python reporting/html_generator.py
    python reporting/html_generator.py --quinzaine Q1_2025_S2
    python reporting/html_generator.py --output mon_rapport.html
"""

import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from storage.storage import StorageManager

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)


# ── Préparation des données ────────────────────────────────────────────────────

def preparer_donnees(sm: StorageManager, quinzaine: str | None = None) -> dict:
    """
    Charge toutes les données nécessaires au dashboard depuis le Parquet.
    Retourne un dict JSON-sérialisable prêt à être injecté dans le HTML.
    """
    quinzaines = sm.lister_quinzaines()
    if not quinzaines:
        log.error("Aucune donnée en base — lance d'abord excel_parser.py")
        return {}

    q_active = quinzaine or quinzaines[-1]
    log.info(f"Quinzaine active : {q_active}")

    kpis    = sm.kpis(quinzaine=q_active)
    projets = sm.charger_quinzaines(quinzaines=[q_active])
    meta    = sm.charger_meta()

    # Delta avec la quinzaine précédente si disponible
    delta = []
    idx = quinzaines.index(q_active)
    if idx > 0:
        q_prev = quinzaines[idx - 1]
        df_delta = sm.delta_quinzaines(q_prev, q_active)
        if not df_delta.empty:
            delta = df_delta.where(df_delta.notna(), None).to_dict(orient="records")

    # Historiques par projet (pour la page détail)
    historiques = {}
    for pid in projets["projet_id"].unique() if not projets.empty else []:
        h = sm.projet(pid)
        if not h.empty:
            historiques[pid] = h.where(h.notna(), None).to_dict(orient="records")

    return {
        "genere_le":     datetime.now().strftime("%d/%m/%Y à %H:%M"),
        "quinzaines":    quinzaines,
        "quinzaine":     q_active,
        "kpis":          kpis,
        "projets":       projets.where(projets.notna(), None).to_dict(orient="records") if not projets.empty else [],
        "meta":          meta.where(meta.notna(), None).to_dict(orient="records") if not meta.empty else [],
        "delta":         delta,
        "historiques":   historiques,
    }


# ── Génération HTML ────────────────────────────────────────────────────────────

def generer_html(donnees: dict, llm_reponses: dict | None = None) -> str:
    """
    Injecte les données dans le template HTML et retourne le fichier complet.
    Les données sont sérialisées en JSON dans un bloc <script> — zéro requête réseau.
    """
    data_json  = json.dumps(donnees,  ensure_ascii=False, indent=2)
    llm_json   = json.dumps(llm_reponses or {}, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Project Intelligence — {donnees.get('quinzaine','')}</title>
<style>
/* ── Fonts système — aucune dépendance externe ── */
:root {{
  --bg:        #0d0f14;
  --surface:   #161920;
  --surface-2: #1e2230;
  --border:    #2a2f40;
  --accent:    #1abc9c;
  --accent-dim:rgba(26,188,156,.12);
  --text:      #e8eaf0;
  --muted:     #6b7280;
  --faint:     #3d4254;
  --on-track:  #1abc9c;
  --at-risk:   #f59e0b;
  --late:      #ef4444;
  --done:      #6366f1;
  --on-hold:   #6b7280;
  --font-head: 'Segoe UI','Helvetica Neue',Arial,sans-serif;
  --font-mono: 'Consolas','Cascadia Code','Courier New',monospace;
}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
html{{font-size:14px;}}
body{{font-family:var(--font-mono);background:var(--bg);color:var(--text);min-height:100vh;display:flex;}}

/* Sidebar */
.sidebar{{width:210px;min-height:100vh;background:var(--surface);border-right:1px solid var(--border);display:flex;flex-direction:column;flex-shrink:0;}}
.logo{{padding:22px 18px 18px;border-bottom:1px solid var(--border);}}
.logo-mark{{font-family:var(--font-head);font-weight:700;font-size:1rem;color:var(--accent);}}
.logo-sub{{font-size:.65rem;color:var(--muted);margin-top:3px;letter-spacing:.08em;text-transform:uppercase;}}
.logo-date{{font-size:.6rem;color:var(--faint);margin-top:4px;}}
.sec{{font-size:.6rem;color:var(--faint);letter-spacing:.12em;text-transform:uppercase;padding:18px 18px 5px;}}
.nav{{display:flex;align-items:center;gap:9px;padding:9px 18px;font-size:.78rem;color:var(--muted);cursor:pointer;border-left:2px solid transparent;transition:all .12s;user-select:none;}}
.nav:hover{{color:var(--text);background:var(--surface-2);}}
.nav.active{{color:var(--accent);border-left-color:var(--accent);background:var(--accent-dim);}}
.nav svg{{width:13px;height:13px;opacity:.7;flex-shrink:0;}}
.nav.active svg{{opacity:1;}}
.sfooter{{margin-top:auto;padding:14px 18px;border-top:1px solid var(--border);}}
.qlabel{{font-size:.6rem;color:var(--faint);text-transform:uppercase;letter-spacing:.1em;margin-bottom:5px;}}
.qsel{{width:100%;background:var(--surface-2);border:1px solid var(--border);color:var(--text);font-family:var(--font-mono);font-size:.75rem;padding:6px 8px;border-radius:6px;cursor:pointer;outline:none;}}
.qsel:focus{{border-color:var(--accent);}}

/* Main */
.main{{flex:1;display:flex;flex-direction:column;min-width:0;}}
.topbar{{height:50px;border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 22px;gap:12px;flex-shrink:0;}}
.tb-title{{font-family:var(--font-head);font-weight:700;font-size:.95rem;}}
.spacer{{flex:1;}}
.badge-ok{{font-size:.62rem;padding:2px 9px;border-radius:20px;color:var(--on-track);border:1px solid rgba(26,188,156,.3);background:rgba(26,188,156,.06);}}
.btn{{background:var(--surface-2);border:1px solid var(--border);color:var(--muted);font-family:var(--font-mono);font-size:.7rem;padding:4px 11px;border-radius:5px;cursor:pointer;transition:all .12s;}}
.btn:hover{{border-color:var(--accent);color:var(--accent);}}
.btn-accent{{background:var(--accent);border:none;color:#fff;padding:8px 16px;border-radius:6px;font-family:var(--font-mono);font-size:.75rem;cursor:pointer;width:100%;margin-top:4px;transition:opacity .12s;}}
.btn-accent:hover{{opacity:.85;}}

/* Pages */
.page{{padding:22px;display:none;overflow-y:auto;}}
.page.active{{display:block;}}

/* KPIs */
.kgrid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:22px;}}
.kcard{{background:var(--surface);border:1px solid var(--border);border-radius:9px;padding:16px;position:relative;overflow:hidden;transition:border-color .15s;}}
.kcard:hover{{border-color:var(--accent);}}
.kcard::before{{content:'';position:absolute;top:0;left:0;width:3px;height:100%;background:var(--kc,var(--accent));}}
.klabel{{font-size:.6rem;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:8px;}}
.kval{{font-family:var(--font-head);font-weight:700;font-size:2rem;color:var(--kc,var(--text));line-height:1;}}
.ksub{{font-size:.62rem;color:var(--muted);margin-top:5px;}}

/* Grille 2 colonnes */
.g2{{display:grid;grid-template-columns:1fr 340px;gap:12px;margin-bottom:12px;}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:9px;overflow:hidden;}}
.card-h{{padding:11px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;}}
.card-t{{font-size:.6rem;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;}}
.card-b{{padding:16px;}}

/* Gantt */
.glist{{display:flex;flex-direction:column;gap:11px;}}
.grow{{display:flex;align-items:center;gap:10px;}}
.gname{{font-size:.76rem;color:var(--text);width:140px;flex-shrink:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.gtrack{{flex:1;height:5px;background:var(--surface-2);border-radius:3px;overflow:hidden;}}
.gfill{{height:100%;border-radius:3px;transition:width .5s cubic-bezier(.4,0,.2,1);}}
.gpct{{font-size:.65rem;color:var(--muted);width:30px;text-align:right;flex-shrink:0;}}

/* Liste projets */
.plist{{display:flex;flex-direction:column;}}
.prow{{display:flex;align-items:center;gap:8px;padding:9px 16px;border-bottom:1px solid var(--border);cursor:pointer;transition:background .1s;}}
.prow:last-child{{border-bottom:none;}}
.prow:hover{{background:var(--surface-2);}}
.pdot{{width:7px;height:7px;border-radius:50%;flex-shrink:0;}}
.pname{{font-size:.78rem;color:var(--text);flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.presp{{font-size:.65rem;color:var(--muted);width:55px;flex-shrink:0;}}
.ppct{{font-size:.65rem;color:var(--muted);width:28px;text-align:right;flex-shrink:0;}}

/* Badge statut */
.badge{{display:inline-block;font-size:.58rem;padding:2px 6px;border-radius:4px;border:1px solid;letter-spacing:.05em;flex-shrink:0;}}
.ON_TRACK{{color:var(--on-track);border-color:rgba(26,188,156,.3);background:rgba(26,188,156,.08);}}
.AT_RISK{{color:var(--at-risk);border-color:rgba(245,158,11,.3);background:rgba(245,158,11,.08);}}
.LATE{{color:var(--late);border-color:rgba(239,68,68,.3);background:rgba(239,68,68,.08);}}
.DONE{{color:var(--done);border-color:rgba(99,102,241,.3);background:rgba(99,102,241,.08);}}
.ON_HOLD{{color:var(--on-hold);border-color:rgba(107,114,128,.3);background:rgba(107,114,128,.08);}}

/* Delta */
.delta-table{{width:100%;border-collapse:collapse;font-size:.78rem;}}
.delta-table th{{font-size:.6rem;color:var(--faint);text-transform:uppercase;letter-spacing:.08em;padding:7px 8px;text-align:left;border-bottom:1px solid var(--border);}}
.delta-table td{{padding:8px 8px;border-bottom:1px solid var(--border);color:var(--muted);}}
.delta-table tr:last-child td{{border-bottom:none;}}
.delta-pos{{color:var(--on-track);font-weight:500;}}
.delta-neg{{color:var(--late);font-weight:500;}}
.delta-neu{{color:var(--muted);}}

/* Détail projet */
.detail-header{{margin-bottom:18px;}}
.detail-name{{font-family:var(--font-head);font-weight:700;font-size:1.1rem;color:var(--accent);}}
.detail-meta{{font-size:.72rem;color:var(--muted);margin-top:4px;}}
.hist-table{{width:100%;border-collapse:collapse;font-size:.78rem;}}
.hist-table th{{font-size:.6rem;color:var(--faint);text-transform:uppercase;letter-spacing:.08em;padding:7px 8px;text-align:left;border-bottom:1px solid var(--border);}}
.hist-table td{{padding:8px 8px;border-bottom:1px solid var(--border);color:var(--muted);vertical-align:top;}}

/* Chat */
.chat-wrap{{max-width:780px;margin:0 auto;display:flex;flex-direction:column;height:calc(100vh - 50px - 44px);}}
.chat-msgs{{flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:14px;padding:16px 0;}}
.msg{{display:flex;gap:10px;align-items:flex-start;}}
.msg.user{{flex-direction:row-reverse;}}
.avatar{{width:28px;height:28px;border-radius:7px;background:var(--surface-2);border:1px solid var(--border);display:flex;align-items:center;justify-content:center;font-size:.58rem;color:var(--accent);flex-shrink:0;}}
.msg.user .avatar{{background:var(--accent-dim);}}
.bubble{{max-width:74%;background:var(--surface);border:1px solid var(--border);border-radius:9px;padding:10px 14px;font-size:.78rem;line-height:1.6;color:var(--text);white-space:pre-wrap;}}
.msg.user .bubble{{background:var(--accent-dim);border-color:rgba(26,188,156,.2);}}
.chat-input-row{{padding:12px 0;border-top:1px solid var(--border);display:flex;gap:8px;}}
.chat-input{{flex:1;background:var(--surface);border:1px solid var(--border);border-radius:7px;padding:9px 13px;color:var(--text);font-family:var(--font-mono);font-size:.78rem;outline:none;resize:none;transition:border-color .12s;}}
.chat-input:focus{{border-color:var(--accent);}}
.chat-send{{background:var(--accent);border:none;border-radius:7px;padding:9px 16px;color:#fff;font-family:var(--font-mono);font-size:.75rem;cursor:pointer;transition:opacity .12s;}}
.chat-send:hover{{opacity:.85;}}
.chat-send:disabled{{opacity:.4;cursor:default;}}

/* Rapports */
.rpt-grid{{display:grid;grid-template-columns:260px 1fr;gap:16px;}}
.form-g{{display:flex;flex-direction:column;gap:5px;margin-bottom:12px;}}
.form-l{{font-size:.6rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;}}
.form-s{{background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:7px 10px;color:var(--text);font-family:var(--font-mono);font-size:.76rem;outline:none;width:100%;}}
.form-s:focus{{border-color:var(--accent);}}
.rpt-preview{{background:var(--surface);border:1px solid var(--border);border-radius:9px;padding:24px;min-height:380px;font-size:.78rem;line-height:1.7;color:var(--text);overflow-y:auto;}}
.rpt-empty{{display:flex;align-items:center;justify-content:center;height:300px;color:var(--faint);font-size:.78rem;}}

/* Loading */
.spin{{display:inline-block;width:12px;height:12px;border:2px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;margin-right:5px;}}
@keyframes spin{{to{{transform:rotate(360deg);}}}}

@media(max-width:1100px){{.g2{{grid-template-columns:1fr;}}.kgrid{{grid-template-columns:repeat(2,1fr);}}}}
</style>
</head>
<body>

<!-- Données injectées par Python — aucune requête réseau -->
<script>
const DATA = {data_json};
const LLM_CACHE = {llm_json};
</script>

<nav class="sidebar">
  <div class="logo">
    <div class="logo-mark">Project Intelligence</div>
    <div class="logo-sub">Équipe DATA</div>
    <div class="logo-date">Généré le {{DATA.genere_le}}</div>
  </div>
  <div class="sec">Navigation</div>
  <div class="nav active" onclick="goPage('dashboard',this)">
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="1" y="1" width="6" height="6" rx="1"/><rect x="9" y="1" width="6" height="6" rx="1"/><rect x="1" y="9" width="6" height="6" rx="1"/><rect x="9" y="9" width="6" height="6" rx="1"/></svg>
    Dashboard
  </div>
  <div class="nav" onclick="goPage('projets',this)">
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 4h12M2 8h8M2 12h5"/></svg>
    Projets
  </div>
  <div class="nav" onclick="goPage('delta',this)">
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 8h12M9 4l4 4-4 4"/></svg>
    Évolution
  </div>
  <div class="nav" onclick="goPage('chat',this)">
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 3h12v8H9l-3 3V11H2z"/></svg>
    Chat LLM
  </div>
  <div class="nav" onclick="goPage('rapports',this)">
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M3 2h7l3 3v9H3z"/><path d="M10 2v4h4"/></svg>
    Rapports
  </div>
  <div class="sfooter">
    <div class="qlabel">Quinzaine</div>
    <select class="qsel" id="qsel" onchange="changerQuinzaine(this.value)"></select>
  </div>
</nav>

<div class="main">
  <div class="topbar">
    <span class="tb-title" id="page-title">Dashboard</span>
    <div class="spacer"></div>
    <span class="badge-ok" id="q-badge"></span>
    <button class="btn" onclick="window.print()">Imprimer</button>
  </div>

  <!-- DASHBOARD -->
  <div class="page active" id="page-dashboard">
    <div class="kgrid" id="kpis"></div>
    <div class="g2">
      <div class="card">
        <div class="card-h"><span class="card-t">Avancement par projet</span></div>
        <div class="card-b"><div class="glist" id="gantt"></div></div>
      </div>
      <div class="card">
        <div class="card-h"><span class="card-t">Projets</span></div>
        <div class="plist" id="plist"></div>
      </div>
    </div>
  </div>

  <!-- PROJETS DÉTAIL -->
  <div class="page" id="page-projets">
    <div class="card">
      <div class="card-h">
        <span class="card-t" id="proj-card-titre">Sélectionne un projet dans le dashboard</span>
        <button class="btn" onclick="goPage('dashboard',document.querySelector('.nav'))">← Retour</button>
      </div>
      <div style="padding:20px" id="proj-detail">
        <p style="color:var(--muted)">Clique sur un projet dans le dashboard pour voir son historique.</p>
      </div>
    </div>
  </div>

  <!-- ÉVOLUTION (DELTA) -->
  <div class="page" id="page-delta">
    <div class="card">
      <div class="card-h"><span class="card-t" id="delta-titre">Évolution quinzaine</span></div>
      <div style="padding:16px" id="delta-content"></div>
    </div>
  </div>

  <!-- CHAT LLM -->
  <div class="page" id="page-chat">
    <div class="chat-wrap">
      <div class="chat-msgs" id="chat-msgs">
        <div class="msg">
          <div class="avatar">AI</div>
          <div class="bubble">Bonjour ! Je réponds à tes questions sur les projets de la quinzaine <strong id="chat-q-label"></strong>.

Exemples :
— Quels projets sont en retard ?
— Quelles décisions ont été prises ?
— Résume l'avancement global
— Y a-t-il des blocages ?

Note : les réponses sont générées par Python au moment de la création de ce fichier.
Pour de nouvelles questions, relance html_generator.py.</div>
        </div>
      </div>
      <div class="chat-input-row">
        <textarea class="chat-input" id="chat-input" rows="2"
          placeholder="Question sur les projets..."
          onkeydown="if(event.key==='Enter'&&!event.shiftKey){{event.preventDefault();sendChat();}}"></textarea>
        <button class="chat-send" onclick="sendChat()">Envoyer</button>
      </div>
    </div>
  </div>

  <!-- RAPPORTS -->
  <div class="page" id="page-rapports">
    <div class="rpt-grid">
      <div>
        <div class="form-g">
          <label class="form-l">Type</label>
          <select class="form-s" id="rpt-type" onchange="document.getElementById('rpt-groupe-proj').style.display=this.value==='projet'?'block':'none'">
            <option value="quinzaine">Quinzaine complète</option>
            <option value="projet">Historique projet</option>
            <option value="delta">Comparaison quinzaines</option>
          </select>
        </div>
        <div class="form-g" id="rpt-groupe-proj" style="display:none">
          <label class="form-l">Projet</label>
          <select class="form-s" id="rpt-projet"></select>
        </div>
        <button class="btn-accent" onclick="genererRapport()">Générer</button>
        <button class="btn-accent" style="background:var(--surface-2);color:var(--text);border:1px solid var(--border);margin-top:6px"
                onclick="window.print()">Imprimer / PDF</button>
        <p style="font-size:.65rem;color:var(--faint);margin-top:8px;line-height:1.5">
          Astuce : utilise Ctrl+P → "Enregistrer en PDF" pour exporter sans serveur.
        </p>
      </div>
      <div class="rpt-preview" id="rpt-preview">
        <div class="rpt-empty">Sélectionne les paramètres et clique sur Générer</div>
      </div>
    </div>
  </div>
</div>

<script>
/* ── Constantes statuts ── */
const COULEURS = {{
  ON_TRACK:'#1abc9c', AT_RISK:'#f59e0b',
  LATE:'#ef4444', DONE:'#6366f1', ON_HOLD:'#6b7280'
}};

/* ── Initialisation ── */
let qActive = DATA.quinzaine;

function init() {{
  // Remplir le select des quinzaines
  const sel = document.getElementById('qsel');
  DATA.quinzaines.forEach(q => {{
    const o = document.createElement('option');
    o.value = q; o.textContent = q;
    if (q === qActive) o.selected = true;
    sel.appendChild(o);
  }});

  document.getElementById('q-badge').textContent = qActive;
  document.getElementById('chat-q-label').textContent = qActive;
  document.querySelector('.logo-date').textContent = 'Généré le ' + DATA.genere_le;

  // Remplir select projets dans rapports
  const rptProj = document.getElementById('rpt-projet');
  DATA.projets.forEach(p => {{
    const o = document.createElement('option');
    o.value = p.projet_id; o.textContent = p.projet_nom;
    rptProj.appendChild(o);
  }});

  renderKpis();
  renderGantt();
  renderPlist();
  renderDelta();
}}

/* ── Navigation ── */
const TITRES = {{
  dashboard:'Dashboard', projets:'Détail projet',
  delta:'Évolution', chat:'Chat LLM', rapports:'Rapports'
}};

function goPage(page, el) {{
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav').forEach(n => n.classList.remove('active'));
  document.getElementById('page-' + page).classList.add('active');
  if (el) el.classList.add('active');
  document.getElementById('page-title').textContent = TITRES[page] || page;
}}

/* ── Changement de quinzaine ── */
function changerQuinzaine(q) {{
  // Le fichier est statique — on ne peut changer que vers la quinzaine intégrée
  if (q !== DATA.quinzaine) {{
    alert('Ce fichier contient uniquement la quinzaine ' + DATA.quinzaine +
          '.\\nRelance html_generator.py --quinzaine ' + q + ' pour générer ce rapport.');
    document.getElementById('qsel').value = DATA.quinzaine;
  }}
}}

/* ── KPIs ── */
function renderKpis() {{
  const k = DATA.kpis;
  const items = [
    {{ label:'Projets actifs',   val:k.nb_projets_actifs, sub:'cette quinzaine',    c:'var(--on-track)' }},
    {{ label:'En retard',        val:k.nb_en_retard,      sub:'projets LATE',        c:'var(--late)' }},
    {{ label:'À risque',         val:k.nb_at_risk,        sub:'projets AT_RISK',     c:'var(--at-risk)' }},
    {{ label:'Avancement moyen', val:k.avancement_moyen+'%', sub:'tous projets actifs', c:'var(--accent)' }},
  ];
  document.getElementById('kpis').innerHTML = items.map(i => `
    <div class="kcard" style="--kc:${{i.c}}">
      <div class="klabel">${{i.label}}</div>
      <div class="kval">${{i.val ?? '—'}}</div>
      <div class="ksub">${{i.sub}}</div>
    </div>`).join('');
}}

/* ── Gantt ── */
function renderGantt() {{
  document.getElementById('gantt').innerHTML = DATA.projets.map(p => `
    <div class="grow">
      <div class="gname" title="${{p.projet_nom}}">${{p.projet_nom}}</div>
      <div class="gtrack">
        <div class="gfill" style="width:${{p.avancement_pct||0}}%;background:${{COULEURS[p.statut]||'#6b7280'}}"></div>
      </div>
      <div class="gpct">${{p.avancement_pct||0}}%</div>
    </div>`).join('');
}}

/* ── Liste projets ── */
function renderPlist() {{
  document.getElementById('plist').innerHTML = DATA.projets.map(p => `
    <div class="prow" onclick="voirProjet('${{p.projet_id}}')">
      <div class="pdot" style="background:${{COULEURS[p.statut]||'#6b7280'}}"></div>
      <div class="pname">${{p.projet_nom}}</div>
      <div class="presp">${{p.responsable_principal||''}}</div>
      <span class="badge ${{p.statut}}">${{p.statut}}</span>
      <div class="ppct">${{p.avancement_pct||0}}%</div>
    </div>`).join('');
}}

/* ── Détail projet ── */
function voirProjet(pid) {{
  const p = DATA.projets.find(x => x.projet_id === pid);
  const hist = DATA.historiques[pid] || [];
  if (!p) return;

  document.getElementById('proj-card-titre').textContent = p.projet_nom + ' — ' + pid;
  document.getElementById('proj-detail').innerHTML = `
    <div class="detail-header">
      <div class="detail-name">${{p.projet_nom}}</div>
      <div class="detail-meta">
        ${{pid}} &nbsp;·&nbsp; Responsable : ${{p.responsable_principal||'—'}}
        &nbsp;·&nbsp; Domaine : ${{p.domaine||'—'}}
        &nbsp;·&nbsp; Phase : ${{p.phase||'—'}}
      </div>
    </div>
    <table class="hist-table">
      <thead><tr>
        <th>Quinzaine</th><th>Statut</th><th>Avanc.</th>
        <th>Livrable</th><th>Décisions</th><th>Blocages</th>
      </tr></thead>
      <tbody>${{hist.map(h => `
        <tr>
          <td>${{h.quinzaine||''}}</td>
          <td><span class="badge ${{h.statut}}">${{h.statut||''}}</span></td>
          <td style="text-align:right">${{h.avancement_pct||0}}%</td>
          <td>${{h.livrable_quinzaine||'—'}}</td>
          <td style="font-size:.72rem">${{h.decisions||'—'}}</td>
          <td style="font-size:.72rem;color:var(--late)">${{h.points_blocage||''}}</td>
        </tr>`).join('')}}</tbody>
    </table>`;

  goPage('projets', document.querySelectorAll('.nav')[1]);
}}

/* ── Delta ── */
function renderDelta() {{
  const delta = DATA.delta;
  if (!delta || !delta.length) {{
    document.getElementById('delta-content').innerHTML =
      '<p style="color:var(--muted);padding:12px">Pas de quinzaine précédente à comparer.</p>';
    return;
  }}
  const q_idx = DATA.quinzaines.indexOf(DATA.quinzaine);
  const q_prev = q_idx > 0 ? DATA.quinzaines[q_idx-1] : '?';
  document.getElementById('delta-titre').textContent =
    `Évolution : ${{q_prev}} → ${{DATA.quinzaine}}`;

  document.getElementById('delta-content').innerHTML = `
    <table class="delta-table">
      <thead><tr>
        <th>Projet</th><th>Statut avant</th><th>Statut après</th>
        <th style="text-align:right">Avant</th>
        <th style="text-align:right">Après</th>
        <th style="text-align:right">Delta</th>
      </tr></thead>
      <tbody>${{delta.map(d => {{
        const delta_v = d.delta_avancement || 0;
        const cl = delta_v > 0 ? 'delta-pos' : delta_v < 0 ? 'delta-neg' : 'delta-neu';
        const sign = delta_v > 0 ? '+' : '';
        return `<tr>
          <td style="color:var(--text);font-weight:500">${{d.projet_nom||''}}</td>
          <td><span class="badge ${{d.statut_avant}}">${{d.statut_avant||'—'}}</span></td>
          <td><span class="badge ${{d.statut_apres}}">${{d.statut_apres||'—'}}</span></td>
          <td style="text-align:right">${{Math.round(d.avancement_avant||0)}}%</td>
          <td style="text-align:right">${{Math.round(d.avancement_apres||0)}}%</td>
          <td style="text-align:right" class="${{cl}}">${{sign}}${{Math.round(delta_v)}}%</td>
        </tr>`;
      }}).join('')}}</tbody>
    </table>`;
}}

/* ── Chat LLM (basé sur le cache Python) ── */
function sendChat() {{
  const input = document.getElementById('chat-input');
  const q = input.value.trim();
  if (!q) return;

  const msgs = document.getElementById('chat-msgs');
  msgs.innerHTML += `
    <div class="msg user">
      <div class="avatar">Toi</div>
      <div class="bubble">${{q}}</div>
    </div>`;
  input.value = '';
  msgs.scrollTop = msgs.scrollHeight;

  // Chercher une réponse dans le cache LLM généré par Python
  const cle = q.toLowerCase().trim();
  let reponse = LLM_CACHE[cle];

  if (!reponse) {{
    // Réponses de base depuis les données intégrées
    reponse = repondreLocal(cle);
  }}

  setTimeout(() => {{
    msgs.innerHTML += `
      <div class="msg">
        <div class="avatar">AI</div>
        <div class="bubble">${{reponse}}</div>
      </div>`;
    msgs.scrollTop = msgs.scrollHeight;
  }}, 300);
}}

function repondreLocal(question) {{
  const projets = DATA.projets;
  if (question.includes('retard') || question.includes('late')) {{
    const late = projets.filter(p => p.statut === 'LATE');
    return late.length
      ? late.length + ' projet(s) en retard :\\n' + late.map(p => '- ' + p.projet_nom + ' (' + (p.responsable_principal||'?') + ')').join('\\n')
      : 'Aucun projet en retard sur ' + DATA.quinzaine + '.';
  }}
  if (question.includes('risque') || question.includes('at_risk')) {{
    const risk = projets.filter(p => p.statut === 'AT_RISK');
    return risk.length
      ? risk.length + ' projet(s) à risque :\\n' + risk.map(p => '- ' + p.projet_nom + ' : ' + (p.risques||'non précisé')).join('\\n')
      : 'Aucun projet à risque sur ' + DATA.quinzaine + '.';
  }}
  if (question.includes('décision') || question.includes('decision')) {{
    const dec = projets.filter(p => p.decisions && p.decisions.trim());
    return dec.length
      ? 'Décisions sur ' + DATA.quinzaine + ' :\\n' + dec.map(p => '- ' + p.projet_nom + ' : ' + p.decisions).join('\\n')
      : 'Aucune décision enregistrée.';
  }}
  if (question.includes('blocage') || question.includes('bloqué')) {{
    const bloc = projets.filter(p => p.points_blocage && p.points_blocage.trim());
    return bloc.length
      ? 'Blocages actifs :\\n' + bloc.map(p => '- ' + p.projet_nom + ' : ' + p.points_blocage).join('\\n')
      : 'Aucun blocage signalé.';
  }}
  if (question.includes('avancement') || question.includes('résumé') || question.includes('resume')) {{
    const k = DATA.kpis;
    return 'Résumé ' + DATA.quinzaine + ' :\\n' +
      '- ' + k.nb_projets_actifs + ' projets actifs\\n' +
      '- ' + k.nb_en_retard + ' en retard · ' + k.nb_at_risk + ' à risque\\n' +
      '- Avancement moyen : ' + k.avancement_moyen + '%\\n' +
      '- ' + k.nb_decisions + ' décision(s) enregistrée(s)';
  }}
  return 'Je peux répondre aux questions sur les retards, risques, décisions, blocages et l\'avancement.\\nRelance html_generator.py avec --llm pour des réponses enrichies par le LLM.';
}}

/* ── Rapports ── */
function genererRapport() {{
  const type = document.getElementById('rpt-type').value;
  const preview = document.getElementById('rpt-preview');

  if (type === 'quinzaine') {{
    const k = DATA.kpis;
    const rows = DATA.projets.map(p => `
      <tr>
        <td style="font-weight:500;color:var(--text)">${{p.projet_id}}</td>
        <td>${{p.projet_nom}}</td>
        <td>${{p.responsable_principal||'—'}}</td>
        <td><span class="badge ${{p.statut}}">${{p.statut}}</span></td>
        <td style="text-align:right">${{p.avancement_pct||0}}%</td>
        <td style="font-size:.72rem">${{p.decisions||'—'}}</td>
      </tr>`).join('');
    preview.innerHTML = `
      <h2 style="font-family:var(--font-head);color:var(--accent);margin-bottom:14px">
        Rapport Quinzaine — ${{DATA.quinzaine}}
      </h2>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:18px">
        ${{[
          ['Actifs', k.nb_projets_actifs, 'var(--on-track)'],
          ['En retard', k.nb_en_retard, 'var(--late)'],
          ['À risque', k.nb_at_risk, 'var(--at-risk)'],
          ['Avancement', k.avancement_moyen+'%', 'var(--accent)'],
        ].map(([l,v,c]) => `
          <div style="background:var(--surface-2);border-radius:7px;padding:10px;border-left:3px solid ${{c}}">
            <div style="font-size:.6rem;color:var(--muted);text-transform:uppercase;letter-spacing:.1em">${{l}}</div>
            <div style="font-size:1.5rem;font-weight:700;color:${{c}}">${{v}}</div>
          </div>`).join('')}}
      </div>
      <table style="width:100%;border-collapse:collapse;font-size:.76rem;color:var(--muted)">
        <thead><tr style="border-bottom:1px solid var(--border);font-size:.6rem;text-transform:uppercase;letter-spacing:.08em;color:var(--faint)">
          <th style="text-align:left;padding:7px 6px">ID</th>
          <th style="text-align:left;padding:7px 6px">Projet</th>
          <th style="text-align:left;padding:7px 6px">Responsable</th>
          <th style="text-align:left;padding:7px 6px">Statut</th>
          <th style="text-align:right;padding:7px 6px">Avanc.</th>
          <th style="text-align:left;padding:7px 6px">Décisions</th>
        </tr></thead>
        <tbody>${{rows}}</tbody>
      </table>`;

  }} else if (type === 'projet') {{
    const pid = document.getElementById('rpt-projet').value;
    const hist = DATA.historiques[pid] || [];
    const p = DATA.projets.find(x => x.projet_id === pid);
    preview.innerHTML = `
      <h2 style="font-family:var(--font-head);color:var(--accent);margin-bottom:4px">${{p?.projet_nom||pid}}</h2>
      <p style="color:var(--muted);font-size:.72rem;margin-bottom:16px">${{pid}} — ${{hist.length}} quinzaine(s)</p>
      <table class="hist-table">
        <thead><tr><th>Quinzaine</th><th>Statut</th><th>Avanc.</th><th>Livrable</th><th>Décisions</th></tr></thead>
        <tbody>${{hist.map(h => `
          <tr>
            <td>${{h.quinzaine}}</td>
            <td><span class="badge ${{h.statut}}">${{h.statut}}</span></td>
            <td style="text-align:right">${{h.avancement_pct||0}}%</td>
            <td>${{h.livrable_quinzaine||'—'}}</td>
            <td style="font-size:.72rem">${{h.decisions||'—'}}</td>
          </tr>`).join('')}}</tbody>
      </table>`;

  }} else {{
    preview.innerHTML = document.getElementById('delta-content').innerHTML ||
      '<p style="color:var(--muted)">Génère d\'abord la page Évolution.</p>';
  }}
}}

init();
</script>
</body>
</html>"""


# ── Point d'entrée ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Génère le dashboard HTML standalone.")
    parser.add_argument("--quinzaine", default=None, help="Quinzaine à afficher (défaut: dernière)")
    parser.add_argument("--output", default="frontend/dashboard.html", help="Fichier de sortie")
    parser.add_argument("--config", default="config.yaml", help="Chemin config.yaml")
    args = parser.parse_args()

    sm = StorageManager(args.config)
    donnees = preparer_donnees(sm, args.quinzaine)

    if not donnees:
        log.error("Impossible de générer le dashboard — aucune donnée disponible.")
        return

    chemin = Path(args.output)
    chemin.parent.mkdir(parents=True, exist_ok=True)
    chemin.write_text(generer_html(donnees), encoding="utf-8")

    log.info(f"Dashboard généré : {chemin}")
    log.info(f"Quinzaine : {donnees['quinzaine']} — {len(donnees['projets'])} projets")
    print(f"\nOuvre ce fichier dans ton navigateur :\n  {chemin.resolve()}\n")


if __name__ == "__main__":
    main()
