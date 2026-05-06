"""
html_generator.py
=========================
Génère le dashboard html

Pages : Vue d'ensemble · Par domaine · Collaborateurs · Roadmap Gantt · Évolutions · Chat

Usage :
    python html_generator.py
    python html_generator.py --quinzaine T1_2026_R1
    python html_generator.py --llm
    python html_generator.py --config config.yaml --output frontend/dashboard.html
"""

import sys
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
import re as _re

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

try:
    from storage.storage import StorageManager
except ImportError:
    from storage import StorageManager  # type: ignore

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)


# ── Donnees ───────────────────────────────────────────────────────────────────

def _calculer_snapshot(sm, q, quinzaines):
    df   = sm.charger_quinzaines(quinzaines=[q])
    kpis = sm.kpis(quinzaine=q)
    idx  = quinzaines.index(q)
    q_prev = quinzaines[idx - 1] if idx > 0 else None
    delta = []
    if q_prev:
        df_d = sm.delta_quinzaines(q_prev, q)
        if not df_d.empty:
            delta = df_d.where(df_d.notna(), None).to_dict(orient="records")
    projets = df.where(df.notna(), None).to_dict(orient="records") if not df.empty else []
    par_domaine, par_resp, par_entite = {}, {}, {}
    _CLE = {"En cours":"en_cours","À risque":"a_risque","En retard":"en_retard","Terminé":"terminé","Stand by":"stand_by"}
    for p in projets:
        d = p.get("domaine") or "Autre"
        par_domaine.setdefault(d, {"total":0,"en_cours":0,"a_risque":0,"en_retard":0,"terminé":0,"stand_by":0})
        par_domaine[d]["total"] += 1
        s_raw = (p.get("statut") or "").strip()
        cle = _CLE.get(s_raw)
        if cle and cle in par_domaine[d]:
            par_domaine[d][cle] += 1
        r = p.get("responsable_principal") or "Non assigné"
        par_resp.setdefault(r, {"total":0,"en_cours":0,"domaines":[]})
        par_resp[r]["total"] += 1
        if s_raw in ("En cours","À risque"): par_resp[r]["en_cours"] += 1
        dom = p.get("domaine") or ""
        if dom and dom not in par_resp[r]["domaines"]:
            par_resp[r]["domaines"].append(dom)
        
        entite_raw = p.get("entite_concerne") or ""
        entite_list = [e.strip() for e in _re.split(r"[;,]", str(entite_raw)) if e.strip()] if entite_raw else []
        for ent in (entite_list or ["Non assigné"]):
            par_entite.setdefault(ent, {"total":0,"en_cours":0,"a_risque":0,"en_retard":0,"terminé":0})
            par_entite[ent]["total"] += 1
            if cle and cle in par_entite[ent]:
                par_entite[ent][cle] += 1
    return {
        "projets": projets, "kpis": kpis, "par_domaine": par_domaine,
        "par_resp": par_resp, "par_entite": par_entite,
        "domaines": sorted({p.get("domaine") 
                            for p in projets
                            if isinstance(p.get("domaine"), str) and p.get("domaine")}),
        "q_prev": q_prev, "delta": delta,
    }


def preparer_donnees(sm, quinzaine=None):
    quinzaines = sm.lister_quinzaines()
    if not quinzaines:
        log.error("Aucune donnee — lance excel_parser.py d'abord")
        return {}
    quinzaines_triees = sorted(quinzaines)
    q_active = quinzaine or quinzaines_triees[-1]
    if q_active not in quinzaines_triees:
        q_active = quinzaines_triees[-1]
    meta = sm.charger_meta()
    df_all = sm.charger_quinzaines()
    historiques = {}
    if not df_all.empty:
        col_id = "projet_id" if "projet_id" in df_all.columns else "ref_sujet"
        for pid in df_all[col_id].unique():
            h = sm.projet(pid)
            if not h.empty:
                historiques[str(pid)] = h.where(h.notna(), None).to_dict(orient="records")
    snapshots = {}
    for q in quinzaines_triees:
        log.info(f"Preparation snapshot : {q}")
        snapshots[q] = _calculer_snapshot(sm, q, quinzaines_triees)
    snap = snapshots[q_active]
    meta_list = meta.where(meta.notna(), None).to_dict(orient="records") if not meta.empty else []
    agenda = sm.charger_agenda()
    agenda_list = agenda.where(agenda.notna(), None).to_dict(orient="records") if not agenda.empty else []


    archivage = sm.charger_archivage(mois_glissants=12)
    archivage_list = archivage.where(archivage.notna(), None).to_dict(orient="records") if not archivage.empty else []

  
    entites = sm.lister_entites()
    
    # Nouveaux projets : dans META type=PROJET, jamais apparus dans les historiques quinzaine
    TYPES_PROJET = {"PROJET"}
    ids_historiques = set(historiques.keys())
    nouveaux_projets = []
    if not meta.empty:
        col_id_meta = "projet_id" if "projet_id" in meta.columns else "ref_sujet"
        col_nom_meta = "projet_nom" if "projet_nom" in meta.columns else "sujet"
        for _, row in meta.iterrows():
            pid_val = str(row.get(col_id_meta, "") or "")
            type_val = str(row.get("type", "") or "").upper()
            if type_val in TYPES_PROJET and pid_val and pid_val not in ids_historiques:
                nouveaux_projets.append({
                    "projet_id": pid_val,
                    "projet_nom": str(row.get(col_nom_meta, "") or ""),
                    "domaine": str(row.get("domaine", "") or ""),
                    "responsable_principal": str(row.get("responsable_principal", "") or ""),
                    "priorite": str(row.get("priorite", "") or ""),
                    "entite_concerne": str(row.get("entite_concerne", "") or ""),
                    "type": type_val,
                })

    # KPIs META-based : compter par type depuis META
    kpis_meta = {"nb_projets": 0, "nb_gouvernance": 0, "nb_outil": 0,
                 "nb_formation": 0, "nb_autre_type": 0, "nb_nouveaux": len(nouveaux_projets)}
    TYPE_MAP = {"PROJET": "nb_projets", "GOUVERNANCE": "nb_gouvernance",
                "OUTIL": "nb_outil", "FORMATION": "nb_formation"}
    if not meta.empty and "type" in meta.columns:
        for _, row in meta.iterrows():
            t = str(row.get("type", "") or "").upper()
            key = TYPE_MAP.get(t, "nb_autre_type")
            kpis_meta[key] += 1

    return {
        "genere_le":      datetime.now().strftime("%d/%m/%Y à %H:%M"),
        "quinzaines":     quinzaines_triees,
        "quinzaine":      q_active,
        "q_prev":         snap["q_prev"],
        "kpis":           snap["kpis"],
        "projets":        snap["projets"],
        "domaines":       snap["domaines"],
        "par_domaine":    snap["par_domaine"],
        "par_resp":       snap["par_resp"],
        "delta":          snap["delta"],
        "entites":        entites,
        "meta":           meta_list,
        "historiques":    historiques,
        "snapshots":      snapshots,
        "agenda":         agenda_list,
        "archivage":      archivage_list,
        "par_entite":     snap["par_entite"],
        "nouveaux_projets": nouveaux_projets,
        "kpis_meta":      kpis_meta,
    }

# ----
#bg-> page centrale, bg2:sidebar ; bg3->encadré; bg4->
# ── CSS ───────────────────────────────────────────────────────────────────────

CSS = """
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=DM+Sans:wght@300;400;500;600;700&display=swap');
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
:root{
  /* ── Light mode (défaut) ── */
  --bg:#f5f4f0;
  --bg2:#ffffff;
  --bg3:#eeecea;
  --bg4:#e2e0db;
  --border:#d4d1ca;
  --border2:#b8b5ae;
 
  --text:#0f0e0c;      
  --text2:#2a2826;     
  --text3:#524f4a;  
 
  --cyan:#2563eb;
  --cyan2:#1d4ed8;
  --cyan-dim:rgba(37,99,235,.10);
 
  --violet:#7c3aed;
  --violet-dim:rgba(124,58,237,.10);
 
  --green:#059669;
  --green-dim:rgba(5,150,105,.10);
 
  --amber:#d97706;
  --amber-dim:rgba(217,119,6,.10);
 
  --red:#dc2626;
  --red-dim:rgba(220,38,38,.10);
 
  --font-body:'DM Sans',system-ui,-apple-system,sans-serif;
  --font-mono:'JetBrains Mono','Courier New',monospace;
  --radius:8px;
  --radius-lg:12px;
  --view_cadre-dim:rgba(0,0,0,.45);
}
 
/* ── Dark mode ── */
body.dark{
  --bg:#151310;
  --bg2:#1c1a18;
  --bg3:#252320;
  --bg4:#302e2b;
  --border:#343530;
  --border2:#4e5053;
 
  --text:#ebebeb;
  --text2:#c8c5c0;
  --text3:#8a8780;
 
  --cyan:#939591;
  --cyan2:#7a7875;
  --cyan-dim:rgba(147,149,145,.15);
 
  --violet:#a78bfa;
  --violet-dim:rgba(167,139,250,.12);
 
  --green:#10d994;
  --green-dim:rgba(16,217,148,.10);
 
  --amber:#f59e0b;
  --amber-dim:rgba(245,158,11,.10);
 
  --red:#f43f5e;
  --red-dim:rgba(244,63,94,.10);
 
  --view_cadre-dim:rgba(0,0,0,.88);
}
html,body{height:100%;font-size:13px;background:var(--bg);color:var(--text);}
body{font-family:var(--font-body);line-height:1.5;overflow:hidden;}
::-webkit-scrollbar{width:4px;height:4px;}
::-webkit-scrollbar-track{background:var(--bg2);}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:4px;}
.shell{display:flex;height:100vh;}
.sidebar{width:220px;min-width:220px;background:var(--bg2);border-right:1px solid #343530;
         display:flex;flex-direction:column;overflow-y:auto;flex-shrink:0;}
.main{flex:1;display:flex;flex-direction:column;overflow:hidden;}
.topbar{background:var(--bg2);border-bottom:1px solid #343530;padding:10px 20px;
        display:flex;align-items:center;gap:12px;flex-shrink:0;}
.content{flex:1;overflow-y:auto;padding:20px;background:var(--bg);}
.logo{padding:18px 16px 14px;border-bottom:1px solid #343530;}
.logo-title{font-family:var(--font-mono);font-size:14px;font-weight:600;color:var(--cyan);
            letter-spacing:.12em;text-transform:uppercase;}
.logo-sub{font-size:10px;color:var(--text3);margin-top:3px;font-family:var(--font-mono);}
.logo-date{font-size:9px;color:var(--text3);margin-top:6px;font-family:var(--font-mono);}
.q-selector-wrap{padding:12px 14px;border-bottom:1px solid #343530;}
.q-selector-label{font-size:9px;font-weight:600;color:var(--text3);letter-spacing:.1em;
                  text-transform:uppercase;margin-bottom:5px;font-family:var(--font-mono);}
.q-selector{width:100%;background:var(--bg3);color:var(--text);border:1px solid var(--border);
            border-radius:var(--radius);padding:6px 8px;font-size:11px;cursor:pointer;outline:none;
            font-family:var(--font-mono);}
.q-selector:focus{border-color:var(--cyan);}
.nav-section{padding:14px 16px 4px;font-size:9px;font-weight:600;text-transform:uppercase;
             letter-spacing:.1em;color:var(--text3);font-family:var(--font-mono);}
.nav-item{display:flex;align-items:center;gap:10px;padding:8px 14px;font-size:12px;
          cursor:pointer;transition:all .12s;color:var(--text2);border-left:2px solid transparent;user-select:none;}
.nav-item:hover{background:var(--bg3);color:var(--text);}
.nav-item.active{background:var(--cyan-dim);color:var(--cyan);border-left-color:var(--cyan);font-weight:500;}
.nav-icon{font-size:13px;width:18px;text-align:center;flex-shrink:0;}
.nav-badge{margin-left:auto;font-size:9px;font-family:var(--font-mono);background:var(--bg4);
           color:var(--text3);padding:1px 5px;border-radius:10px;}
.nav-item.active .nav-badge{background:var(--cyan-dim);color:var(--cyan);}
.sidebar-footer{margin-top:auto;padding:12px 14px;border-top:1px solid #343530;
                font-size:9px;color:var(--text3);font-family:var(--font-mono);}
.page-title{font-size:13px;font-weight:600;color:var(--text);font-family:var(--font-mono);}
.page-title::before{content:'> ';color:var(--cyan);}
.snap-info{font-size:10px;color:var(--text3);font-family:var(--font-mono);background:var(--bg3);
           padding:3px 8px;border-radius:20px;border:1px solid var(--border);}
.spacer{flex:1;}
.gen-at{font-size:10px;color:var(--text3);font-family:var(--font-mono);}
.btn-theme{font-size:10px;padding:5px 12px;background:transparent;color:var(--cyan);
         border:1px solid var(--cyan);border-radius:var(--radius);cursor:pointer;
         transition:all .15s;}
.btn-theme:hover{background:var(--cyan);color:var(--bg);}
.page{display:none;}.page.active{display:block;}
.metrics-row{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:16px;}
.metric-card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius-lg);
             padding:14px 16px;position:relative;overflow:hidden;transition:border-color .15s;}
.metric-card:hover{border-color:var(--border2);}
.metric-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;}
.metric-card.c-cyan::before{background:linear-gradient(90deg,var(--cyan),transparent);}
.metric-card.c-red::before{background:linear-gradient(90deg,var(--red),transparent);}
.metric-card.c-green::before{background:linear-gradient(90deg,var(--green),transparent);}
.metric-card.c-violet::before{background:linear-gradient(90deg,var(--violet),transparent);}
.metric-card.c-amber::before{background:linear-gradient(90deg,var(--amber),transparent);}
.metric-label{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:.1em;
              font-family:var(--font-mono);margin-bottom:6px;}
.metric-value{font-size:26px;font-weight:700;font-family:var(--font-mono);line-height:1;}
.metric-sub{font-size:9px;color:var(--text3);margin-top:4px;font-family:var(--font-mono);}
.metric-card.c-cyan .metric-value{color:var(--cyan);}
.metric-card.c-red .metric-value{color:var(--red);}
.metric-card.c-green .metric-value{color:var(--green);}
.metric-card.c-violet .metric-value{color:var(--violet);}
.metric-card.c-amber .metric-value{color:var(--amber);}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px;}
.card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius-lg);
      padding:16px;margin-bottom:12px;}
.card-title{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.12em;
            color:var(--text3);margin-bottom:12px;font-family:var(--font-mono);
            display:flex;align-items:center;gap:6px;}
.card-title::before{content:'▸';color:var(--cyan);font-size:10px;}
.bar-rows{display:flex;flex-direction:column;gap:8px;}
.bar-row{display:flex;align-items:center;gap:8px;}
.bar-label{font-size:11px;min-width:110px;max-width:110px;color:var(--text2);
           overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.bar-track{flex:1;height:5px;background:var(--bg4);border-radius:4px;overflow:hidden;}
.bar-fill{height:100%;border-radius:4px;}
.bar-count{font-size:10px;font-weight:600;min-width:22px;text-align:right;
           color:var(--text2);font-family:var(--font-mono);}
.proj-list{display:flex;flex-direction:column;gap:4px;}
.proj-item{display:flex;align-items:center;gap:7px;padding:8px 10px;border-radius:var(--radius);
           border:1px solid transparent;font-size:11px;cursor:pointer;
           transition:all .12s;background:var(--bg3);}
.proj-item:hover{border-color:var(--border2);background:var(--bg4);}
.proj-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0;}
.proj-name{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text);}
.proj-resp{font-size:9px;color:var(--text3);min-width:70px;text-align:right;font-family:var(--font-mono);}
.proj-pct{font-size:10px;font-weight:600;color:var(--text2);min-width:32px;
          text-align:right;font-family:var(--font-mono);}
.badge{font-size:9px;padding:2px 7px;border-radius:20px;font-weight:600;white-space:nowrap;
       flex-shrink:0;font-family:var(--font-mono);letter-spacing:.04em;}
.bON_TRACK{background:var(--green-dim);color:var(--green);border:1px solid rgba(16,217,148,.2);}
.bAT_RISK{background:var(--amber-dim);color:var(--amber);border:1px solid rgba(245,158,11,.2);}
.bLATE{background:var(--red-dim);color:var(--red);border:1px solid rgba(244,63,94,.2);}
.bDONE{background:var(--violet-dim);color:var(--violet);border:1px solid rgba(139,92,246,.2);}
.bON_HOLD{background:var(--bg4);color:var(--text3);border:1px solid var(--border);}
.bLIVRE{background:var(--green-dim);color:var(--green);border:1px solid rgba(16,217,148,.2);}
.bEN_COURS{background:var(--cyan-dim);color:var(--cyan);border:1px solid rgba(0,212,255,.2);}
.bNON_LIVRE{background:var(--red-dim);color:var(--red);border:1px solid rgba(244,63,94,.2);}
.bREPORTE{background:var(--amber-dim);color:var(--amber);border:1px solid rgba(245,158,11,.2);}
.collab-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:14px;}
.collab-card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius-lg);
             padding:14px;cursor:pointer;transition:all .15s;}
.collab-card:hover{border-color:var(--border2);background:var(--bg3);}
.collab-card.selected{border-color:var(--cyan);box-shadow:0 0 0 1px var(--cyan-dim);}
.avatar{width:36px;height:36px;border-radius:var(--radius);display:flex;align-items:center;
        justify-content:center;font-size:11px;font-weight:700;flex-shrink:0;font-family:var(--font-mono);}
.collab-header{display:flex;align-items:center;gap:8px;margin-bottom:8px;}
.collab-name{font-size:12px;font-weight:600;color:var(--text);}
.collab-sub{font-size:9px;color:var(--text3);font-family:var(--font-mono);}
.charge-bar{height:3px;background:var(--bg4);border-radius:3px;overflow:hidden;margin-top:8px;}
.charge-fill{height:100%;border-radius:3px;background:var(--cyan);}
.fchip.active { color: var(--ent-col, var(--cyan)); border-color: var(--ent-col, var(--cyan)); }
.gantt-controls{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;align-items:center;}
.gantt-controls select,.gantt-controls label{font-size:11px;color:var(--text2);font-family:var(--font-mono);}
.gantt-controls select{padding:4px 8px;border:1px solid var(--border);border-radius:var(--radius);
                       background:var(--bg3);color:var(--text);cursor:pointer;}
.gantt-wrap{overflow-x:auto;}
.gantt-table{border-collapse:collapse;min-width:700px;width:100%;font-size:10px;}
.gantt-table th,.gantt-table td{border:0;padding:0;}
.g-label{padding:5px 10px;font-size:10px;color:var(--text2);white-space:nowrap;
         max-width:160px;min-width:160px;overflow:hidden;text-overflow:ellipsis;
         cursor:pointer;font-family:var(--font-mono);}
.g-label:hover{color:var(--cyan);}
.g-header{text-align:center;font-size:9px;color:var(--text3);padding:4px 2px;
          border-bottom:1px solid #343530;min-width:44px;font-family:var(--font-mono);}
.g-cell{padding:3px 2px;position:relative;min-width:44px;height:28px;vertical-align:middle;}
.g-bar{position:absolute;top:6px;bottom:6px;border-radius:3px;}
.g-now{position:absolute;top:0;width:1.5px;bottom:0;z-index:5;background:var(--red);opacity:.8;}
.g-today-head{border-bottom:2px solid var(--red)!important;color:var(--red)!important;font-weight:700;}
.gantt-legend{display:flex;flex-wrap:wrap;gap:12px;margin-top:10px;}
.gantt-legend span{font-size:9px;color:var(--text3);display:flex;align-items:center;
                   gap:4px;font-family:var(--font-mono);}

.gantt-scroll-wrap{overflow-x:auto;cursor:grab;user-select:none;position:relative;}
.gantt-scroll-wrap:active{cursor:grabbing;}
.gantt-svg-container{position:relative;}
.gantt-toolbar{display:flex;gap:6px;align-items:center;margin-bottom:10px;flex-wrap:wrap;}
.gantt-toolbar select{font-size:11px;padding:4px 8px;border:1px solid var(--border2);
  border-radius:var(--radius);background:var(--bg3);color:var(--text);cursor:pointer;
  font-family:var(--font-mono);}
.granularity-btn{font-size:10px;padding:3px 10px;border-radius:20px;
  border:1px solid var(--border2);color:var(--text2);cursor:pointer;
  background:var(--bg2);font-family:var(--font-mono);transition:all .12s;}
.granularity-btn.active{background:var(--cyan-dim);color:var(--cyan);border-color:var(--cyan);}
.gantt-nav-btn{font-size:13px;padding:3px 12px;background:var(--bg2);
  border:1px solid var(--border2);border-radius:var(--radius);
  color:var(--text);cursor:pointer;font-family:var(--font-mono);transition:all .12s;}
.gantt-nav-btn:hover{border-color:var(--cyan);color:var(--cyan);}
.stat-tab:hover { color: var(--text) !important; }
.stat-tab.active { color: var(--cyan) !important; }
.tl-item{display:flex;gap:12px;padding:10px 0;border-bottom:1px solid #343530;}
.tl-item:last-child{border-bottom:none;}
.tl-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;margin-top:4px;}
.tl-body{flex:1;}
.tl-title{font-size:12px;font-weight:600;margin-bottom:3px;cursor:pointer;color:var(--text);}
.tl-title:hover{color:var(--cyan);}
.tl-meta{display:flex;gap:5px;align-items:center;flex-wrap:wrap;margin-top:3px;}
.chat-wrap{display:flex;flex-direction:column;height:calc(100vh - 110px);max-width:860px;margin:0 auto;}
.chat-header{font-family:var(--font-mono);font-size:10px;color:var(--text3);
             margin-bottom:8px;padding-bottom:8px;border-bottom:1px solid #343530;}
.chat-qs{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:10px;}
.chat-q{font-size:10px;padding:4px 10px;background:var(--bg3);border:1px solid var(--border);
        border-radius:20px;cursor:pointer;color:var(--text2);transition:all .12s;font-family:var(--font-mono);}
.chat-q:hover{border-color:var(--cyan);color:var(--cyan);}
.chat-msgs{flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:10px;padding:4px 0;}
.msg{display:flex;gap:10px;align-items:flex-start;}
.msg.user{flex-direction:row-reverse;}
.msg-av{width:28px;height:28px;border-radius:var(--radius);background:var(--bg3);
        border:1px solid var(--border);display:flex;align-items:center;justify-content:center;
        font-size:9px;font-weight:700;color:var(--text3);flex-shrink:0;font-family:var(--font-mono);}
.msg.user .msg-av{background:var(--cyan-dim);border-color:var(--cyan);color:var(--cyan);}
.bubble{max-width:76%;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius-lg);
        padding:10px 14px;font-size:12px;line-height:1.7;color:var(--text);white-space:pre-wrap;}
.msg.user .bubble{background:var(--bg3);border-color:var(--border2);}
.chat-bar{padding:10px 0;border-top:1px solid #343530;display:flex;gap:8px;flex-shrink:0;}
.chat-input{flex:1;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);
            padding:9px 13px;font-family:var(--font-mono);font-size:11px;outline:none;
            resize:none;color:var(--text);transition:border-color .12s;}
.chat-input:focus{border-color:var(--cyan);}
.chat-input::placeholder{color:var(--text3);}
.chat-send{background:var(--cyan);border:none;border-radius:var(--radius);padding:9px 18px;
           color:var(--bg);font-family:var(--font-mono);font-size:11px;cursor:pointer;
           font-weight:600;transition:all .12s;}
.chat-send:hover{background:var(--cyan2);}
.chat-send:disabled{opacity:.3;cursor:default;}
.modal-overlay{display:none;position:fixed;inset:0;background: (--view_cadre-dim);
               backdrop-filter:blur(4px);z-index:200;align-items:center;justify-content:center;}
.modal-overlay.open{display:flex;}
.modal{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius-lg);
       width:600px;max-width:95vw;max-height:88vh;overflow-y:auto;padding:24px;
       box-shadow:0 24px 64px rgba(0,0,0,.5);}
.modal-close{float:right;cursor:pointer;font-size:16px;color:var(--text3);
             border:none;background:none;line-height:1;padding:0;font-family:var(--font-mono);}
.modal-close:hover{color:var(--text);}
.modal-title{font-size:15px;font-weight:600;margin-bottom:4px;color:var(--text);}
.modal-id{font-size:9px;color:var(--cyan);font-family:var(--font-mono);margin-bottom:10px;}
.modal-row{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:12px;}
.modal-sec{margin-top:14px;}
.modal-stitle{font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;
              color:var(--text3);margin-bottom:6px;font-family:var(--font-mono);}
.modal-stitle::before{content:'▸ ';color:var(--cyan);}
.modal-text{font-size:12px;color:var(--text2);line-height:1.7;}
.prog-track{height:6px;background:var(--bg4);border-radius:4px;overflow:hidden;margin-top:6px;}
.prog-fill{height:100%;border-radius:4px;}
.meta-grid{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:6px;}
.meta-item{background:var(--bg3);border-radius:var(--radius);padding:7px 10px;}
.meta-key{font-size:8px;color:var(--text3);font-family:var(--font-mono);text-transform:uppercase;
          letter-spacing:.08em;margin-bottom:2px;}
.meta-val{font-size:11px;color:var(--text);font-family:var(--font-mono);}
.hist-row{display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid #343530;font-size:10px;}
.hist-q{min-width:100px;color:var(--text3);font-family:var(--font-mono);}
.filter-strip{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:12px;}
.fchip{font-size:10px;padding:3px 10px;border-radius:20px;border:1px solid var(--border);
       color:var(--text2);cursor:pointer;background:var(--bg2);transition:all .12s;font-family:var(--font-mono);}
.fchip:hover{border-color:var(--cyan);color:var(--cyan);}
.fchip.active{background:var(--cyan-dim);color:var(--cyan);border-color:var(--cyan);}
@media(max-width:1100px){.metrics-row{grid-template-columns:repeat(3,1fr);}
@media(max-width:900px){.grid2{grid-template-columns:1fr;}.collab-grid{grid-template-columns:1fr 1fr;}
@media print{
  .sidebar,.topbar,.chat-wrap,.modal-overlay{display:none!important;}
  .content{overflow:visible;padding:10px;}
  .page{display:block!important;}
  body{background:#fff;color:#000;}
  .card,.metric-card{border:1px solid #ccc;background:#fff;}
}
"""


# ── SCRIPT JS ─────────────────────────────────────────────────────────────────

SCRIPT = """
const PALETTE=["#00d4ff","#10d994","#8b5cf6","#f59e0b","#f43f5e","#a855f7","#06b6d4","#84cc16","#fb923c","#e879f9"];
const SC={"En cours":"#10d994","À risque":"#f59e0b","En retard":"#f43f5e","Terminé":"#8b5cf6","Stand by":"#475569"};
const PAGES={overview:"Vue d'ensemble",domaines:"Analyse par domaine",collabs:"Analyse par collaborateur",gantt:"Roadmap",evolutions:"Évolution",calendrier:"Calendrier",stats:"Statistiques",chat:"Chat"};
let selCollab=null;
let selEntiteGantt="";
let ganttGranularity = "month";   
let ganttOffsetPx    = 0;         
let ganttFiltreStatut = "";
let ganttFiltreEntite = "";
let ganttFiltreDomaine = "";
let ganttFiltreCollab = "";
let ganttGroupBy = "domaine";   // "domaine" | "collab"
let ganttIsDragging  = false;
let ganttDragStartX  = 0;
let ganttDragStartOffset = 0;
 
const GANTT_UNIT_W = { week:40, month:80, quarter:180 };
const GANTT_PAST   = { week:8,  month:3,  quarter:2   };
const GANTT_FUTURE = { week:24, month:9,  quarter:6   };

function esc(s){return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");}
function badge(st){const cls={"En cours":"bEN_COURS","À risque":"bAT_RISK","En retard":"bLATE","Terminé":"bDONE","Stand by":"bON_HOLD"}[st]||"bON_HOLD";return`<span class="badge ${cls}">${st||"—"}</span>`;}
function domColor(d){const ds=[...new Set(DATA.projets.map(p=>p.domaine).filter(Boolean))].sort();return PALETTE[ds.indexOf(d)%PALETTE.length]||PALETTE[0];}
function entColor(e){const es=[...new Set((DATA.entites||[]))].sort();return PALETTE[(es.indexOf(e)+5)%PALETTE.length]||PALETTE[5];}
function respColor(r){const rs=[...new Set(Object.keys(DATA.par_resp||{}))].sort();return PALETTE[(rs.indexOf(r)+3)%PALETTE.length]||PALETTE[3];}
function initials(n){return(n||"??").split(/\\s+/).map(w=>w[0]).join("").toUpperCase().slice(0,2);}
function avStyle(n){const c=[["rgba(0,212,255,.12)","#00d4ff"],["rgba(16,217,148,.12)","#10d994"],["rgba(139,92,246,.12)","#8b5cf6"],["rgba(245,158,11,.12)","#f59e0b"],["rgba(244,63,94,.12)","#f43f5e"],["rgba(168,85,247,.12)","#a855f7"]];const[bg,fg]=c[(n||"X").charCodeAt(0)%c.length];return`background:${bg};color:${fg}`;}
function pp(p){return p.avancement_pct||0;}
function nom(p){return p.projet_nom||p.sujet||"";}
function pid(p){return p.projet_id||p.ref_sujet||"";}
function projItem(p){
  const col=SC[p.statut]||"#475569";
  const prio=p.priorite||p.priorite_meta||"";
  const PRIO_STYLE={
    "ÉLEVÉ":  "background:var(--red-dim);color:var(--red);border:1px solid rgba(244,63,94,.2)",
    "ELEVE":  "background:var(--red-dim);color:var(--red);border:1px solid rgba(244,63,94,.2)",
    "MOYEN":  "background:var(--amber-dim);color:var(--amber);border:1px solid rgba(245,158,11,.2)",
    "FAIBLE": "background:var(--green-dim);color:var(--green);border:1px solid rgba(16,217,148,.2)",
  };
  const prioKey=(prio||"").toUpperCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");
  const prioStyle=PRIO_STYLE[prioKey]||PRIO_STYLE[(prio||"").toUpperCase()]||"background:var(--bg4);color:var(--text3);border:1px solid var(--border)";
  const prioTag=prio&&prio!=="nan"?`<span style="font-size:9px;padding:2px 7px;border-radius:20px;font-weight:600;white-space:nowrap;flex-shrink:0;font-family:var(--font-mono);letter-spacing:.04em;${prioStyle}">Priorité : ${esc(prio)}</span>`:"";
  return`<div class="proj-item" onclick="openModal('${esc(pid(p))}')">
    <span class="proj-dot" style="background:${col}"></span>
    <span class="proj-name" title="${esc(nom(p))}">${esc(nom(p))}</span>
    ${badge(p.statut)}
    ${prioTag}
    <span class="proj-resp">${esc(p.responsable_principal||"")}</span>
  </div>`;
}



function switchQuinzaine(q){
  if(!DATA.snapshots||!DATA.snapshots[q])return;
  const snap=DATA.snapshots[q];
  DATA.quinzaine=q;DATA.q_prev=snap.q_prev;DATA.kpis=snap.kpis;DATA.projets=snap.projets;
  DATA.domaines=snap.domaines;DATA.par_domaine=snap.par_domaine;DATA.par_resp=snap.par_resp;DATA.delta=snap.delta;
  DATA.par_entite=snap.par_entite;
  document.getElementById("snap-info").textContent=q+(snap.q_prev?" <- "+snap.q_prev:"");
  ["chat-q-label","chat-q-label2"].forEach(id=>{const el=document.getElementById(id);if(el)el.textContent=q;});
  document.getElementById("nb-overview").textContent=DATA.projets.length;
  document.getElementById("nb-evol").textContent=DATA.delta.length;
  renderOverview();renderDomaines();renderCollabs();renderEvolutions();
  const gp=document.getElementById("page-gantt");
  if(gp&&gp.innerHTML.trim()!=="")renderGantt();
  const cp=document.getElementById("page-calendrier");
  if(cp)cp.innerHTML="";
}

(function init(){
  document.getElementById("logo-date").textContent="Généré le "+DATA.genere_le;
  document.getElementById("gen-at").textContent=DATA.genere_le;
  document.getElementById("snap-info").textContent=DATA.quinzaine+(DATA.q_prev?" <- "+DATA.q_prev:"");
  ["chat-q-label","chat-q-label2"].forEach(id=>{const el=document.getElementById(id);if(el)el.textContent=DATA.quinzaine;});
  document.getElementById("nb-overview").textContent=DATA.projets.length;
  document.getElementById("nb-evol").textContent=(DATA.delta||[]).length;
  const nbCal=document.getElementById("nb-cal");
  if(nbCal)nbCal.textContent=(DATA.agenda||[]).length;
  document.getElementById("sidebar-footer").textContent=DATA.quinzaines.length+" quinzaine(s) chargee(s)";
  const sel=document.getElementById("q-selector");
  [...DATA.quinzaines].reverse().forEach(q=>{const o=document.createElement("option");o.value=q;o.textContent=q;if(q===DATA.quinzaine)o.selected=true;sel.appendChild(o);});
  const qsChat=[["quels projets sont en retard ?","en retard"],["quels projets sont à risque ?","a risque"],["quelles décisions ont été prises ?","decisions"],["y a-t-il des blocages actifs ?","blocages"],["quelles actions sont a mener en priorité ?","actions prio"],["quels projets arrivent bientôt à échéance ?","echeances"],["synthèse de la quinzaine","synthèse"],
["comparaison avec la quinzaine précédente", "comparaison"],
["tendance sur toutes les quinzaines", "tendance"]];
  document.getElementById("chat-qs").innerHTML=qsChat.map(([q,l])=>`<button class="chat-q" onclick="askChat('${q}')">${l}</button>`).join("");
  document.querySelectorAll(".nav-item[data-page]").forEach(el=>{
    el.addEventListener("click",()=>{
      document.querySelectorAll(".nav-item").forEach(n=>n.classList.remove("active"));
      document.querySelectorAll(".page").forEach(p=>p.classList.remove("active"));
      el.classList.add("active");const pg=el.dataset.page;
      document.getElementById("page-"+pg).classList.add("active");
      document.getElementById("page-title").textContent=PAGES[pg];
      if(pg==="gantt")renderGantt();
    if(pg==="calendrier")renderCalendrier();
    if(pg==="stats")renderStats();
    });
  });
  document.getElementById("modal-overlay").addEventListener("click",e=>{if(e.target===document.getElementById("modal-overlay"))closeModal();});
  renderOverview();renderDomaines();renderCollabs();renderEvolutions();
})();

function _eclaterEntites(val){
  if(!val||String(val).trim()==="")return[];
  return String(val).split(/[;,]/).map(e=>e.trim()).filter(Boolean);
}

const ENTITE_GROUPE="COFIDIS GROUP";
function _projetMatchEntite(p, ent){
  if(!ent)return true;
  const entites=_eclaterEntites(p.entite_concerne||p.entite_concerne_meta||"");
  // Si le projet est tagué COFIDIS GROUP, il matche toutes les entités
  if(entites.some(e=>e.toUpperCase()===ENTITE_GROUPE))return true;
  return entites.includes(ent);
}

function buildFiltreEntite(prefix){
  const entites=DATA.entites||[];
  if(!entites.length)return"";
  return`<div class="filter-strip" id="fe-${prefix}" style="margin-bottom:8px">
    <span style="font-size:9px;color:var(--text3);line-height:22px;font-family:var(--font-mono)">entité :</span>
    <span class="fchip active" data-ent="" onclick="handleFiltreEntite('${prefix}',this,'')">Toutes</span>
    ${entites.map(e=>`<span class="fchip" data-ent="${esc(e)}" style="--ent-col:${entColor(e)}" onclick="handleFiltreEntite('${prefix}',this,'${esc(e)}')">${esc(e)}</span>`).join("")}
  </div>`;
}

function handleFiltreEntite(prefix, el, ent){
  document.querySelectorAll(`#fe-${prefix} .fchip`).forEach(c=>c.classList.remove("active"));
  el.classList.add("active");
  if(prefix==="ov")filterOverviewByEntite(ent);
  else if(prefix==="dom")filterDomainesByEntite(ent);
  else if(prefix==="col")filterCollabsByEntite(ent);
  else if(prefix==="gantt"){selEntiteGantt=ent;buildGantt();}
}

function attachFiltreEntite(prefix, callback){
  document.querySelectorAll(`#fe-${prefix} .fchip`).forEach(c=>{
    c.onclick=()=>{
      document.querySelectorAll(`#fe-${prefix} .fchip`).forEach(x=>x.classList.remove("active"));
      c.classList.add("active");
      callback(c.dataset.ent||"");
    };
  });
}

function filterOverviewByEntite(ent){
  const PRIO_H=["ÉLEVÉ","ELEVE","ELEVÉ","HIGH"];
  const alertes=DATA.projets.filter(p=>(p.statut==="En retard"||p.statut==="À risque"||p.points_blocage)&&_projetMatchEntite(p,ent));
  const allF=DATA.projets.filter(p=>_projetMatchEntite(p,ent));
  const sortFn=(a,b)=>({"En retard":0,"À risque":1,"En cours":2,"Stand by":3,"Terminé":4}[a.statut]||9)-
                       ({"En retard":0,"À risque":1,"En cours":2,"Stand by":3,"Terminé":4}[b.statut]||9);
  const hasPrio=p=>{const pv=(p.priorite||p.priorite_meta||"").toUpperCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");return PRIO_H.some(h=>pv.includes(h));};
  const top=[...allF.filter(hasPrio).sort(sortFn),...allF.filter(p=>!hasPrio(p)).sort(sortFn)].slice(0,10);
  const listAl=document.getElementById("ov-proj-list");
  if(listAl)listAl.innerHTML=alertes.map(projItem).join("")||'<div style="font-size:10px;color:var(--text3);padding:8px;font-family:var(--font-mono)">// aucune alerte pour cette entite</div>';
  const listTop=document.getElementById("ov-top-list");
  if(listTop)listTop.innerHTML=top.map(projItem).join("")||'<div style="font-size:10px;color:var(--text3);padding:8px;font-family:var(--font-mono)">// aucun projet pour cette entite</div>';
}

function filterDomainesByEntite(ent){
  document.querySelectorAll(".dom-sec").forEach(s=>{
    if(!ent){s.style.display="";return;}
    const dom=s.dataset.dom;
    const hasProj=DATA.projets.some(p=>p.domaine===dom&&_projetMatchEntite(p,ent));
    s.style.display=hasProj?"":"none";
  });
}

function filterCollabsByEntite(ent){
  const filtered=ent?DATA.projets.filter(p=>_projetMatchEntite(p,ent)):DATA.projets;
  const resp=selCollab;
  const detail=filtered.filter(p=>p.responsable_principal===resp);
  const list=document.querySelector("#page-collabs .card .proj-list");
  if(list)list.innerHTML=detail.map(projItem).join("")||'<div style="font-size:10px;color:var(--text3);padding:8px;font-family:var(--font-mono)">// aucun projet pour cette entite</div>';
}

function buildArchivageSection(){
  const arch=DATA.archivage||[];
  if(!arch.length)return"";
  const MFR=["Jan","Fev","Mar","Avr","Mai","Jun","Jul","Aou","Sep","Oct","Nov","Dec"];
  return`<div class="card">
    <div class="card-title">projets archivés — 12 mois glissants (${arch.length})</div>
    <div class="proj-list">
      ${arch.map(a=>{
        const entites=_eclaterEntites(a.entite_concerne||"");
        let dateStr="";
        const _dfin=[a.date_fin,a.date_fin_prevue].find(v=>v&&!["nan","None","undefined","NaN"].includes(String(v).trim())&&String(v).trim()!=="");
        if(_dfin){
          try{
            const _s=String(_dfin).trim();
            const parts=_s.includes("/")?_s.split("/"):_s.split("-").reverse();
            const dt=new Date(parts[2],parts[1]-1,parts[0]);
            dateStr=!isNaN(dt.getTime())?"clôt. "+dt.getDate()+" "+MFR[dt.getMonth()]+" "+dt.getFullYear():"";
          }catch(e){dateStr="";}
        }
        return`<div class="proj-item" onclick="openModalArchivage('${esc(a.projet_id||a.ref_sujet)}')">
          <span class="proj-dot" style="background:var(--violet)"></span>
          <span class="proj-name">${esc(a.projet_nom||a.sujet||"")}</span>
          <span class="badge bDONE">Terminé</span>
          ${entites.slice(0,2).map(e=>`<span style="font-size:9px;padding:1px 5px;border-radius:8px;background:${entColor(e)}22;color:${entColor(e)};border:1px solid ${entColor(e)}44;font-family:var(--font-mono)">${esc(e)}</span>`).join("")}
          <span class="proj-resp">${dateStr}</span>
        </div>`;
      }).join("")}
    </div>
  </div>`;
}

function openModalArchivage(pid){
  const a=(DATA.archivage||[]).find(x=>(x.projet_id||x.ref_sujet)===pid);
  if(!a)return;
  const entites=_eclaterEntites(a.entite_concerne||"");
  const collabsTemp=a.collaborateurs_temporaires?String(a.collaborateurs_temporaires).split(/[;,]/).map(c=>c.trim()).filter(Boolean):[];
  document.getElementById("modal-body").innerHTML=`
    <button class="modal-close" onclick="closeModal()">x</button>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
      <span class="badge bDONE">Archivé</span>
      ${entites.map(e=>`<span style="font-size:9px;padding:2px 6px;border-radius:8px;background:var(--violet-dim);color:var(--violet);border:1px solid rgba(139,92,246,.2)">${esc(e)}</span>`).join("")}
    </div>
    <div class="modal-title">${esc(a.projet_nom||a.sujet||pid)}</div>
    <div class="modal-id">${esc(a.projet_id||a.ref_sujet||"")}</div>
    <div class="modal-row">
      ${a.domaine?`<span class="badge bON_HOLD">${esc(a.domaine)}</span>`:""}
      ${a.priorite?`<span class="badge bON_HOLD">prio:${esc(a.priorite)}</span>`:""}
      ${a.eta_projet?`<span class="badge bDONE">${esc(a.eta_projet)}</span>`:""}
    </div>
    <div class="modal-sec"><div class="modal-stitle">informations projet</div>
      <div class="meta-grid">
        ${[["responsable",a.responsable_principal],["date début",a.date_debut],["date fin prév.",a.date_fin],["budget j/sem",a.budget_jours],["effectifs",a.effectifs],["type",a.type]].filter(([,v])=>v&&v!=="undefined"&&v!=="nan").map(([k,v])=>`<div class="meta-item"><div class="meta-key">${k}</div><div class="meta-val">${esc(v)}</div></div>`).join("")}
      </div>
    </div>
    ${collabsTemp.length?`<div class="modal-sec"><div class="modal-stitle">collaborateurs temporaires</div>
      <div style="display:flex;gap:4px;flex-wrap:wrap">${collabsTemp.map(c=>`<span style="font-size:10px;padding:2px 8px;border-radius:12px;background:var(--bg3);color:var(--text2);border:1px solid var(--border2)">${esc(c)}</span>`).join("")}</div>
    </div>`:""}
    ${a.eta_intervention?`<div class="modal-sec"><div class="modal-stitle">période d'intervention</div><div class="modal-text">${esc(a.eta_intervention)}</div></div>`:""}
    ${a.description?`<div class="modal-sec"><div class="modal-stitle">description</div><div class="modal-text" style="color:var(--text3)">${esc(a.description)}</div></div>`:""}
  `;
  document.getElementById("modal-overlay").classList.add("open");
}




function renderOverview(){
  const k=DATA.kpis||{};const P=DATA.projets||[];
  const km=DATA.kpis_meta||{};
  const NP=DATA.nouveaux_projets||[];
  const ARCH=DATA.archivage||[];
  const maxD=Object.values(DATA.par_domaine||{}).length?Math.max(...Object.values(DATA.par_domaine).map(d=>d.total),1):1;
  const maxR=Object.values(DATA.par_resp||{}).length?Math.max(...Object.values(DATA.par_resp).map(r=>r.total),1):1;

  // KPIs META : nb projets type PROJET uniquement
  const nbProjetsActifs=km.nb_projets||0;
  // En retard/risque : uniquement sur type PROJET dans quinzaine
  const projetsSeuls=P.filter(p=>{ const t=(p.type||p.type_meta||"").toUpperCase(); return t===""||t==="PROJET"; });
  const nbRetard=projetsSeuls.filter(p=>p.statut==="En retard").length;
  const nbRisque=projetsSeuls.filter(p=>p.statut==="À risque").length;
  const nbTermines=ARCH.length;
  const nbNouveaux=NP.length;
  const autresTypes=(km.nb_gouvernance||0)+(km.nb_outil||0)+(km.nb_formation||0)+(km.nb_autre_type||0);

  const PRIO_HAUTE=["ÉLEVÉ","ELEVE","ELEVÉ","ÉLEVEE","HIGH"];
  const topPrio=[...P].filter(p=>{
    const pv=(p.priorite||p.priorite_meta||"").toUpperCase()
      .normalize("NFD").replace(/[\u0300-\u036f]/g,"");
    return PRIO_HAUTE.some(h=>pv.includes(h));
  }).sort((a,b)=>({"En retard":0,"À risque":1,"En cours":2,"Stand by":3,"Terminé":4}[a.statut]||9)-
                  ({"En retard":0,"À risque":1,"En cours":2,"Stand by":3,"Terminé":4}[b.statut]||9));
  // Si pas assez de projets élevés, compléter avec les autres par statut
  const autresPrio=[...P].filter(p=>{
    const pv=(p.priorite||p.priorite_meta||"").toUpperCase()
      .normalize("NFD").replace(/[\u0300-\u036f]/g,"");
    return !PRIO_HAUTE.some(h=>pv.includes(h));
  }).sort((a,b)=>({"En retard":0,"À risque":1,"En cours":2,"Stand by":3,"Terminé":4}[a.statut]||9)-
                  ({"En retard":0,"À risque":1,"En cours":2,"Stand by":3,"Terminé":4}[b.statut]||9));
  const top=[...topPrio,...autresPrio].slice(0,10);
  const alertes=projetsSeuls.filter(p=>p.statut==="En retard"||p.statut==="À risque"||p.points_blocage);

  document.getElementById("page-overview").innerHTML=`
    <div class="metrics-row" style="grid-template-columns:repeat(6,1fr)">
      <div class="metric-card c-cyan">
        <div class="metric-label">projets actifs</div>
        <div class="metric-value">${nbProjetsActifs}</div>
        <div class="metric-sub">type PROJET dans META</div>
      </div>
      <div class="metric-card c-red">
        <div class="metric-label">à risque / retard</div>
        <div class="metric-value">${nbRetard+nbRisque}</div>
        <div class="metric-sub">${nbRetard} retard · ${nbRisque} risque</div>
      </div>
      <div class="metric-card c-green">
        <div class="metric-label">terminés</div>
        <div class="metric-value">${nbTermines}</div>
        <div class="metric-sub">depuis archivage</div>
      </div>
      <div class="metric-card c-violet">
        <div class="metric-label">nouveaux projets</div>
        <div class="metric-value">${nbNouveaux}</div>
        <div class="metric-sub">jamais renseignés</div>
      </div>
      <div class="metric-card c-amber">
        <div class="metric-label">autres sujets</div>
        <div class="metric-value">${autresTypes}</div>
        <div class="metric-sub">gouv · outil · form · autre</div>
      </div>
      <div class="metric-card c-amber">
        <div class="metric-label">décisions / blocages</div>
        <div class="metric-value">${k.nb_decisions||0}</div>
        <div class="metric-sub">${k.nb_blocages||0} blocage(s)</div>
      </div>
    </div>
    ${alertes.length?`<div class="card"><div class="card-title">alertes actives (${alertes.length})</div><div class="proj-list" style="max-height:220px;overflow-y:auto" id="ov-proj-list">${alertes.map(projItem).join("")}</div></div>`:""}
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:12px">
     <div class="card"><div class="card-title">par entité</div>
        <div class="bar-rows">${Object.entries(DATA.par_entite||{}).sort((a,b)=>b[1].total-a[1].total).map(([e,s])=>`
          <div class="bar-row" style="max-height:200px;overflow-y:auto">
            <span class="bar-label" title="${esc(e)}">${esc(e)}</span>
            <div class="bar-track"><div class="bar-fill" style="width:${Math.round(s.total/Math.max(...Object.values(DATA.par_entite).map(x=>x.total),1)*100)}%;background:${entColor(e)}"></div></div>
            <span class="bar-count">${s.total}</span>
          </div>`).join("")}</div>
      </div>
      <div class="card"><div class="card-title">par domaine</div>
        <div class="bar-rows">${Object.entries(DATA.par_domaine).sort((a,b)=>b[1].total-a[1].total).map(([d,s])=>`
          <div class="bar-row" style="max-height:200px;overflow-y:auto">
            <span class="bar-label" title="${esc(d)}">${esc(d)}</span>
            <div class="bar-track"><div class="bar-fill" style="width:${Math.round(s.total/maxD*100)}%;background:${domColor(d)}"></div></div>
            <span class="bar-count">${s.total}</span>
          </div>`).join("")}</div>
      </div>
      <div class="card"><div class="card-title">par responsable</div>
        <div class="bar-rows">${Object.entries(DATA.par_resp).sort((a,b)=>b[1].total-a[1].total).map(([r,s])=>`
          <div class="bar-row" style="max-height:200px;overflow-y:auto">
            <span class="bar-label" title="${esc(r)}">${esc(r)}</span>
            <div class="bar-track"><div class="bar-fill" style="width:${Math.round(s.total/maxR*100)}%;background:${respColor(r)}"></div></div>
            <span class="bar-count">${s.total}</span>
          </div>`).join("")}</div>
      </div>
      
    </div>
    ${NP.length?`<div class="card"><div class="card-title">nouveaux projets jamais renseignés (${NP.length})</div>
      <div class="proj-list" style="max-height:180px;overflow-y:auto">
        ${NP.map(np=>`<div class="proj-item" style="cursor:default">
          <span class="proj-dot" style="background:var(--cyan)"></span>
          <span class="proj-name">${esc(np.projet_nom||np.projet_id)}</span>
          <span style="font-size:9px;font-family:var(--font-mono);color:var(--cyan);white-space:nowrap">NOUVEAU</span>
          ${np.priorite&&np.priorite!=="nan"?`<span style="font-size:9px;font-family:var(--font-mono);color:var(--text3)">P:${esc(np.priorite)}</span>`:""}
          <span class="proj-resp">${esc(np.responsable_principal||"")}</span>
        </div>`).join("")}
      </div></div>`:""}
    <div class="card"><div class="card-title">projets — vue prioritaire (${top.length})</div><div class="proj-list" style="max-height:320px;overflow-y:auto" id="ov-top-list">${top.map(projItem).join("")}</div></div>
    ${buildArchivageSection()}`;

}

function renderDomaines(){
  let html=buildFiltreEntite("dom")+'<div class="filter-strip" id="df"><span class="fchip active" data-val="">Tous</span>'+DATA.domaines.map(d=>'<span class="fchip" data-val="'+esc(d)+'">'+esc(d)+'</span>').join('')+'</div>';
  html+=DATA.domaines.map(dom=>{
    const pr=DATA.projets.filter(p=>p.domaine===dom);const s=DATA.par_domaine[dom]||{};
    let badges='';
    if(s.en_cours)badges+=`<span class="badge bEN_COURS">${s.en_cours} En cours</span>`;
    if(s.a_risque)badges+=`<span class="badge bAT_RISK">${s.a_risque} À risque</span>`;
    if(s.late)badges+=`<span class="badge bLATE">${s.late} En retard</span>`;
    if(s.terminé)badges+=`<span class="badge bDONE">${s.terminé} Terminé</span>`;
    return`<div class="card dom-sec" data-dom="${esc(dom)}"><div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;flex-wrap:wrap;gap:6px"><div style="display:flex;align-items:center;gap:8px"><span style="width:8px;height:8px;border-radius:50%;background:${domColor(dom)};flex-shrink:0"></span><span style="font-size:12px;font-weight:600;color:var(--text);font-family:var(--font-mono)">${esc(dom)}</span></div><div style="display:flex;gap:4px;flex-wrap:wrap">${badges}</div></div><div class="proj-list">${pr.map(projItem).join("")}</div></div>`;
  }).join("");
  document.getElementById("page-domaines").innerHTML=html;
  document.querySelectorAll("#df .fchip").forEach(c=>{c.addEventListener("click",()=>{document.querySelectorAll("#df .fchip").forEach(x=>x.classList.remove("active"));c.classList.add("active");const v=c.dataset.val;document.querySelectorAll(".dom-sec").forEach(s=>{s.style.display=(!v||s.dataset.dom===v)?"":"none";});});});
}

function renderCollabs(sel){
  sel=sel||selCollab||Object.keys(DATA.par_resp)[0]||"";selCollab=sel;
  const resps=Object.entries(DATA.par_resp).sort((a,b)=>b[1].total-a[1].total);
  const maxE=Math.max(...resps.map(([,r])=>r.en_cours),1);
  const detail=DATA.projets.filter(p=>p.responsable_principal===sel).sort((a,b)=>({"En retard":0,"À risque":1,"En cours":2,"Stand by":3,"Terminé":4}[a.statut]||9)-({"En retard":0,"À risque":1,"En cours":2,"Stand by":3,"Terminé":4}[b.statut]||9));
  document.getElementById("page-collabs").innerHTML=`
    <div class="collab-grid">${resps.map(([name,r])=>`<div class="collab-card ${name===sel?"selected":""}" onclick="renderCollabs('${esc(name)}')"><div class="collab-header"><div class="avatar" style="background:${respColor(name)}22;color:${respColor(name)}">${initials(name)}</div><div><div class="collab-name">${esc(name)}</div><div class="collab-sub">${r.total} projet${r.total>1?"s":""} · ${r.en_cours} actif${r.en_cours>1?"s":""}</div></div></div><div style="display:flex;gap:4px;flex-wrap:wrap">${r.domaines.slice(0,3).map(d=>`<span style="font-size:9px;padding:2px 6px;border-radius:10px;background:var(--bg4);color:var(--text3);font-family:var(--font-mono)">${esc(d)}</span>`).join("")}</div><div class="charge-bar"><div class="charge-fill" style="width:${Math.round(r.en_cours/maxE*100)}%;background:${respColor(name)}"></div></div></div>`).join("")}</div>
    <div class="card"><div class="card-title">projets :: ${esc(sel)} (${detail.length})</div><div class="proj-list">${detail.map(projItem).join("")||'<div style="color:var(--text3);font-size:11px;font-family:var(--font-mono);padding:8px">// aucun projet</div>'}</div></div>`;
}


 
function renderGantt(){
  const el = document.getElementById("page-gantt");
  if(!el) return;
  el.innerHTML = `
    <div class="gantt-toolbar">
      <button class="gantt-nav-btn" onclick="ganttNav(-3)">&#8249;&#8249;</button>
      <button class="gantt-nav-btn" onclick="ganttNav(-1)">&#8249;</button>
      <button class="gantt-nav-btn" onclick="ganttGoToday()">Aujourd'hui</button>
      <button class="gantt-nav-btn" onclick="ganttNav(1)">&#8250;</button>
      <button class="gantt-nav-btn" onclick="ganttNav(3)">&#8250;&#8250;</button>
      <div style="flex:1"></div>
      <span style="font-size:9px;color:var(--text3);font-family:var(--font-mono)">granularité :</span>
      <button class="granularity-btn active" id="gbtn-week"    onclick="setGranularity('week')">Semaine</button>
      <button class="granularity-btn"        id="gbtn-month"   onclick="setGranularity('month')">Mois</button>
      <button class="granularity-btn"        id="gbtn-quarter" onclick="setGranularity('quarter')">Trimestre</button>
      <span style="font-size:9px;color:var(--text3);font-family:var(--font-mono);margin-left:8px">statut :</span>
      <select id="gf" onchange="ganttFiltreStatut=this.value;buildGantt()">
        <option value="">Tous</option>
        <option value="En cours">En cours</option>
        <option value="À risque">À risque</option>
        <option value="En retard">En retard</option>
        <option value="Terminé">Terminé</option>
      </select>
      ${(DATA.entites||[]).length?`
      <span style="font-size:9px;color:var(--text3);font-family:var(--font-mono)">entité :</span>
      <select id="ge" onchange="ganttFiltreEntite=this.value;buildGantt()">
        <option value="">Toutes</option>
        ${(DATA.entites||[]).map(e=>`<option value="${esc(e)}">${esc(e)}</option>`).join("")}
      </select>`:""}
      <span style="font-size:9px;color:var(--text3);font-family:var(--font-mono);margin-left:8px">vue :</span>
      <button class="granularity-btn active" id="gbtn-dom" onclick="setGanttGroup('domaine')">Domaine</button>
      <button class="granularity-btn" id="gbtn-col" onclick="setGanttGroup('collab')">Collaborateur</button>
      <select id="gdom" onchange="ganttFiltreDomaine=this.value;buildGantt()" style="margin-left:8px">
        <option value="">Tous domaines</option>
        ${(DATA.domaines||[]).map(d=>`<option value="${esc(d)}">${esc(d)}</option>`).join("")}
      </select>
    </div>
    <div class="card" style="padding:0;overflow:hidden">
      <div id="gantt-scroll-area" class="gantt-scroll-wrap">
        <div id="gantt-inner"></div>
      </div>
    </div>
    <div class="gantt-legend" id="gl"></div>`;
 
  // Démarrer en vue mois par défaut
  ganttGranularity = "month";
  ganttOffsetPx    = 0;
  _attachGanttScroll();
  buildGantt();
}
 
function setGranularity(g){
  ganttGranularity = g;
  ganttOffsetPx    = 0;
  document.querySelectorAll(".granularity-btn").forEach(b=>b.classList.remove("active"));
  document.getElementById("gbtn-"+g)?.classList.add("active");
  buildGantt();
}
 
function setGanttGroup(g){
  ganttGroupBy=g;
  document.getElementById("gbtn-dom")?.classList.toggle("active", g==="domaine");
  document.getElementById("gbtn-col")?.classList.toggle("active", g==="collab");
  buildGantt();
}

function ganttNav(n){
  const uw = GANTT_UNIT_W[ganttGranularity]||80;
  ganttOffsetPx -= n * uw * 2;
  buildGantt();
}
 
function ganttGoToday(){
  ganttOffsetPx = 0;
  buildGantt();
}
 
function _attachGanttScroll(){
  const wrap = document.getElementById("gantt-scroll-area");
  if(!wrap) return;
 
  // Drag scroll souris
  wrap.addEventListener("mousedown", e=>{
    ganttIsDragging   = true;
    ganttDragStartX   = e.clientX;
    ganttDragStartOffset = ganttOffsetPx;
    e.preventDefault();
  });
  window.addEventListener("mousemove", e=>{
    if(!ganttIsDragging) return;
    ganttOffsetPx = ganttDragStartOffset + (e.clientX - ganttDragStartX);
    buildGantt();
  });
  window.addEventListener("mouseup", ()=>{ ganttIsDragging = false; });
 
  // Scroll molette horizontal
  wrap.addEventListener("wheel", e=>{
    e.preventDefault();
    ganttOffsetPx -= e.deltaX || e.deltaY;
    buildGantt();
  }, { passive:false });
 
  // Touch mobile
  let touchStartX = 0;
  wrap.addEventListener("touchstart", e=>{ touchStartX = e.touches[0].clientX; }, {passive:true});
  wrap.addEventListener("touchmove", e=>{
    const dx = e.touches[0].clientX - touchStartX;
    ganttOffsetPx += dx;
    touchStartX = e.touches[0].clientX;
    buildGantt();
  }, {passive:true});
}
 
function buildGantt(){
  const inner = document.getElementById("gantt-inner");
  if(!inner) return;
 
  const uw    = GANTT_UNIT_W[ganttGranularity] || 80;
  const past  = GANTT_PAST[ganttGranularity]   || 3;
  const fut   = GANTT_FUTURE[ganttGranularity] || 9;
  const now   = new Date();
  const ROW_H = 28;
  const HDR_H = 48;
  const LBL_W = 170;
  const MFR   = ["Jan","Fév","Mar","Avr","Mai","Jun","Jul","Aoû","Sep","Oct","Nov","Déc"];
  const TRIM  = ["T1","T2","T3","T4"];
 
  // ── Générer les unités temporelles ──────────────────────────────────
  function startOfWeek(d){
    const r=new Date(d);r.setDate(r.getDate()-(r.getDay()||7)+1);
    r.setHours(0,0,0,0);return r;
  }
 
  let units = [];  // { label, start, end }
  if(ganttGranularity === "week"){
    const origin = startOfWeek(now);
    for(let i=-past*4; i<=fut*4; i++){
      const s = new Date(origin); s.setDate(s.getDate()+i*7);
      const e = new Date(s); e.setDate(e.getDate()+6);
      units.push({ label: s.getDate()+"/"+MFR[s.getMonth()], start:new Date(s), end:new Date(e) });
    }
  } else if(ganttGranularity === "month"){
    for(let i=-past; i<=fut; i++){
      const s = new Date(now.getFullYear(), now.getMonth()+i, 1);
      const e = new Date(now.getFullYear(), now.getMonth()+i+1, 0);
      units.push({ label: MFR[s.getMonth()]+"'"+String(s.getFullYear()).slice(2), start:new Date(s), end:new Date(e) });
    }
  } else {
    for(let i=-past; i<=fut; i++){
      const qBase = Math.floor(now.getMonth()/3);
      const qOff  = qBase + i;
      const yr    = now.getFullYear() + Math.floor(qOff/4);
      const qIdx  = ((qOff%4)+4)%4;
      const s     = new Date(yr, qIdx*3, 1);
      const e     = new Date(yr, qIdx*3+3, 0);
      units.push({ label: TRIM[qIdx]+" "+yr, start:new Date(s), end:new Date(e) });
    }
  }
 
  const totalUnits = units.length;
  const svgW       = LBL_W + totalUnits * uw;
 
  // ── Position pixel d'une date ────────────────────────────────────────
  const rangeStart = units[0].start;
  const rangeEnd   = units[units.length-1].end;
  const rangeMs    = rangeEnd - rangeStart;
  function xPos(d){
    return LBL_W + Math.max(0, Math.min(1,(d-rangeStart)/rangeMs)) * (totalUnits*uw);
  }
 
  // ── Position "aujourd'hui" ───────────────────────────────────────────
  const todayX = xPos(now);
 
  // ── Filtrer projets avec dates valides ───────────────────────────────
  const metaById={};
  (DATA.meta||[]).forEach(m=>{ metaById[m.projet_id||m.ref_sujet]=m; });
 
  function parseDate(s){
    if(!s||String(s).trim()===""||String(s)==="nan")return null;
    const v=String(s).trim();
    const parts=v.includes("/")?v.split("/"):null;
    if(parts&&parts.length===3)return new Date(parts[2],parts[1]-1,parts[0]);
    try{return new Date(v);}catch(e){return null;}
  }
 
  let projets = DATA.projets.filter(p=>{
    if(ganttFiltreStatut&&p.statut!==ganttFiltreStatut) return false;
    if(ganttFiltreEntite&&!_projetMatchEntite(p,ganttFiltreEntite)) return false;
    if(ganttFiltreDomaine&&p.domaine!==ganttFiltreDomaine) return false;
    if(ganttFiltreCollab&&p.responsable_principal!==ganttFiltreCollab) return false;
    const m = metaById[p.projet_id||p.ref_sujet]||{};
    const deb = parseDate(p.date_debut||m.date_debut);
    const fin = parseDate(p.date_fin||m.date_fin);
    return deb&&fin; // masquer sans dates
  });
 
  // ── Grouper selon ganttGroupBy ──────────────────────────────────────
  const groups={};
  projets.forEach(p=>{
    const k=ganttGroupBy==="collab"
      ?(p.responsable_principal||"Non assigné")
      :(p.domaine||"Autre");
    if(!groups[k])groups[k]=[];
    groups[k].push(p);
  });
 
  // ── Calcul hauteur totale SVG ────────────────────────────────────────
  let totalRows = 0;
  Object.values(groups).forEach(items=>{ totalRows += 1 + items.length; });
  const svgH = HDR_H + totalRows * ROW_H + 20;
 
  // ── Construction SVG ─────────────────────────────────────────────────
  let svg = `<svg xmlns="http://www.w3.org/2000/svg"
    width="${svgW + Math.abs(ganttOffsetPx)}" height="${svgH}"
    style="display:block;transition:transform .05s linear">`;
 
  // Fond et grille verticale
  svg += `<rect width="100%" height="100%" fill="var(--bg2)"/>`;
 
  // En-têtes unités
  units.forEach((u,i)=>{
    const x = LBL_W + i*uw;
    const isNow = u.start <= now && now <= u.end;
    svg += `<rect x="${x}" y="0" width="${uw}" height="${HDR_H}"
      fill="${isNow?"rgba(0,212,255,.06)":"var(--bg3)"}"
      stroke="var(--border)" stroke-width="0.5"/>`;
    svg += `<text x="${x+uw/2}" y="${HDR_H/2+4}" text-anchor="middle"
      font-size="10" fill="${isNow?"var(--cyan)":"var(--text3)"}"
      font-family="var(--font-mono)" font-weight="${isNow?"600":"400"}">${u.label}</text>`;
    // Ligne verticale grille
    svg += `<line x1="${x}" y1="${HDR_H}" x2="${x}" y2="${svgH}"
      stroke="var(--border)" stroke-width="0.5" opacity="0.5"/>`;
  });
 
  // Ligne fixe label col
  svg += `<rect x="0" y="0" width="${LBL_W}" height="${svgH}"
    fill="var(--bg2)" stroke="var(--border)" stroke-width="0.5"/>`;
  svg += `<text x="10" y="${HDR_H/2+4}" font-size="9" fill="var(--text3)"
    font-family="var(--font-mono)">PROJET</text>`;
 
  // Lignes projets
  let rowIdx = 0;
  Object.entries(groups).sort().forEach(([grp,items],gi)=>{
    const gc = PALETTE[gi%PALETTE.length];
    const gy = HDR_H + rowIdx * ROW_H;
 
    // En-tête groupe
    svg += `<rect x="0" y="${gy}" width="${svgW}" height="${ROW_H}"
      fill="var(--bg3)" stroke="var(--border)" stroke-width="0.3"/>`;
    svg += `<text x="10" y="${gy+ROW_H/2+4}" font-size="9" font-weight="600"
      fill="${gc}" font-family="var(--font-mono)" letter-spacing="0.08em"
      text-transform="uppercase">${grp.toUpperCase()}</text>`;
    rowIdx++;
 
    items.forEach(p=>{
      const ry  = HDR_H + rowIdx * ROW_H;
      const col = SC[p.statut]||gc;
      const m   = metaById[p.projet_id||p.ref_sujet]||{};
      const deb = parseDate(p.date_debut||m.date_debut);
      const fin = parseDate(p.date_fin||m.date_fin);
      const pv  = p.avancement_pct||0;
      const isOver  = fin < now && p.statut!=="Terminé";
      const isSoon  = fin > now && (fin-now)<30*24*3600*1000;
      const x1  = xPos(deb);
      const x2  = Math.max(xPos(fin), x1+4);
      const bw  = x2-x1;
 
      // Fond ligne alternée
      svg += `<rect x="0" y="${ry}" width="${svgW}" height="${ROW_H}"
        fill="${rowIdx%2===0?"rgba(255,255,255,.01)":"transparent"}"
        stroke="var(--border)" stroke-width="0.3"/>`;
 
      // Label projet
      const nomTronc = (nom(p)||"").slice(0,22)+(nom(p).length>22?"…":"");
      svg += `<text x="8" y="${ry+ROW_H/2+4}" font-size="10"
        fill="var(--text2)" font-family="var(--font-mono)"
        style="cursor:pointer" onclick="openModal('${p.projet_id||p.ref_sujet}')">${esc(nomTronc)}</text>`;
 
      // Barre projet
      if(bw>0){
        if(isOver){
          // Barre pointillée pour projets dépassés
          svg += `<rect x="${x1}" y="${ry+6}" width="${bw}" height="${ROW_H-14}"
            fill="none" stroke="${col}" stroke-width="1.5"
            stroke-dasharray="4,3" rx="3" opacity="0.6"/>`;
        } else {
          // Barre pleine
          svg += `<rect x="${x1}" y="${ry+6}" width="${bw}" height="${ROW_H-14}"
            fill="${col}" rx="3" opacity="0.8"/>`;
          // Barre progression interne
          if(pv>0&&pv<100){
            svg += `<rect x="${x1}" y="${ry+6}" width="${bw*pv/100}" height="${ROW_H-14}"
              fill="${col}" rx="3" opacity="0.4"/>`;
          }
          // Label pourcentage si assez large
          if(bw>30){
            svg += `<text x="${x1+bw/2}" y="${ry+ROW_H/2+4}" text-anchor="middle"
              font-size="9" fill="white" font-family="var(--font-mono)"
              pointer-events="none">${pv}%</text>`;
          }
        }
        // Point rouge échéance proche
        if(isSoon){
          svg += `<circle cx="${x2}" cy="${ry+ROW_H/2}" r="4"
            fill="#f43f5e" stroke="var(--bg2)" stroke-width="1.5"/>`;
        }
      }
      rowIdx++;
    });
  });
 
  // Ligne aujourd'hui — par-dessus tout
  svg += `<line x1="${todayX}" y1="0" x2="${todayX}" y2="${svgH}"
    stroke="#f43f5e" stroke-width="1.5" opacity="0.7" stroke-dasharray="4,3"/>`;
  svg += `<text x="${todayX+4}" y="12" font-size="8" fill="#f43f5e"
    font-family="var(--font-mono)">auj.</text>`;
 
  svg += `</svg>`;
 
  inner.innerHTML = svg;
 
  // Légende
  const gl = document.getElementById("gl");
  if(gl){
    gl.innerHTML =
      `<span><i style="width:12px;height:1.5px;background:#f43f5e;display:inline-block"></i> aujourd'hui</span>`+
      `<span><i style="width:12px;height:8px;border:1.5px dashed #94a3b8;border-radius:2px;display:inline-block"></i> date dépassée</span>`+
      `<span><i style="width:6px;height:6px;border-radius:50%;background:#f43f5e;display:inline-block"></i> échéance &lt;30j</span>`+
      `<span style="color:var(--text3);font-family:var(--font-mono);font-size:9px">
        ← glisser pour naviguer · molette pour scroller →
      </span>`;
  }
}

function computeStats(){
  const snaps=DATA.snapshots||{};
  const qs=DATA.quinzaines||[];
  const hist=DATA.historiques||{};
  const meta=DATA.meta||[];

  // ── Série temporelle : une entrée par quinzaine ──────────────────
  const serie=qs.map(q=>{
    const s=snaps[q]||{};
    const k=s.kpis||{};
    const p=s.projets||[];
    const total=p.length||1;
    const enDiff=(k.nb_at_risk||0)+(k.nb_en_retard||0);
    const livres=p.filter(x=>x.livrable_statut==="LIVRE").length;
    const nonLivres=p.filter(x=>x.livrable_statut==="NON LIVRE").length;
    const reportes=p.filter(x=>x.livrable_statut==="REPORTE").length;
    const avecLivrable=p.filter(x=>x.livrable_statut&&x.livrable_statut.trim()!=="").length;
    return {
      q,
      avancement:k.avancement_moyen||0,
      tauxDiff:Math.round(enDiff/total*100),
      nbDiff:enDiff,
      nbBlocages:k.nb_blocages||0,
      nbDecisions:k.nb_decisions||0,
      nbActifs:k.nb_projets_actifs||0,
      livres, nonLivres, reportes, avecLivrable,
      tauxLivre:avecLivrable>0?Math.round(livres/avecLivrable*100):null,
      parDomaine:s.par_domaine||{},
      parResp:s.par_resp||{},
      projets:p,
    };
  });

  // ── Nouveaux projets par quinzaine ────────────────────────────────
  // Pour chaque quinzaine Q, un projet est "nouveau" s'il apparaît
  // dans Q mais pas dans les quinzaines antérieures
  const serieNouveaux=[];
  const idsVus=new Set();
  qs.forEach(q=>{
    const projIds=new Set(
      (snaps[q]?.projets||[])
        .filter(p=>{const t=(p.type||p.type_meta||"").toUpperCase();return t===""||t==="PROJET"||t==="NAN";})
        .map(p=>p.projet_id||p.ref_sujet)
        .filter(Boolean)
    );
    let nbNew=0;
    projIds.forEach(id=>{if(!idsVus.has(id)){nbNew++;idsVus.add(id);}});
    serieNouveaux.push({q,nbNew,nbCumul:idsVus.size});
  });

  // ── Vélocité par domaine : delta avancement moyen entre quinzaines ─
  const domaines=[...new Set(qs.flatMap(q=>(snaps[q]?.projets||[]).map(p=>p.domaine).filter(Boolean)))];
  const velociteDomaine={};
  domaines.forEach(dom=>{
    const pts=[];
    for(let i=1;i<qs.length;i++){
      const avant=(snaps[qs[i-1]]?.projets||[]).filter(p=>p.domaine===dom);
      const apres=(snaps[qs[i]]?.projets||[]).filter(p=>p.domaine===dom);
      if(!avant.length||!apres.length)continue;
      const avMap={};avant.forEach(p=>avMap[p.projet_id||p.ref_sujet]=p.avancement_pct||0);
      const deltas=apres.map(p=>{const id=p.projet_id||p.ref_sujet;return avMap[id]!=null?(p.avancement_pct||0)-avMap[id]:null;}).filter(x=>x!=null);
      if(deltas.length)pts.push(deltas.reduce((a,b)=>a+b,0)/deltas.length);
    }
    velociteDomaine[dom]=pts.length?Math.round(pts.reduce((a,b)=>a+b,0)/pts.length*10)/10:null;
  });

  // ── Signaux faibles ───────────────────────────────────────────────
  const signauxFaibles=[];
  Object.entries(hist).forEach(([pid,rows])=>{
    if(rows.length<2)return;
    const sorted=[...rows].sort((a,b)=>a.quinzaine.localeCompare(b.quinzaine));
    const nom=sorted[0].projet_nom||sorted[0].sujet||pid;

    // Stagnation : delta < 5% sur 2 quinzaines consécutives
    for(let i=1;i<sorted.length;i++){
      const delta=Math.abs((sorted[i].avancement_pct||0)-(sorted[i-1].avancement_pct||0));
      if(delta<5&&sorted[i].statut!=="Terminé"&&sorted[i].statut!=="Stand by"){
        signauxFaibles.push({
          type:"STAGNATION",
          nom, pid,
          detail:`Avancement stable (Δ${Math.round(delta)}%) entre ${sorted[i-1].quinzaine} et ${sorted[i].quinzaine}`,
          statut:sorted[i].statut, quinzaine:sorted[i].quinzaine,
        });
        break;
      }
    }

    // Oscillation : statut change ≥ 3 fois
    const changements=sorted.filter((r,i)=>i>0&&r.statut!==sorted[i-1].statut).length;
    if(changements>=3){
      signauxFaibles.push({
        type:"OSCILLATION",
        nom, pid,
        detail:`Statut a changé ${changements} fois sur ${sorted.length} quinzaines`,
        statut:sorted[sorted.length-1].statut,
        quinzaine:sorted[sorted.length-1].quinzaine,
      });
    }

    // Trainards : ≥ 3 quinzaines consécutives En retard ou À risque
    let streak=0,maxStreak=0;
    sorted.forEach(r=>{
      if(r.statut==="En retard"||r.statut==="À risque"){streak++;maxStreak=Math.max(maxStreak,streak);}
      else streak=0;
    });
    if(maxStreak>=3){
      signauxFaibles.push({
        type:"TRAINARD",
        nom, pid,
        detail:`${maxStreak} quinzaines consécutives en difficulté`,
        statut:sorted[sorted.length-1].statut,
        quinzaine:sorted[sorted.length-1].quinzaine,
      });
    }
  });

  // Concentration charge : responsable avec > 40% des projets actifs
  const dernierSnap=serie[serie.length-1]||{};
  const parResp=dernierSnap.parResp||{};
  const totalActifs=Object.values(parResp).reduce((s,r)=>s+r.total,0)||1;
  const concentrations=Object.entries(parResp)
    .map(([r,d])=>{return{resp:r,n:d.total,pct:Math.round(d.total/totalActifs*100)};  })
    .filter(x=>x.pct>40);

  return {serie, velociteDomaine, signauxFaibles, concentrations, domaines, serieNouveaux};
}

// ── SVG helpers ────────────────────────────────────────────────────────

function svgLine(points,color,h=120,pad=30){
  if(points.length<2)return"";
  const vals=points.map(p=>p.y);
  const min=Math.min(...vals),max=Math.max(...vals);
  const range=max-min||1;
  const w=points.length>1?(points[points.length-1].x-points[0].x):1;
  const pts=points.map(p=>{
    const x=pad+(p.x-points[0].x)/(w||1)*(400-pad*2);
    const y=pad+(1-(p.y-min)/range)*(h-pad*2);
    return`${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const first=pts.split(" ")[0],last=pts.split(" ").pop();
  const [lx,ly]=last.split(",");
  const [fx,fy]=first.split(",");
  return`<polyline points="${pts}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    <polygon points="${pts} ${lx},${h-pad} ${fx},${h-pad}" fill="${color}" opacity="0.08"/>
    ${points.map((p,i)=>{const[px,py]=pts.split(" ")[i].split(",");return`<circle cx="${px}" cy="${py}" r="3" fill="${color}" stroke="var(--bg2)" stroke-width="1.5"/>`;}  ).join("")}`;
}

function svgChart(serie,keyFn,color,label,unit="%",h=140){
  if(!serie.length)return`<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);padding:12px">// données insuffisantes</div>`;
  const pad=32;const W=400;
  const points=serie.map((s,i)=>({x:i,y:keyFn(s),label:s.q}));
  const vals=points.map(p=>p.y);
  const min=Math.min(...vals),max=Math.max(...vals);
  const range=max-min||1;

  // Ticks sans doublons
  const rawTicks=[0,.25,.5,.75,1].map(t=>min+t*range);
  const ticks=[...new Set(rawTicks.map(t=>Math.round(t)))];

  // Coordonnées X cohérentes entre points et labels
  const xOf=i=>pad+i/(points.length-1||1)*(W-pad*2);
  const yOf=v=>pad+(1-(v-min)/range)*(h-pad*2);

  const ptsStr=points.map(p=>`${xOf(p.x).toFixed(1)},${yOf(p.y).toFixed(1)}`).join(" ");
  const [lx]=ptsStr.split(" ").pop().split(",");
  const [fx]=ptsStr.split(" ")[0].split(",");

  return`<svg viewBox="0 0 ${W} ${h}" style="width:100%;height:${h}px;overflow:visible">
    <defs><linearGradient id="g${label.replace(/\s/g,"_")}" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="${color}" stop-opacity=".3"/>
      <stop offset="100%" stop-color="${color}" stop-opacity="0"/>
    </linearGradient></defs>
    ${ticks.map(t=>{const y=yOf(t);
      return`<line x1="${pad}" y1="${y.toFixed(1)}" x2="${W-10}" y2="${y.toFixed(1)}" stroke="var(--border)" stroke-width="0.5"/>
        <text x="${pad-4}" y="${(y+3).toFixed(1)}" text-anchor="end" font-size="8" fill="var(--text3)" font-family="var(--font-mono)">${t}${unit}</text>`;
    }).join("")}
    <polyline points="${ptsStr}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
    <polygon points="${ptsStr} ${lx},${h-pad} ${fx},${h-pad}" fill="${color}" opacity="0.08"/>
    ${points.map((p,i)=>`
      <circle cx="${xOf(i).toFixed(1)}" cy="${yOf(p.y).toFixed(1)}" r="3" fill="${color}" stroke="var(--bg2)" stroke-width="1.5"/>
      <text x="${xOf(i).toFixed(1)}" y="${h-4}" text-anchor="middle" font-size="7.5" fill="var(--text3)" font-family="var(--font-mono)">${p.label.replace(/_/g," ")}</text>
    `).join("")}
  </svg>`;
}

function barChart(items,color){
  if(!items.length)return`<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);padding:8px">// aucune donnée</div>`;
  const max=Math.max(...items.map(i=>i.val),1);
  return items.sort((a,b)=>b.val-a.val).map(item=>{
    const pct=Math.round(item.val/max*100);
    const color2=item.color||color;
    return`<div style="display:flex;align-items:center;gap:8px;margin-bottom:7px">
      <span style="font-size:10px;min-width:110px;max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text2)" title="${esc(item.label)}">${esc(item.label)}</span>
      <div style="flex:1;height:6px;background:var(--bg4);border-radius:4px;overflow:hidden">
        <div style="width:${pct}%;height:100%;border-radius:4px;background:${color2};transition:width .5s ease"></div>
      </div>
      <span style="font-size:10px;font-weight:600;min-width:32px;text-align:right;font-family:var(--font-mono);color:var(--text2)">${item.val}${item.unit||""}</span>
    </div>`;
  }).join("");
}

function jauge(val,max,color,label,sublabel=""){
  const pct= val / max;
  const angle= Math.PI * pct;
  const r=38;const cx=50;const cy=50;
  const x=cx + r * Math.cos(Math.PI + angle);
  const y=cy + r * Math.sin(Math.PI + angle);
  const large=pct>50?1:0;
  return`<div style="text-align:center">
    <svg viewBox="0 0 100 70" style="width:90px;height:63px">
      <path d="M${cx-r},${cy} A${r},${r} 0 1 1 ${cx+r},${cy}" fill="none" stroke="var(--bg4)" stroke-width="8" stroke-linecap="round"/>
      ${pct>0?`<path d="M${cx-r},${cy} A${r},${r} 0 ${large} 1 ${x.toFixed(1)},${y.toFixed(1)}" fill="none" stroke="${color}" stroke-width="8" stroke-linecap="round"/>`:"" }
      <text x="50" y="52" text-anchor="middle" font-size="14" font-weight="700" fill="${color}" font-family="var(--font-mono)">${val}${max===100?"%":""}</text>
    </svg>
    <div style="font-size:10px;font-weight:600;color:var(--text);margin-top:-6px">${label}</div>
    ${sublabel?`<div style="font-size:9px;color:var(--text3);font-family:var(--font-mono)">${sublabel}</div>`:""}
  </div>`;
}

// ── Render principal ────────────────────────────────────────────────────

function renderStats(){
  const el=document.getElementById("page-stats");
  if(!el)return;
  const st=computeStats();
  const s=st.serie;
  const last=s[s.length-1]||{};
  const hasHist=s.length>=2;

  // Sous-onglets
  const TABS=[
    {id:"sante",   label:"Santé"},
    {id:"signaux", label:"Signaux faibles"},
  ];
  let activeTab="sante";

  function renderTab(){
    const body=document.getElementById("stats-body");if(!body)return;
    if(activeTab==="sante") body.innerHTML=renderSante(st,s,last,hasHist);
    else if(activeTab==="signaux") body.innerHTML=renderSignaux(st,last);
  }

  el.innerHTML=`
    <div style="display:flex;gap:4px;margin-bottom:14px;border-bottom:1px solid var(--border);padding-bottom:0" id="stats-tabs">
      ${TABS.map(t=>`<div class="stat-tab${t.id===activeTab?" active":""}" data-tab="${t.id}"
        style="font-size:11px;padding:7px 14px;cursor:pointer;font-family:var(--font-mono);
        border-bottom:2px solid ${t.id===activeTab?"var(--cyan)":"transparent"};
        color:${t.id===activeTab?"var(--cyan)":"var(--text3)"};
        margin-bottom:-1px;transition:all .12s"
        onclick="switchStatTab('${t.id}')">${t.label}</div>`).join("")}
    </div>
    <div id="stats-body"></div>`;

  window.switchStatTab=function(tab){
    activeTab=tab;
    document.querySelectorAll(".stat-tab").forEach(t=>{
      const isActive=t.dataset.tab===tab;
      t.style.borderBottom=isActive?"2px solid var(--cyan)":"2px solid transparent";
      t.style.color=isActive?"var(--cyan)":"var(--text3)";
    });
    renderTab();
  };
  renderTab();
}

// ── Onglet Santé ────────────────────────────────────────────────────────

function buildRepDomaine(last){
  const P=last.projets||[];
  const byDom={};
  P.forEach(p=>{
    const d=p.domaine||"Autre";
    if(!byDom[d])byDom[d]={total:0,statuts:{}};
    byDom[d].total++;
    byDom[d].statuts[p.statut]=(byDom[d].statuts[p.statut]||0)+1;
  });
  const max=Math.max(...Object.values(byDom).map(d=>d.total),1);
  return Object.entries(byDom).sort((a,b)=>b[1].total-a[1].total).map(([d,data])=>{
    const pct=Math.round(data.total/max*100);
    const tags=Object.entries(data.statuts).sort((a,b)=>b[1]-a[1])
      .map(([st,n])=>`<span style="font-size:8px;padding:1px 5px;border-radius:8px;background:${SC[st]||"#475569"}22;color:${SC[st]||"#475569"};border:1px solid ${SC[st]||"#475569"}44">${st}:${n}</span>`).join(" ");
    return`<div style="margin-bottom:8px">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:3px">
        <span style="font-size:10px;min-width:100px;max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text2)" title="${esc(d)}">${esc(d)}</span>
        <div style="flex:1;height:5px;background:var(--bg4);border-radius:4px;overflow:hidden">
          <div style="width:${pct}%;height:100%;border-radius:4px;background:${domColor(d)}"></div>
        </div>
        <span style="font-size:10px;font-weight:600;min-width:20px;text-align:right;font-family:var(--font-mono);color:var(--text2)">${data.total}</span>
      </div>
      <div style="padding-left:106px;display:flex;gap:3px;flex-wrap:wrap">${tags}</div>
    </div>`;
  }).join("");
}

function buildRepCollab(last){
  const P=last.projets||[];
  const byCol={};
  P.forEach(p=>{
    const r=p.responsable_principal||"Non assigné";
    if(!byCol[r])byCol[r]={total:0,statuts:{}};
    byCol[r].total++;
    byCol[r].statuts[p.statut]=(byCol[r].statuts[p.statut]||0)+1;
  });
  const max=Math.max(...Object.values(byCol).map(d=>d.total),1);
  return Object.entries(byCol).sort((a,b)=>b[1].total-a[1].total).map(([r,data])=>{
    const pct=Math.round(data.total/max*100);
    const col=respColor(r);
    const tags=Object.entries(data.statuts).sort((a,b)=>b[1]-a[1])
      .map(([st,n])=>`<span style="font-size:8px;padding:1px 5px;border-radius:8px;background:${SC[st]||"#475569"}22;color:${SC[st]||"#475569"};border:1px solid ${SC[st]||"#475569"}44">${st}:${n}</span>`).join(" ");
    return`<div style="margin-bottom:8px">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:3px">
        <span style="font-size:10px;min-width:100px;max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text2)" title="${esc(r)}">${esc(r)}</span>
        <div style="flex:1;height:5px;background:var(--bg4);border-radius:4px;overflow:hidden">
          <div style="width:${pct}%;height:100%;border-radius:4px;background:${col}"></div>
        </div>
        <span style="font-size:10px;font-weight:600;min-width:20px;text-align:right;font-family:var(--font-mono);color:var(--text2)">${data.total}</span>
      </div>
      <div style="padding-left:106px;display:flex;gap:3px;flex-wrap:wrap">${tags}</div>
    </div>`;
  }).join("");
}

function renderSante(st,s,last,hasHist){
  const avMoy=last.avancement||0;
  const tauxDiff=last.tauxDiff||0;
  const nbBloc=last.nbBlocages||0;

  // Tendance avancement
  let tendAv="stable",tendCol="var(--text3)";
  if(s.length>=2){
    const delta=s[s.length-1].avancement-s[s.length-2].avancement;
    if(delta>2){tendAv=`+${delta.toFixed(1)}% vs Q-1`;tendCol="var(--green)";}
    else if(delta<-2){tendAv=`${delta.toFixed(1)}% vs Q-1`;tendCol="var(--red)";}
    else{tendAv=`stable vs Q-1`;tendCol="var(--text3)";}
  }

  return`
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:14px">

      ${jauge(tauxDiff,100,"var(--red)","Taux en difficulté",`${last.nbDiff||0} projets`)}
      ${jauge(nbBloc,Math.max(nbBloc,10),"var(--amber)","Blocages actifs","cette quinzaine")}
    </div>
    <div class="grid2">

      <div class="card">
        <div class="card-title">évolution taux en difficulté</div>
        ${hasHist
          ?svgChart(s,x=>x.tauxDiff,"var(--red)","difficulté","%")
          :`<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);padding:8px">// disponible dès 2 quinzaines</div>`}
      </div>
    </div>
    <div class="grid2">
      <div class="card">
        <div class="card-title">blocages & décisions par quinzaine</div>
        ${hasHist
          ?`<div style="margin-bottom:8px">`+svgChart(s,x=>x.nbBlocages,"var(--amber)","blocages","",120)+`</div>
            <div style="font-size:9px;color:var(--amber);font-family:var(--font-mono);margin-bottom:4px">▲ Blocages</div>
            <div>`+svgChart(s,x=>x.nbDecisions,"var(--green)","décisions","",120)+`</div>
            <div style="font-size:9px;color:var(--green);font-family:var(--font-mono)">▲ Décisions</div>`
          :`<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);padding:8px">// disponible dès 2 quinzaines</div>`}
      </div>
      <div class="card">
        <div class="card-title">nouveaux projets par quinzaine</div>
        ${st.serieNouveaux&&st.serieNouveaux.length>=2
          ?`<div style="margin-bottom:6px">`+
            svgChart(st.serieNouveaux,x=>x.nbNew,"var(--cyan)","nouveaux","",110)+
            `</div><div style="font-size:9px;color:var(--cyan);font-family:var(--font-mono);margin-bottom:10px">▲ Nouveaux projets par quinzaine</div>`+
            svgChart(st.serieNouveaux,x=>x.nbCumul,"var(--violet)","cumulés","",110)+
            `<div style="font-size:9px;color:var(--violet);font-family:var(--font-mono);margin-top:4px">▲ Projets cumulés (portefeuille total)</div>`
          :`<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);padding:8px">// disponible dès 2 quinzaines</div>`}
      </div>
    <div class="card">
        <div class="card-title" style="display:flex;align-items:center;justify-content:space-between">
          <span>répartition statuts — quinzaine courante</span>
          <div style="display:flex;gap:4px">
            <span class="fchip active" id="rep-toggle-dom" onclick="toggleRep('dom')" style="font-size:9px;padding:2px 8px">Par domaine</span>
            <span class="fchip" id="rep-toggle-col" onclick="toggleRep('col')" style="font-size:9px;padding:2px 8px">Par collaborateur</span>
          </div>
        </div>
        <div id="rep-content">
          ${buildRepDomaine(last)}
        </div>
      </div>
    </div>`;

  window.toggleRep=function(mode){
    document.getElementById("rep-toggle-dom")?.classList.toggle("active", mode==="dom");
    document.getElementById("rep-toggle-col")?.classList.toggle("active", mode==="col");
    const el=document.getElementById("rep-content");
    if(el) el.innerHTML=mode==="dom"?buildRepDomaine(last):buildRepCollab(last);
  };
}

// ── Onglet Vélocité ─────────────────────────────────────────────────────

function renderVelocite(st,s,last,hasHist){
  const velItems=Object.entries(st.velociteDomaine)
    .filter(([,v])=>v!=null)
    .map(([d,v])=>{return{label:d,val:Math.max(v,0),rawVal:v,unit:"%/Q",color:domColor(d)};  });

  const respItems=Object.entries(last.parResp||{})
    .map(([r,d])=>{return{label:r,val:d.total,unit:" proj"};  });

  return`
    <div class="card" style="margin-bottom:12px">
      <div class="card-title">vélocité par domaine (Δ avancement moyen / quinzaine)</div>
      ${velItems.length
        ?barChart(velItems,"var(--cyan)")
        :`<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);padding:8px">// disponible dès 2 quinzaines</div>`}
      ${velItems.length?`<div style="font-size:9px;color:var(--text3);font-family:var(--font-mono);margin-top:8px">
        Domaine le plus rapide : <span style="color:var(--green)">
        ${velItems.sort((a,b)=>b.val-a.val)[0]?.label||"—"}</span>
        · Domaine le plus lent : <span style="color:var(--amber)">
        ${velItems.sort((a,b)=>a.val-b.val)[0]?.label||"—"}</span>
      </div>`:"" }
    </div>
    <div class="grid2">
      <div class="card">
        <div class="card-title">charge par responsable</div>
        ${barChart(respItems,"var(--violet)")}
        ${st.concentrations.length
          ?`<div style="margin-top:10px;padding:8px;background:var(--amber-dim);border-radius:var(--radius);border:1px solid rgba(245,158,11,.2)">
              <div style="font-size:9px;color:var(--amber);font-family:var(--font-mono);font-weight:600;margin-bottom:4px">⚠ Concentration détectée</div>
              ${st.concentrations.map(c=>`<div style="font-size:10px;color:var(--text2)">${esc(c.resp)} porte ${c.pct}% des projets actifs</div>`).join("")}
            </div>`
          :`<div style="font-size:9px;color:var(--green);font-family:var(--font-mono);margin-top:8px">✓ Charge bien répartie</div>`}
      </div>
      <div class="card">
        <div class="card-title">évolution projets actifs</div>
        ${hasHist
          ?svgChart(s,x=>x.nbActifs,"var(--violet)","actifs","")
          :`<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);padding:8px">// disponible dès 2 quinzaines</div>`}
      </div>
    </div>`;
}

// ── Onglet Livraisons ───────────────────────────────────────────────────

function renderLivraisons(st,s,last,hasHist){
  const avecData=s.filter(x=>x.avecLivrable>0);
  const hasTaux=avecData.length>0;
  const dernierTaux=hasTaux?avecData[avecData.length-1].tauxLivre:null;

  return`
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:14px">
      ${jauge(dernierTaux!=null?dernierTaux:0,100,"var(--green)","Taux livré",dernierTaux!=null?`${last.livres||0} / ${last.avecLivrable||0}`:"données insuffisantes")}
      ${jauge(last.reportes||0,Math.max(last.avecLivrable||1,1),"var(--amber)","Reportés",`${last.reportes||0} livrable(s)`)}
      ${jauge(last.nonLivres||0,Math.max(last.avecLivrable||1,1),"var(--red)","Non livrés",`${last.nonLivres||0} livrable(s)`)}
    </div>
    <div class="card" style="margin-bottom:12px">
      <div class="card-title">évolution taux de livraison</div>
      ${hasTaux&&hasHist
        ?svgChart(avecData,x=>x.tauxLivre,"var(--green)","livraison","%")
        :`<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);padding:8px">
          // ${hasTaux?"disponible dès 2 quinzaines avec données":"livrable_statut non renseigné dans les fiches"}</div>`} }
    </div>
    <div class="card">
      <div class="card-title">répartition livrables — quinzaine courante</div>
      ${last.avecLivrable>0
        ?barChart([
            {label:"Livré",    val:last.livres||0,    unit:"",color:"var(--green)"},
            {label:"En cours", val:(last.avecLivrable-(last.livres||0)-(last.nonLivres||0)-(last.reportes||0)),unit:"",color:"var(--cyan)"},
            {label:"Non livré",val:last.nonLivres||0, unit:"",color:"var(--red)"},
            {label:"Reporté",  val:last.reportes||0,  unit:"",color:"var(--amber)"},
          ].filter(x=>x.val>0),"var(--green)")
        :`<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);padding:8px">
            // livrable_statut non renseigné — les données apparaîtront dès que les fiches sont remplies</div>`}
    </div>`;
}

// ── Onglet Signaux faibles ──────────────────────────────────────────────

function renderSignaux(st,last){
  const TYPE_SF={
    STAGNATION:{label:"Stagnation",     color:"var(--amber)",icon:"≈"},
    OSCILLATION:{label:"Instabilité",    color:"var(--violet)",icon:"~"},
    TRAINARD:   {label:"Cas persistant", color:"var(--red)",  icon:"!"},
  };

  const parType={};
  st.signauxFaibles.forEach(s=>{
    if(!parType[s.type])parType[s.type]=[];
    parType[s.type].push(s);
  });

  const hasSignaux=st.signauxFaibles.length>0;

  return`
    ${!hasSignaux?`
      <div class="card" style="text-align:center;padding:32px">
        <div style="font-size:24px;margin-bottom:8px">✓</div>
        <div style="font-size:13px;font-weight:600;color:var(--green);font-family:var(--font-mono)">Aucun signal faible détecté</div>
        <div style="font-size:10px;color:var(--text3);margin-top:6px;font-family:var(--font-mono)">// disponible dès 2+ quinzaines avec données</div>
      </div>`:""}
    ${Object.entries(parType).map(([type,items])=>{
      const cfg=TYPE_SF[type]||{};
      return`<div class="card" style="margin-bottom:10px">
        <div class="card-title" style="color:${cfg.color}">${cfg.icon} ${cfg.label} (${items.length})</div>
        <div class="proj-list">
          ${items.map(sig=>`<div class="proj-item" onclick="openModal('${esc(sig.pid)}')" style="flex-direction:column;align-items:flex-start;gap:3px">
            <div style="display:flex;align-items:center;gap:7px;width:100%">
              <span class="proj-dot" style="background:${cfg.color}"></span>
              <span class="proj-name" style="font-size:11px">${esc(sig.nom)}</span>
              ${badge(sig.statut)}
            </div>
            <div style="font-size:9px;color:var(--text3);font-family:var(--font-mono);padding-left:13px">${esc(sig.detail)}</div>
          </div>`).join("")}
        </div>
      </div>`;
    }).join("")}
    <div class="card">
      <div class="card-title">à propos des signaux faibles</div>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px">
        ${Object.entries(TYPE_SF).map(([,cfg])=>`
          <div style="background:var(--bg3);border-radius:var(--radius);padding:10px">
            <div style="font-size:11px;font-weight:600;color:${cfg.color};margin-bottom:4px">${cfg.icon} ${cfg.label}</div>
            <div style="font-size:9px;color:var(--text3);line-height:1.5">${
              cfg.label==="Stagnation"?"Δ avancement < 5% sur 2 quinzaines consécutives, projet non terminé":
              cfg.label==="Instabilité"?"Statut change ≥ 3 fois sur l'historique":
              "≥ 3 quinzaines consécutives À risque ou En retard"
            }</div>
          </div>`).join("")}
      </div>
    </div>`;
}



function renderEvolutions(){
  const delta=DATA.delta||[];
  const sub=DATA.q_prev?`delta :: ${DATA.q_prev} → ${DATA.quinzaine}`:`snapshot initial :: ${DATA.quinzaine}`;
  let html=`<div style="font-size:10px;color:var(--text3);margin-bottom:14px;font-family:var(--font-mono)"># ${sub}</div>`;
  html+=`<div class="card"><div class="card-title">changements détectés (${delta.length})</div>`;
  if(!delta.length){
    html+=`<div style="font-size:11px;color:var(--text3);padding:8px;font-family:var(--font-mono)">// ${DATA.q_prev?"aucun changement":"premier snapshot"}</div>`;
  } else {
    html+='<div>'+delta.map(d=>{
      const dv=d.delta_avancement||0;
      const sign=dv>0?"+":"";
      const dc=dv>0?"var(--green)":dv<0?"var(--red)":"var(--text3)";
      const vide=v=>!v||["nan","None","undefined",""].includes(String(v).trim());

      // Tag avancement
      const dvTag=dv!==0
        ?`<span style="font-size:9px;font-family:var(--font-mono);color:${dc}">${sign}${Math.round(dv)}%</span>`
        :"";

      // Tag statut
      const stTag=d.statut_avant&&d.statut_apres&&d.statut_avant!==d.statut_apres
        ?`<span style="font-size:9px;color:var(--text3);font-family:var(--font-mono)">${d.statut_avant} → ${d.statut_apres}</span>`
        :"";

      // Tags autres champs avec valeurs avant/après
         const LABELS={
          phase:               {label:"phase",    truncate:12},
          points_blocage:      {label:"blocage",  truncate:0},
          livrable_statut:     {label:"livrable", truncate:0},
          responsable_principal:{label:"resp.",   truncate:10},
        };
        const champs=Object.keys(LABELS).filter(c=>d[c+"_avant"]!==undefined||d[c+"_apres"]!==undefined);
        const autresTags=champs.map(c=>{
          const av=d[c+"_avant"], ap=d[c+"_apres"];
          const cfg=LABELS[c];
          const fmt=v=>cfg.truncate>0?String(v).trim().slice(0,cfg.truncate)+(String(v).trim().length>cfg.truncate?"…":""):String(v).trim();
        
          if(c==="points_blocage"){
            if(vide(av)&&!vide(ap)) return`<span style="font-size:9px;font-family:var(--font-mono);color:var(--red)">⚠ blocage</span>`;
            if(!vide(av)&&vide(ap)) return`<span style="font-size:9px;font-family:var(--font-mono);color:var(--green)">✓ blocage résolu</span>`;
            return"";
          }
          if(vide(av)&&!vide(ap))
            return`<span style="font-size:9px;font-family:var(--font-mono);color:var(--green)">+${cfg.label}: ${fmt(ap)}</span>`;
          if(!vide(av)&&vide(ap))
            return`<span style="font-size:9px;font-family:var(--font-mono);color:var(--red)">-${cfg.label}: ${fmt(av)}</span>`;
          if(!vide(av)&&!vide(ap)&&String(av).trim()!==String(ap).trim())
            return`<span style="font-size:9px;font-family:var(--font-mono);color:var(--amber)">${cfg.label}: ${fmt(av)} → ${fmt(ap)}</span>`;
          return"";
        }).filter(Boolean).join(" ");

      const dotCol=d.statut_avant!==d.statut_apres?(SC[d.statut_apres]||"var(--text3)"):dc||"var(--text3)";
      const meta=[stTag,dvTag,autresTags].filter(Boolean).join(" ");

      return`<div class="tl-item">
        <div class="tl-dot" style="background:${dotCol}"></div>
        <div class="tl-body">
          <div class="tl-title" onclick="openModal('${esc(d.projet_id||d.ref_sujet)}')">${esc(d.projet_nom||d.sujet||d.projet_id)}</div>
          <div class="tl-meta" style="display:flex;gap:6px;flex-wrap:wrap;align-items:center">
            ${meta}
            ${badge(d.statut_apres||"Stand by")}
          </div>
        </div>
      </div>`;
    }).join('')+'</div>';
  }
  html+='</div>';
  const alertes=DATA.projets.filter(p=>p.points_blocage||p.statut==="En retard");
  if(alertes.length)html+=`<div class="card"><div class="card-title">points d'attention (${alertes.length})</div><div class="proj-list">${alertes.map(projItem).join("")}</div></div>`;
  document.getElementById("page-evolutions").innerHTML=html;
}
function renderCalendrier(){
  const el=document.getElementById("page-calendrier");
  if(!el)return;
  const AGENDA=DATA.agenda||[];
  const TYPES_CAL={
    REUNION:  {label:"Réunion",   bg:"rgba(37,99,235,.12)",  border:"#2563eb",text:"#2563eb"},
    LIVRAISON:{label:"Livraison", bg:"rgba(5,150,105,.12)",  border:"#059669",text:"#059669"},
    ACTUALITE:{label:"Actualité", bg:"rgba(217,119,6,.12)",  border:"#d97706",text:"#d97706"},
    JALON:    {label:"Jalon",     bg:"rgba(124,58,237,.12)", border:"#7c3aed",text:"#7c3aed"},
    EVENEMENT:{label:"Évènement", bg:"rgba(244,63,94,.12)",  border:"#f43f5e",text:"#f43f5e"},
    AUTRE:    {label:"Autre",     bg:"rgba(107,104,96,.12)", border:"#6b6860",text:"#6b6860"},
  };
  const MFR_LONG=["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Août","Septembre","Octobre","Novembre","Décembre"];
  const MFR_SHORT=["Jan","Fév","Mar","Avr","Mai","Jun","Jul","Aoû","Sep","Oct","Nov","Déc"];
  const now=new Date();
  let curY=now.getFullYear(),curM=now.getMonth();
  let activeTypes=new Set(Object.keys(TYPES_CAL));
  let calView="day";
 
  function evtsForDate(y,m,d){
    const key=`${y}-${String(m+1).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
    return AGENDA.filter(e=>e.date===key&&activeTypes.has(e.type));
  }
 
  function openEvt(idx){
    const e=AGENDA[idx];if(!e)return;
    const t=TYPES_CAL[e.type]||TYPES_CAL.AUTRE;
    const dt=new Date(e.date);
    const dateStr=dt.toLocaleDateString("fr-FR",{weekday:"long",day:"numeric",month:"long",year:"numeric"});
    const projLie=e.projet_ref?(DATA.projets.find(p=>(p.projet_id||p.ref_sujet)===e.projet_ref)||null):null;
    document.getElementById("modal-body").innerHTML=`
      <button class="modal-close" onclick="closeModal()">x</button>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
        <div style="width:10px;height:10px;border-radius:50%;background:${t.border};flex-shrink:0"></div>
        <span class="badge" style="background:${t.bg};color:${t.text};border:1px solid ${t.border};font-family:var(--font-mono);font-size:9px">${t.label}</span>
      </div>
      <div class="modal-title">${esc(e.titre)}</div>
      <div class="modal-id" style="color:var(--text3)">${dateStr}</div>
      ${e.description&&e.description!=="nan"?`<div class="modal-sec"><div class="modal-stitle">description</div><div class="modal-text">${esc(e.description)}</div></div>`:""}
      ${projLie?`<div class="modal-sec"><div class="modal-stitle">projet lié</div>
        <div class="proj-item" onclick="closeModal();openModal('${esc(projLie.projet_id||projLie.ref_sujet)}')" style="cursor:pointer">
          <span class="proj-dot" style="background:${SC[projLie.statut]||'#475569'}"></span>
          <span class="proj-name">${esc(projLie.projet_nom||projLie.sujet||"")}</span>
          ${badge(projLie.statut)}
          <span class="proj-pct">${projLie.avancement_pct||0}%</span>
        </div></div>`:
        e.projet_ref&&e.projet_ref!=="nan"?`<div class="modal-sec"><div class="modal-stitle">projet lié</div>
          <div class="modal-text" style="color:var(--text3);font-family:var(--font-mono)">${esc(e.projet_ref)}</div></div>`:""}
    `;
    document.getElementById("modal-overlay").classList.add("open");
  }
 
  function buildDay(){
    const todayStr=`${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,"0")}-${String(now.getDate()).padStart(2,"0")}`;
    const first=new Date(curY,curM,1);
    const startDow=(first.getDay()+6)%7;
    const daysInMonth=new Date(curY,curM+1,0).getDate();
    const daysInPrev=new Date(curY,curM,0).getDate();
    const upcoming=AGENDA.filter(e=>e.date>=todayStr&&activeTypes.has(e.type))
      .sort((a,b)=>a.date.localeCompare(b.date)).slice(0,6);
    const titre=`<span style="cursor:pointer;border-bottom:1px dashed var(--border2)" onclick="calSwitchView('month')">${MFR_LONG[curM]}</span> <span style="cursor:pointer;border-bottom:1px dashed var(--border2)" onclick="calSwitchView('year')">${curY}</span>`;
    let html=`
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px">
        <button onclick="calNav(-1)" style="font-size:14px;padding:4px 14px;background:var(--bg2);border:1px solid var(--border2);border-radius:var(--radius);color:var(--text);cursor:pointer;font-family:var(--font-mono)">&#8249;</button>
        <span style="flex:1;text-align:center;font-size:14px;font-weight:600;font-family:var(--font-mono);color:var(--text)">${titre}</span>
        <button onclick="calNav(1)" style="font-size:14px;padding:4px 14px;background:var(--bg2);border:1px solid var(--border2);border-radius:var(--radius);color:var(--text);cursor:pointer;font-family:var(--font-mono)">&#8250;</button>
      </div>
      <div style="display:flex;gap:5px;flex-wrap:wrap;margin-bottom:12px" id="cal-type-filters">
        ${Object.entries(TYPES_CAL).map(([k,v])=>`
          <span class="fchip${activeTypes.has(k)?" active":""}"
            style="${activeTypes.has(k)?'color:'+v.text+';border-color:'+v.border+';background:'+v.bg:''}"
            onclick="calToggleType('${k}')" data-t="${k}">
            <span style="width:6px;height:6px;border-radius:50%;background:${v.border};display:inline-block;margin-right:4px"></span>${v.label}
          </span>`).join("")}
      </div>
      <div style="display:grid;grid-template-columns:1fr 260px;gap:12px">
        <div class="card" style="padding:10px">
          <div style="display:grid;grid-template-columns:repeat(7,1fr);gap:1px;background:var(--border);border-radius:6px;overflow:hidden">
            ${["Lun","Mar","Mer","Jeu","Ven","Sam","Dim"].map(d=>`
              <div style="background:var(--bg3);padding:5px;text-align:center;font-size:9px;font-weight:600;color:var(--text3);font-family:var(--font-mono)">${d}</div>`).join("")}`;
    for(let i=0;i<startDow;i++){
      html+=`<div style="background:var(--bg2);min-height:76px;padding:5px;opacity:.25">
        <div style="font-size:10px;color:var(--text3);font-family:var(--font-mono)">${daysInPrev-startDow+1+i}</div></div>`;
    }
    for(let d=1;d<=daysInMonth;d++){
      const isToday=d===now.getDate()&&curM===now.getMonth()&&curY===now.getFullYear();
      const evts=evtsForDate(curY,curM,d);
      const show=evts.slice(0,3);const more=evts.length-3;
      html+=`<div style="background:${isToday?'rgba(37,99,235,.05)':'var(--bg2)'};min-height:76px;padding:5px;border:${isToday?'1px solid rgba(37,99,235,.25)':'1px solid transparent'};transition:background .12s;cursor:${evts.length?'pointer':'default'}" ${evts.length?`onclick="calOpenDay(${curY},${curM},${d})"`:''}>`;
      html+=`<div style="font-size:10px;font-weight:600;margin-bottom:3px;font-family:var(--font-mono);color:${isToday?'var(--cyan)':'var(--text3)'}">
        ${isToday?`<span style="background:var(--cyan);color:var(--bg);border-radius:50%;width:17px;height:17px;display:inline-flex;align-items:center;justify-content:center;font-size:9px">${d}</span>`:d}</div>`;
      html+=show.map(e=>{const t=TYPES_CAL[e.type]||TYPES_CAL.AUTRE;const idx=AGENDA.indexOf(e);
        return`<div style="font-size:9px;padding:2px 5px;border-radius:3px;margin-bottom:2px;background:${t.bg};border-left:2px solid ${t.border};color:${t.text};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;cursor:pointer" onclick="event.stopPropagation();openEvt(${idx})" title="${esc(e.titre)}">${esc(e.titre)}</div>`;
      }).join("");
      if(more>0)html+=`<div style="font-size:8px;color:var(--text3);font-family:var(--font-mono);padding:1px 4px">+${more} autre${more>1?'s':''}</div>`;
      html+=`</div>`;
    }
    const total=startDow+daysInMonth;const rem=(7-total%7)%7;
    for(let i=1;i<=rem;i++){
      html+=`<div style="background:var(--bg2);min-height:76px;padding:5px;opacity:.25">
        <div style="font-size:10px;color:var(--text3);font-family:var(--font-mono)">${i}</div></div>`;
    }
    html+=`</div></div>
        <div style="display:flex;flex-direction:column;gap:10px">
          <div class="card">
            <div class="card-title">prochains événements</div>
            ${upcoming.length?upcoming.map(e=>{
              const dt=new Date(e.date);const t=TYPES_CAL[e.type]||TYPES_CAL.AUTRE;
              const idx=AGENDA.indexOf(e);
              const diffJ=Math.ceil((dt-now)/(1000*60*60*24));
              return`<div class="proj-item" style="cursor:pointer;flex-direction:column;align-items:flex-start;gap:4px" onclick="openEvt(${idx})">
                <div style="display:flex;align-items:center;gap:7px;width:100%">
                  <div style="min-width:30px;text-align:center;background:var(--bg3);border-radius:5px;padding:3px 0;flex-shrink:0">
                    <div style="font-size:12px;font-weight:600;color:var(--text);font-family:var(--font-mono);line-height:1">${dt.getDate()}</div>
                    <div style="font-size:8px;color:var(--text3);text-transform:uppercase">${MFR_SHORT[dt.getMonth()]}</div>
                  </div>
                  <span class="proj-name" style="font-size:11px">${esc(e.titre)}</span>
                  <span class="badge" style="background:${t.bg};color:${t.text};border:1px solid ${t.border};margin-left:auto;font-size:8px">${t.label}</span>
                </div>
                <div style="font-size:9px;color:var(--text3);font-family:var(--font-mono);padding-left:37px">
                  ${diffJ===0?"aujourd'hui":diffJ===1?"demain":"dans "+diffJ+" j"}
                  ${e.projet_ref&&e.projet_ref!=="nan"?" · "+e.projet_ref:""}
                </div>
              </div>`;
            }).join(""):`<div style="font-size:11px;color:var(--text3);padding:8px 0;font-family:var(--font-mono)">// aucun événement à venir</div>`}
          </div>
          <div class="card">
            <div class="card-title">légende</div>
            ${Object.entries(TYPES_CAL).map(([,v])=>
              `<div style="display:flex;align-items:center;gap:7px;margin-bottom:7px;font-size:11px;color:var(--text2)">
                <span style="width:8px;height:8px;border-radius:50%;background:${v.border};flex-shrink:0"></span>
                ${v.label}
              </div>`).join("")}
          </div>
        </div>
      </div>`;
    el.innerHTML=html;
  }
 
  function buildMonth(){
    const titre=`<span style="cursor:pointer;border-bottom:1px dashed var(--border2)" onclick="calSwitchView('year')">${curY}</span>`;
    let html=`
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
        <button onclick="calNav(-1)" style="font-size:14px;padding:4px 14px;background:var(--bg2);border:1px solid var(--border2);border-radius:var(--radius);color:var(--text);cursor:pointer;font-family:var(--font-mono)">&#8249;</button>
        <span style="flex:1;text-align:center;font-size:14px;font-weight:600;font-family:var(--font-mono);color:var(--text)">${titre}</span>
        <button onclick="calNav(1)" style="font-size:14px;padding:4px 14px;background:var(--bg2);border:1px solid var(--border2);border-radius:var(--radius);color:var(--text);cursor:pointer;font-family:var(--font-mono)">&#8250;</button>
      </div>
      <div class="card"><div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">`;
    for(let m=0;m<12;m++){
      const isCurrentM=m===curM&&curY===now.getFullYear();
      const isTodayM=m===now.getMonth()&&curY===now.getFullYear();
      const nbEvts=AGENDA.filter(e=>{const d=new Date(e.date);return d.getMonth()===m&&d.getFullYear()===curY&&activeTypes.has(e.type);}).length;
      html+=`<div onclick="calSelectMonth(${m})" style="padding:12px 8px;text-align:center;border-radius:var(--radius);cursor:pointer;background:${isCurrentM?'var(--cyan-dim)':'var(--bg3)'};border:1px solid ${isCurrentM?'var(--cyan)':isTodayM?'var(--border2)':'transparent'};transition:all .12s">
        <div style="font-size:12px;font-weight:${isCurrentM?'600':'400'};color:${isCurrentM?'var(--cyan)':'var(--text)'};font-family:var(--font-mono)">${MFR_SHORT[m]}</div>
        <div style="font-size:8px;color:var(--text3);margin-top:4px;font-family:var(--font-mono)">${nbEvts>0?nbEvts+" evt":"—"}</div>
      </div>`;
    }
    html+=`</div></div>`;
    el.innerHTML=html;
  }
 
  function buildYear(){
    const decBase=Math.floor(curY/10)*10;
    let html=`
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
        <button onclick="calNavDec(-1)" style="font-size:14px;padding:4px 14px;background:var(--bg2);border:1px solid var(--border2);border-radius:var(--radius);color:var(--text);cursor:pointer;font-family:var(--font-mono)">&#8249;</button>
        <span style="flex:1;text-align:center;font-size:13px;font-weight:600;font-family:var(--font-mono);color:var(--text3)">${decBase} — ${decBase+11}</span>
        <button onclick="calNavDec(1)" style="font-size:14px;padding:4px 14px;background:var(--bg2);border:1px solid var(--border2);border-radius:var(--radius);color:var(--text);cursor:pointer;font-family:var(--font-mono)">&#8250;</button>
      </div>
      <div class="card"><div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">`;
    for(let i=0;i<12;i++){
      const yr=decBase+i;
      const isCurY=yr===curY;
      const isTodayY=yr===now.getFullYear();
      const nbEvts=AGENDA.filter(e=>new Date(e.date).getFullYear()===yr&&activeTypes.has(e.type)).length;
      html+=`<div onclick="calSelectYear(${yr})" style="padding:14px 8px;text-align:center;border-radius:var(--radius);cursor:pointer;background:${isCurY?'var(--cyan-dim)':'var(--bg3)'};border:1px solid ${isCurY?'var(--cyan)':isTodayY?'var(--border2)':'transparent'};transition:all .12s">
        <div style="font-size:13px;font-weight:${isCurY?'600':'400'};color:${isCurY?'var(--cyan)':isTodayY?'var(--text)':'var(--text2)'};font-family:var(--font-mono)">${yr}</div>
        <div style="font-size:8px;color:var(--text3);margin-top:4px;font-family:var(--font-mono)">${nbEvts>0?nbEvts+" evt":"—"}</div>
      </div>`;
    }
    html+=`</div></div>`;
    el.innerHTML=html;
  }
 
  function calSwitchView(v){calView=v;buildCal();}
  window.calSwitchView=calSwitchView;
  window.calSelectMonth=function(m){curM=m;calView="day";buildCal();};
  window.calSelectYear=function(y){curY=y;calView="month";buildCal();};
 
  function buildCal(){
    if(calView==="month")buildMonth();
    else if(calView==="year")buildYear();
    else buildDay();
  }
 
  window.calNav=function(dir){
    if(calView==="day"){curM+=dir;if(curM>11){curM=0;curY++;}if(curM<0){curM=11;curY--;}}
    else if(calView==="month"){curY+=dir;}
    buildCal();
  };
  window.calNavDec=function(dir){curY+=dir*10;buildCal();};
 
  window.calOpenDay=function(y,m,d){
    const key=`${y}-${String(m+1).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
    const evts=AGENDA.filter(e=>e.date===key&&activeTypes.has(e.type));
    if(evts.length===1){openEvt(AGENDA.indexOf(evts[0]));return;}
    const dt=new Date(y,m,d);
    const dateStr=dt.toLocaleDateString("fr-FR",{weekday:"long",day:"numeric",month:"long"});
    document.getElementById("modal-body").innerHTML=`
      <button class="modal-close" onclick="closeModal()">x</button>
      <div class="modal-title">${dateStr}</div>
      <div class="modal-id">${evts.length} événement(s)</div>
      <div class="proj-list" style="margin-top:12px">
        ${evts.map(e=>{const t=TYPES_CAL[e.type]||TYPES_CAL.AUTRE;const idx=AGENDA.indexOf(e);
          return`<div class="proj-item" onclick="closeModal();setTimeout(()=>openEvt(${idx}),150)">
            <span class="proj-dot" style="background:${t.border}"></span>
            <span class="proj-name">${esc(e.titre)}</span>
            <span class="badge" style="background:${t.bg};color:${t.text};border:1px solid ${t.border};font-size:8px">${t.label}</span>
          </div>`;
        }).join("")}
      </div>`;
    document.getElementById("modal-overlay").classList.add("open");
  };
 
  window.calToggleType=function(t){
    if(activeTypes.has(t)){if(activeTypes.size>1)activeTypes.delete(t);}
    else activeTypes.add(t);
    document.querySelectorAll("#cal-type-filters .fchip").forEach(c=>{
      const tt=c.dataset.t;const v=TYPES_CAL[tt];
      c.className="fchip"+(activeTypes.has(tt)?" active":"");
      c.style.cssText=activeTypes.has(tt)?"color:"+v.text+";border-color:"+v.border+";background:"+v.bg:"";
    });
    buildDay();
  };
 
  window.openEvt=openEvt;
  buildCal();
}

function openModal(id){
  const p=DATA.projets.find(x=>pid(x)===id);if(!p)return;
  const hist=(DATA.historiques||{})[id]||[];
  const pv=pp(p);const col=SC[p.statut]||"#475569";
  const metaById={};(DATA.meta||[]).forEach(m=>{metaById[m.projet_id||m.ref_sujet]=m;});
  const m=metaById[id]||{};
  const descUnique = m.description && m.description !== "nan" ? m.description : (p.description && p.description !== "nan" ? p.description : "");
  const metaItems=[["ref/id",id],["domaine",p.domaine||m.domaine],["entite",p.entite_concerne||m.entite_concerne],["priorite",p.priorite||m.priorite],["budget j/sem",p.budget_jours||m.budget_jours],["date debut",p.date_debut||m.date_debut],["date fin prev.",p.date_fin||m.date_fin],["effectifs",p.effectifs||m.effectifs],["type",m.type]].filter(([,v])=>v&&v!=="undefined");
  document.getElementById("modal-body").innerHTML=`
    <button class="modal-close" onclick="closeModal()">x</button>
    <div class="modal-title">${esc(nom(p))}</div>
    <div class="modal-id">${esc(id)}</div>
    <div class="modal-row">${badge(p.statut)}${(p.domaine||m.domaine)?`<span class="badge bON_HOLD">${esc(p.domaine||m.domaine)}</span>`:""}${p.phase?`<span class="badge bON_HOLD">${esc(p.phase)}</span>`:""}${(p.priorite||m.priorite)?`<span class="badge bON_HOLD">prio:${esc(p.priorite||m.priorite)}</span>`:""}</div>
    <div class="modal-sec"><div class="modal-stitle">avancement — ${pv}%</div><div class="prog-track"><div class="prog-fill" style="width:${pv}%;background:${col}"></div></div></div>
    <div class="modal-sec"><div class="modal-stitle">informations projet</div><div class="meta-grid">${metaItems.map(([k,v])=>`<div class="meta-item"><div class="meta-key">${esc(k)}</div><div class="meta-val">${esc(v)}</div></div>`).join("")}</div></div>
    ${p.livrable_quinzaine?`<div class="modal-sec"><div class="modal-stitle">livrable quinzaine</div><div class="modal-text">${esc(p.livrable_quinzaine)} ${badge(p.livrable_statut||"Stand by")}</div></div>`:""}
    ${p.actions_realises?`<div class="modal-sec"><div class="modal-stitle">actions realisees</div><div class="modal-text">${esc(p.actions_realises)}</div></div>`:""}
    ${p.actions_a_mener?`<div class="modal-sec"><div class="modal-stitle">actions a mener</div><div class="modal-text">${esc(p.actions_a_mener)}${p.actions_echeance?`<br><span style="font-size:10px;color:var(--amber);font-family:var(--font-mono)">// echeance : ${esc(p.actions_echeance)}</span>`:""}</div></div>`:""}
    ${p.risques?`<div class="modal-sec"><div class="modal-stitle">risques</div><div class="modal-text" style="color:var(--amber)">${esc(p.risques)}${p.risque_niveau?` ${badge("À risque")}`:""} </div></div>`:""}
    ${p.points_blocage?`<div class="modal-sec"><div class="modal-stitle">blocages</div><div class="modal-text" style="color:var(--red)">${esc(p.points_blocage)}</div></div>`:""}
    ${(p.commentaire_libre||p.commentaire)?`<div class="modal-sec"><div class="modal-stitle">commentaire</div><div class="modal-text">${esc(p.commentaire_libre||p.commentaire)}</div></div>`:""}
    ${descUnique?`<div class="modal-sec"><div class="modal-stitle">description</div><div class="modal-text" style="color:var(--text3)">${esc(descUnique)}</div></div>`:""}
    ${hist.length>1?`<div class="modal-sec"><div class="modal-stitle">historique (${hist.length} quinzaines)</div>${hist.map(h=>`<div class="hist-row"><span class="hist-q">${esc(h.quinzaine)}</span>${badge(h.statut)}<span style="font-weight:600;font-family:var(--font-mono);font-size:10px">${h.avancement_pct||0}%</span><span style="color:var(--text3);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:10px">${esc(h.actions_realises||h.livrable_quinzaine||"")}</span></div>`).join("")}</div>`:""}`;
  document.getElementById("modal-overlay").classList.add("open");
}
function closeModal(){document.getElementById("modal-overlay").classList.remove("open");}

function askChat(q){document.getElementById("chat-input").value=q;sendChat();}
function getChatApiUrl(){return window.CHAT_API_URL||document.body.dataset.api||"";}

async function sendChat(){
  const input=document.getElementById("chat-input");const q=input.value.trim();if(!q)return;
  const msgs=document.getElementById("chat-msgs");const btn=document.querySelector(".chat-send");
  msgs.innerHTML+=`<div class="msg user"><div class="msg-av">TOI</div><div class="bubble">${esc(q)}</div></div>`;
  input.value="";btn.disabled=true;msgs.scrollTop=msgs.scrollHeight;
  const pid2=`msg-${Date.now()}`;
  msgs.innerHTML+=`<div class="msg" id="${pid2}"><div class="msg-av">IA</div><div class="bubble" style="color:var(--text3);font-family:var(--font-mono)">computing...</div></div>`;
  msgs.scrollTop=msgs.scrollHeight;
  const cacheKey=DATA.quinzaine+":"+q.toLowerCase().trim();
  const cached=LLM[cacheKey]||LLM[q.toLowerCase().trim()];
  if(cached){_rep(pid2,cached);btn.disabled=false;msgs.scrollTop=msgs.scrollHeight;return;}
  try{
    const resp=await fetch(getChatApiUrl()+"/api/chat",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({question:q,quinzaine:DATA.quinzaine}),signal:AbortSignal.timeout(30000)});
    if(!resp.ok)throw new Error("HTTP "+resp.status);
    const data=await resp.json();
    _rep(pid2,data.reponse||data.response||data.answer||JSON.stringify(data));
  }catch(err){
    const warn='<span style="font-size:9px;color:var(--amber);font-family:var(--font-mono);display:block;margin-bottom:6px">// API offline - reponse locale</span>';
    _rep(pid2,warn+repondreLocal(q));
  }
  btn.disabled=false;msgs.scrollTop=msgs.scrollHeight;
}
function _rep(id,t){const b=document.getElementById(id)?.querySelector(".bubble");if(b)b.innerHTML=String(t).replace(/\\n/g,"<br>");}

function repondreLocal(q){
  const ql=q.toLowerCase();const P=DATA.projets;const k=DATA.kpis;
  const snaps=DATA.snapshots||{};
  const quinzaines=Object.keys(snaps).sort();

  if(/synth|resume|bilan/.test(ql)){
    const bloqués=P.filter(p=>p.points_blocage&&p.points_blocage.trim());
    const retard=P.filter(p=>p.statut==="En retard");
    const actions=P.filter(p=>p.actions_a_mener&&p.actions_a_mener.trim());
    return "Synthèse "+DATA.quinzaine+"\\n"
      +"- "+k.nb_projets_actifs+" projets actifs · avancement moyen : "+k.avancement_moyen+"%\\n"
      +"- "+retard.length+" en retard · "+(k.nb_at_risk||0)+" à risque · "+bloqués.length+" bloqués"
      +(retard.length?"\\n\\nEn retard :\\n"+retard.map(p=>"  · "+nom(p)+" ("+(p.responsable_principal||"?")+")"+(p.points_blocage?" ⚠ "+p.points_blocage:"")).join("\\n"):"")
      +(bloqués.length?"\\n\\nBlocages :\\n"+bloqués.map(p=>"  · "+nom(p)+" :: "+p.points_blocage).join("\\n"):"")
      +(actions.length?"\\n\\nActions à mener :\\n"+actions.slice(0,5).map(p=>"  · "+nom(p)+" : "+p.actions_a_mener.slice(0,80)).join("\\n"):"");
  }

  if(/compar|évolution|evolution|progression/.test(ql)){
    if(quinzaines.length<2)return "// Une seule quinzaine disponible.";
    const qA=snaps[quinzaines[quinzaines.length-2]];
    const qB=snaps[quinzaines[quinzaines.length-1]];
    const avA=qA.kpis?.avancement_moyen||0;
    const avB=qB.kpis?.avancement_moyen||0;
    const deltaAv=(avB-avA).toFixed(1);
    const retA=qA.kpis?.nb_en_retard||0,retB=qB.kpis?.nb_en_retard||0;
    const blocA=qA.kpis?.nb_blocages||0,blocB=qB.kpis?.nb_blocages||0;
    const pA={};(qA.projets||[]).forEach(p=>pA[pid(p)]=p);
    const changes=(qB.projets||[]).filter(p=>{const pa=pA[pid(p)];return pa&&pa.statut!==p.statut;});
    return "Comparaison "+quinzaines[quinzaines.length-2]+" → "+quinzaines[quinzaines.length-1]+"\\n"
      +"- Avancement : "+avA+"% → "+avB+"% ("+(deltaAv>0?"+":"")+deltaAv+"%)\\n"
      +"- En retard  : "+retA+" → "+retB+" ("+(retB-retA>=0?"+":"")+(retB-retA)+")\\n"
      +"- Blocages   : "+blocA+" → "+blocB+" ("+(blocB-blocA>=0?"+":"")+(blocB-blocA)+")"
      +(changes.length?"\\n\\nChangements de statut :\\n"+changes.map(p=>"  · "+nom(p)+" : "+pA[pid(p)].statut+" → "+p.statut).join("\\n"):"");
  }

  if(/tendance|historique|trend/.test(ql)){
    if(quinzaines.length<2)return "// Données insuffisantes pour une tendance.";
    const serie=quinzaines.map(q=>({q,av:snaps[q].kpis?.avancement_moyen||0,ret:snaps[q].kpis?.nb_en_retard||0}));
    const debut=serie[0],fin=serie[serie.length-1];
    const deltaAv=(fin.av-debut.av).toFixed(1);
    return "Tendance sur "+quinzaines.length+" quinzaines ("+debut.q+" → "+fin.q+")\\n"
      +"- Avancement : "+debut.av+"% → "+fin.av+"% ("+(deltaAv>0?"+":"")+deltaAv+"%)\\n"
      +"- En retard  : "+debut.ret+" → "+fin.ret+"\\n\\nDétail :\\n"
      +serie.map(s=>"  "+s.q+" : "+s.av+"% · "+s.ret+" retard").join("\\n");
  }

  if(/retard|late/.test(ql)){const r=P.filter(p=>p.statut==="En retard");return r.length?r.length+" projet(s) en retard :\\n"+r.map(p=>"- "+nom(p)+" ("+(p.responsable_principal||"?")+")"+(p.points_blocage?" :: "+p.points_blocage:"")).join("\\n"):"Aucun projet en retard sur "+DATA.quinzaine+".";}
  if(/risque|at_risk/.test(ql)){const r=P.filter(p=>p.statut==="À risque");return r.length?r.length+" projet(s) à risque :\\n"+r.map(p=>"- "+nom(p)+" :: "+(p.risques||"non précisé")).join("\\n"):"Aucun projet à risque.";}
  if(/blocage|bloqu/.test(ql)){const r=P.filter(p=>p.points_blocage&&p.points_blocage.trim());return r.length?"Blocages actifs :\\n"+r.map(p=>"- "+nom(p)+" :: "+p.points_blocage).join("\\n"):"Aucun blocage signalé.";}
  if(/priorit|action/.test(ql)){const r=P.filter(p=>p.actions_a_mener&&p.actions_a_mener.trim());return r.length?"Actions à mener :\\n"+r.map(p=>"- "+nom(p)+" : "+p.actions_a_mener+(p.actions_echeance?" ("+p.actions_echeance+")":"")).join("\\n"):"Aucune action enregistrée.";}
  if(/échéance|fin|deadline/.test(ql)){const mbi={};(DATA.meta||[]).forEach(m=>{mbi[m.projet_id||m.ref_sujet]=m;});const r=P.filter(p=>{const m=mbi[pid(p)];return m&&m.date_fin;});return r.length?"Échéances :\\n"+r.map(p=>{const m=mbi[pid(p)];return"- "+nom(p)+" → "+m.date_fin;}).join("\\n"):"Aucune date de fin dans META.";}
  if(/décision|decision/.test(ql)){const r=P.filter(p=>p.decisions&&p.decisions.trim());return r.length?"Décisions sur "+DATA.quinzaine+" :\\n"+r.map(p=>"- "+nom(p)+" : "+p.decisions).join("\\n"):"Aucune décision enregistrée.";}
  const match=P.find(p=>{const n=nom(p).toLowerCase();return n&&ql.includes(n);});
  if(match){const mbi={};(DATA.meta||[]).forEach(m=>{mbi[m.projet_id||m.ref_sujet]=m;});const m=mbi[pid(match)]||{};return nom(match)+":\\n- Statut : "+match.statut+" · "+pp(match)+"%\\n- Responsable : "+(match.responsable_principal||"?")+"\\n"+(m.date_fin?"- Fin prévue : "+m.date_fin+"\\n":"")+(match.risques?"- Risques : "+match.risques+"\\n":"")+(match.points_blocage?"- Blocages : "+match.points_blocage+"\\n":"");}
  return"// "+DATA.quinzaine+"\\n- actifs : "+k.nb_projets_actifs+" | retard : "+k.nb_en_retard+" | risque : "+(k.nb_at_risk||0)+"\\n- avancement moyen : "+k.avancement_moyen+"%\\n- blocages : "+k.nb_blocages+"\\n(Connecte l'API pour des réponses enrichies)";
}

function toggleTheme(){
  const dark= document.body.classList.toggle('dark');
  const b=  document.getElementById('btn-theme')
  if(b) b.textContent = dark ?  '☀️' :'🌙';
  localStorage.setItem('theme', dark ? 'dark' : 'light');
  }

(function(){
  if(localStorage.getItem('theme')==='dark'){
  document.body.classList.add('dark');}

  document.addEventListener('DOMContentLoaded', function(){
  const b= document.getElementById('btn-theme');
   if(b) b.textContent= document.body.classList.contains('dark') ? '☀️':'🌙' ;
  });
  })();

"""


# ── HTML ──────────────────────────────────────────────────────────────────────

def generer_html(donnees, llm_cache=None):
    data_js = json.dumps(donnees, ensure_ascii=False)
    llm_js  = json.dumps(llm_cache or {}, ensure_ascii=False)
    q_label = donnees.get("quinzaine", "")

    head = (
        '<!DOCTYPE html>\n<html lang="fr">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width,initial-scale=1.0">\n'
        f'<title>MONITORING:: {q_label}</title>\n'
        '<style>' + CSS + '</style>\n</head>\n<body>'
    )

    html = ""
    html += '<div class="shell">'
    html += '<aside class="sidebar">'
    html += '<div class="logo">'
    html += '<div class="logo-title">Equipe data science</div>'
    html += '<div class="logo-sub">Outil de monitoring</div>'
    html += '<div class="logo-date" id="logo-date"></div>'
    html += '</div>'
    html += '<div class="q-selector-wrap">'
    html += '<div class="q-selector-label">Quinzaine</div>'
    html += '<select class="q-selector" id="q-selector" onchange="switchQuinzaine(this.value)"></select>'
    html += '</div>'
    html += '<div class="nav-section">Navigation</div>'
    html += '<div class="nav-item active" data-page="overview"><span class="nav-icon">&#9672;</span>Vue d\'ensemble<span class="nav-badge" id="nb-overview">&#8212;</span></div>'
    html += '<div class="nav-item" data-page="domaines"><span class="nav-icon">&#11041;</span>Par domaine</div>'
    html += '<div class="nav-item" data-page="collabs"><span class="nav-icon">&#9678;</span>Collaborateurs</div>'
    html += '<div class="nav-item" data-page="gantt"><span class="nav-icon">&#9636;</span>Roadmap Gantt</div>'
    html += '<div class="nav-item" data-page="evolutions"><span class="nav-icon">&#9651;</span>Evolutions<span class="nav-badge" id="nb-evol">&#8212;</span></div>'
    html += '<div class="nav-item" data-page="calendrier"><span class="nav-icon">▦</span>Calendrier<span class="nav-badge" id="nb-cal">—</span></div>'
    html += '<div class="nav-item" data-page="stats"><span class="nav-icon">◉</span>Statistiques</div>'
    html += '<div class="nav-section">Outils</div>'
    html += '<div class="nav-item" data-page="chat"><span class="nav-icon">&#8984;</span>Chat</div>'
    html += '<div class="sidebar-footer" id="sidebar-footer"></div>'
    html += '</aside>'
    html += '<div class="main">'
    html += '<div class="topbar">'
    html += '<span class="page-title" id="page-title">overview</span>'
    html += '<span class="snap-info" id="snap-info"></span>'
    html += '<div class="spacer"></div>'
    html += '<span class="gen-at" id="gen-at"></span>'
    html += '<button class="btn-theme" id="btn-theme" onclick="toggleTheme()">🌙️</button>'
    html += '</div>'
    html += '<div class="content">'
    html += '<div class="page active" id="page-overview"></div>'
    html += '<div class="page" id="page-domaines"></div>'
    html += '<div class="page" id="page-collabs"></div>'
    html += '<div class="page" id="page-gantt"></div>'
    html += '<div class="page" id="page-evolutions"></div>'
    html += '<div class="page" id="page-stats"></div>'
    html += '<div class="page" id="page-calendrier"></div>'
    html += '<div class="page" id="page-chat">'
    html += '<div class="chat-wrap">'
    html += '<div class="chat-header">&gt; assistant_llm :: quinzaine=<span id="chat-q-label"></span></div>'
    html += '<div class="chat-qs" id="chat-qs"></div>'
    html += '<div class="chat-msgs" id="chat-msgs">'
    html += '<div class="msg">'
    html += '<div class="msg-av">IA</div>'
    html += '<div class="bubble">Connecte au serveur puis pose ta question sur les projets de la quinzaine <strong id="chat-q-label2"></strong>.\n\nUtilise les suggestions ou tape ta propre question.</div>'
    html += '</div>'
    html += '</div>'
    html += '<div class="chat-bar">'
    html += '<textarea class="chat-input" id="chat-input" rows="2" placeholder="$ query --question \'...\'" onkeydown="if(event.key===\'Enter\'&&!event.shiftKey){event.preventDefault();sendChat();}"></textarea>'
    html += '<button class="chat-send" onclick="sendChat()">ENVOYER</button>'
    html += '</div>'
    html += '</div>'
    html += '</div>'
    html += '</div>'
    html += '</div>'
    html += '</div>'
    html += '</div>'
    html += '<div class="modal-overlay" id="modal-overlay"><div class="modal" id="modal-body"></div></div>'
    html += '\n<script>\n'
    html += 'const DATA=' + data_js + ';\n'
    html += 'const LLM=' + llm_js + ';\n'
    html += SCRIPT
    html += '\n</script>\n</body>\n</html>'

    return head + html


# ── Entrypoint ────────────────────────────────────────────────────────────────

def generer_dashboard(config_path="config.yaml", quinzaine=None, llm_reponses=None, output=None):
    import yaml
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) if Path(config_path).exists() else {}
    chemin_out = output or cfg.get("paths", {}).get("dashboard_out", "frontend/dashboard.html")
    try:
        from storage.storage import StorageManager as SM
    except ImportError:
        from storage import StorageManager as SM  # type: ignore
    sm = SM(config_path)
    donnees = preparer_donnees(sm, quinzaine)
    if not donnees:
        return None
    chemin = Path(chemin_out)
    chemin.parent.mkdir(parents=True, exist_ok=True)
    chemin.write_text(generer_html(donnees, llm_reponses or {}), encoding="utf-8")
    log.info(f"Dashboard -> {chemin}")
    print(f"\nOuvre : {chemin.resolve()}\n")
    return str(chemin.resolve())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quinzaine", default=None, help="Ex: T1_2026_R1")
    parser.add_argument("--output",    default=None)
    parser.add_argument("--config",    default="config.yaml")
    parser.add_argument("--llm",       action="store_true")
    args = parser.parse_args()
    llm_cache = {}
    if args.llm:
        try:
            from rag_engine import enrichir_html_generator
            llm_cache = enrichir_html_generator(args.config, quinzaine=args.quinzaine)
        except ImportError:
            try:
                from query.rag_engine import enrichir_html_generator
                llm_cache = enrichir_html_generator(args.config, quinzaine=args.quinzaine)
            except Exception as e:
                log.warning(f"LLM indisponible : {e}")
    generer_dashboard(config_path=args.config, quinzaine=args.quinzaine,
                      llm_reponses=llm_cache, output=args.output)


if __name__ == "__main__":
    main()