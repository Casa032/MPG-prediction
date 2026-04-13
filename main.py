"""
api/main.py
===========
Serveur FastAPI — pont entre le dashboard HTML et les données Parquet.

Lancement :
    uvicorn api.main:app --reload --port 8000

Endpoints disponibles :
    GET  /api/quinzaines          → liste des quinzaines stockées
    GET  /api/kpis                → KPIs du dashboard
    GET  /api/projets             → liste des projets d'une quinzaine
    GET  /api/projet/{projet_id}  → historique complet d'un projet
    GET  /api/delta               → comparaison deux quinzaines
    POST /api/chat                → question en langage naturel → LLM
    GET  /api/rapport             → génère un rapport HTML
    POST /api/rapport/pdf         → exporte en PDF (brique 8)
"""

import sys
import logging
from pathlib import Path

# Ajouter la racine du projet au path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
import pandas as pd

from storage.storage import StorageManager

# ── Config logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── App FastAPI ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Project Intelligence API",
    description="API de monitoring de projets — données Parquet + LLM",
    version="1.0.0",
)

# CORS — autorise le dashboard HTML à appeler l'API
# En production, restreindre à l'IP du serveur qui sert le HTML
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # ← restreindre en prod : ["http://mon-serveur"]
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instancier le StorageManager une seule fois au démarrage
sm = StorageManager()


# ── Modèles Pydantic ───────────────────────────────────────────────────────────

class QuestionChat(BaseModel):
    """Corps de la requête POST /api/chat"""
    question: str
    quinzaine: str | None = None   # si None : utilise la dernière quinzaine


# ── Utilitaires ────────────────────────────────────────────────────────────────

def df_to_records(df: pd.DataFrame) -> list:
    """Convertit un DataFrame en liste de dicts JSON-sérialisable."""
    if df.empty:
        return []
    # Remplacer NaN par None pour la sérialisation JSON
    return df.where(pd.notna(df), None).to_dict(orient="records")


def _get_quinzaine(quinzaine: str | None) -> str:
    """Retourne la quinzaine demandée ou la dernière disponible."""
    if quinzaine:
        return quinzaine
    quinzaines = sm.lister_quinzaines()
    if not quinzaines:
        raise HTTPException(status_code=404, detail="Aucune donnée en base — lance run_pipeline.py")
    return quinzaines[-1]


# ── ENDPOINTS ──────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    """Health check — vérifie que l'API tourne."""
    return {
        "status": "ok",
        "app": "Project Intelligence API",
        "docs": "/docs",
        "infos_stockage": sm.infos(),
    }


@app.get("/api/quinzaines")
def get_quinzaines():
    """
    Liste toutes les quinzaines disponibles en base, triées.

    Réponse : ["Q1_2025_S1", "Q1_2025_S2", ...]
    """
    quinzaines = sm.lister_quinzaines()
    if not quinzaines:
        return []
    return quinzaines


@app.get("/api/kpis")
def get_kpis(quinzaine: str | None = Query(default=None)):
    """
    KPIs principaux pour le dashboard.

    Paramètre optionnel : ?quinzaine=Q1_2025_S2
    Si absent : utilise la dernière quinzaine.

    Réponse :
        {
            "quinzaine": "Q1_2025_S2",
            "nb_projets_actifs": 5,
            "nb_en_retard": 1,
            "nb_at_risk": 2,
            "nb_done": 1,
            "nb_on_hold": 0,
            "avancement_moyen": 64.5,
            "nb_decisions": 8,
            "nb_blocages": 2
        }
    """
    q = _get_quinzaine(quinzaine)
    kpis = sm.kpis(quinzaine=q)
    if not kpis:
        raise HTTPException(status_code=404, detail=f"Aucune donnée pour la quinzaine '{q}'")
    return kpis


@app.get("/api/projets")
def get_projets(quinzaine: str | None = Query(default=None)):
    """
    Liste des projets pour une quinzaine donnée.

    Paramètre optionnel : ?quinzaine=Q1_2025_S2
    Si absent : utilise la dernière quinzaine.

    Réponse : liste de projets avec tous leurs champs.
    """
    q = _get_quinzaine(quinzaine)
    df = sm.charger_quinzaines(quinzaines=[q])
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Quinzaine '{q}' introuvable")
    return df_to_records(df)


@app.get("/api/projet/{projet_id}")
def get_projet(projet_id: str):
    """
    Historique complet d'un projet sur toutes les quinzaines.

    Réponse : liste de lignes triées chronologiquement.
    """
    df = sm.projet(projet_id)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"Projet '{projet_id}' introuvable")
    return {
        "projet_id":   projet_id,
        "projet_nom":  df["projet_nom"].iloc[0] if "projet_nom" in df.columns else "",
        "quinzaines":  df_to_records(df),
        "nb_quinzaines": len(df),
    }


@app.get("/api/delta")
def get_delta(
    q_avant: str = Query(..., description="Quinzaine de référence ex: Q1_2025_S1"),
    q_apres: str = Query(..., description="Quinzaine à comparer ex: Q1_2025_S2"),
):
    """
    Compare deux quinzaines — retourne les évolutions par projet.

    Paramètres obligatoires : ?q_avant=Q1_2025_S1&q_apres=Q1_2025_S2

    Réponse : liste avec statut_avant, statut_apres, delta_avancement.
    """
    df = sm.delta_quinzaines(q_avant, q_apres)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Impossible de comparer '{q_avant}' et '{q_apres}' — données manquantes"
        )
    return df_to_records(df)


@app.get("/api/statuts")
def get_statuts(quinzaine: str | None = Query(default=None)):
    """
    Répartition des projets par statut pour une quinzaine.

    Réponse : {"ON_TRACK": 5, "AT_RISK": 2, "LATE": 1, ...}
    """
    q = _get_quinzaine(quinzaine)
    return sm.projets_par_statut(quinzaine=q)


@app.get("/api/meta")
def get_meta():
    """
    Référentiel META des projets (sheet META du fichier Excel).

    Réponse : liste de tous les projets avec leur description.
    """
    df = sm.charger_meta()
    if df.empty:
        return []
    return df_to_records(df)


# ── CHAT LLM ──────────────────────────────────────────────────────────────────

@app.post("/api/chat")
def post_chat(body: QuestionChat):
    """
    Répond à une question en langage naturel sur les projets.

    Corps :
        { "question": "Quels projets sont en retard ?", "quinzaine": "Q1_2025_S2" }

    Réponse :
        { "reponse": "...", "sources": [...], "quinzaine": "Q1_2025_S2" }

    Note : connecter ici ton LLM interne via le rag_engine (brique 6).
    En attendant, retourne une réponse basée sur les données Parquet directement.
    """
    q = body.quinzaine or _get_quinzaine(None)
    question = body.question.lower()

    # ── Réponses directes depuis Parquet (sans LLM) ──────────────────────────
    # Ces règles simples fonctionnent immédiatement sans LLM.
    # Remplacer par rag_engine.query(body.question) quand la brique 6 est prête.

    df = sm.charger_quinzaines(quinzaines=[q])
    if df.empty:
        return {"reponse": f"Aucune donnée pour la quinzaine {q}.", "sources": [], "quinzaine": q}

    reponse = ""
    sources = []

    # Projets en retard
    if any(mot in question for mot in ["retard", "late", "retardé"]):
        late = df[df["statut"] == "LATE"][["projet_id", "projet_nom", "responsable_principal", "avancement_pct"]]
        if late.empty:
            reponse = f"Aucun projet en retard sur la quinzaine {q}."
        else:
            lignes = "\n".join(f"- {r.projet_nom} ({r.responsable_principal}) — {r.avancement_pct}%"
                               for r in late.itertuples())
            reponse = f"{len(late)} projet(s) en retard sur {q} :\n{lignes}"
            sources = late["projet_id"].tolist()

    # Projets à risque
    elif any(mot in question for mot in ["risque", "at_risk", "risqué"]):
        risk = df[df["statut"] == "AT_RISK"][["projet_id", "projet_nom", "responsable_principal", "risques"]]
        if risk.empty:
            reponse = f"Aucun projet à risque sur {q}."
        else:
            lignes = "\n".join(f"- {r.projet_nom} ({r.responsable_principal}) : {r.risques or 'non précisé'}"
                               for r in risk.itertuples())
            reponse = f"{len(risk)} projet(s) à risque sur {q} :\n{lignes}"
            sources = risk["projet_id"].tolist()

    # Décisions prises
    elif any(mot in question for mot in ["décision", "decision", "décidé"]):
        dec = df[df["decisions"].str.strip() != ""][["projet_id", "projet_nom", "decisions"]]
        if dec.empty:
            reponse = f"Aucune décision enregistrée sur {q}."
        else:
            lignes = "\n".join(f"- {r.projet_nom} : {r.decisions}" for r in dec.itertuples())
            reponse = f"Décisions prises sur {q} :\n{lignes}"
            sources = dec["projet_id"].tolist()

    # Blocages
    elif any(mot in question for mot in ["bloqué", "blocage", "bloquer"]):
        bloc = df[df["points_blocage"].str.strip() != ""][["projet_id", "projet_nom", "points_blocage"]]
        if bloc.empty:
            reponse = f"Aucun blocage signalé sur {q}."
        else:
            lignes = "\n".join(f"- {r.projet_nom} : {r.points_blocage}" for r in bloc.itertuples())
            reponse = f"Blocages actifs sur {q} :\n{lignes}"
            sources = bloc["projet_id"].tolist()

    # Avancement d'un projet spécifique
    elif any(mot in question for mot in ["avancement", "progression", "avancé"]):
        kpis = sm.kpis(quinzaine=q)
        reponse = (f"Sur la quinzaine {q} :\n"
                   f"- Avancement moyen : {kpis.get('avancement_moyen', '?')}%\n"
                   f"- Projets actifs : {kpis.get('nb_projets_actifs', '?')}\n"
                   f"- Terminés : {kpis.get('nb_done', '?')}")

    # Résumé général
    else:
        kpis = sm.kpis(quinzaine=q)
        reponse = (
            f"Résumé de la quinzaine {q} :\n"
            f"- {kpis.get('nb_projets_actifs', '?')} projet(s) actif(s)\n"
            f"- {kpis.get('nb_en_retard', '?')} en retard · {kpis.get('nb_at_risk', '?')} à risque\n"
            f"- Avancement moyen : {kpis.get('avancement_moyen', '?')}%\n"
            f"- {kpis.get('nb_decisions', '?')} décision(s) enregistrée(s)\n\n"
            f"Pose une question plus précise (ex: 'projets en retard', 'décisions prises', 'blocages')."
        )

    # ── TODO : brancher le LLM interne ici ──────────────────────────────────
    # from query.rag_engine import RagEngine
    # rag = RagEngine()
    # reponse = rag.query(body.question, contexte=df.to_string())

    return {"reponse": reponse, "sources": sources, "quinzaine": q}


# ── RAPPORTS ──────────────────────────────────────────────────────────────────

@app.get("/api/rapport")
def get_rapport(
    type: str = Query(default="quinzaine", description="quinzaine | projet | delta"),
    quinzaine: str | None = Query(default=None),
    projet_id: str | None = Query(default=None),
    q_avant: str | None = Query(default=None),
    q_apres: str | None = Query(default=None),
):
    """
    Génère un rapport HTML prêt à afficher dans le dashboard.

    Types disponibles :
        quinzaine : résumé de toute une quinzaine
        projet    : historique d'un projet (?projet_id=PROJ-001)
        delta     : comparaison deux quinzaines (?q_avant=...&q_apres=...)

    Réponse : { "html": "<h2>...</h2>..." }
    """
    if type == "quinzaine":
        q = _get_quinzaine(quinzaine)
        df = sm.charger_quinzaines(quinzaines=[q])
        kpis = sm.kpis(quinzaine=q)
        html = _html_rapport_quinzaine(q, df, kpis)

    elif type == "projet":
        if not projet_id:
            raise HTTPException(status_code=400, detail="Paramètre projet_id requis")
        df = sm.projet(projet_id)
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Projet '{projet_id}' introuvable")
        html = _html_rapport_projet(projet_id, df)

    elif type == "delta":
        if not q_avant or not q_apres:
            raise HTTPException(status_code=400, detail="Paramètres q_avant et q_apres requis")
        df = sm.delta_quinzaines(q_avant, q_apres)
        html = _html_rapport_delta(q_avant, q_apres, df)

    else:
        raise HTTPException(status_code=400, detail=f"Type '{type}' inconnu")

    return {"html": html}


@app.post("/api/rapport/pdf")
def post_rapport_pdf(
    type: str = Query(default="quinzaine"),
    quinzaine: str | None = Query(default=None),
):
    """
    Génère et retourne un PDF du rapport.
    Nécessite WeasyPrint installé (brique 8).

    TODO : implémenter pdf_builder.py (brique 8)
    """
    # ── TODO : brancher pdf_builder ──────────────────────────────────────────
    # from reporting.pdf_builder import generer_pdf
    # chemin_pdf = generer_pdf(type=type, quinzaine=quinzaine)
    # return FileResponse(chemin_pdf, media_type="application/pdf")
    raise HTTPException(
        status_code=501,
        detail="Export PDF non encore implémenté — brique 8 (pdf_builder.py) à coder"
    )


# ── Générateurs HTML pour les rapports ────────────────────────────────────────

def _html_rapport_quinzaine(quinzaine: str, df: pd.DataFrame, kpis: dict) -> str:
    """Génère le HTML d'un rapport de quinzaine."""
    if df.empty:
        return f"<p>Aucune donnée pour {quinzaine}</p>"

    couleurs = {
        "ON_TRACK": "#1abc9c", "AT_RISK": "#f59e0b",
        "LATE": "#ef4444", "DONE": "#6366f1", "ON_HOLD": "#6b7280",
    }

    lignes_projets = ""
    for _, row in df.iterrows():
        statut = row.get("statut", "")
        couleur = couleurs.get(statut, "#6b7280")
        pct = row.get("avancement_pct", 0)
        lignes_projets += f"""
        <tr>
          <td style="font-weight:500;color:#e8eaf0">{row.get('projet_id','')}</td>
          <td>{row.get('projet_nom','')}</td>
          <td>{row.get('responsable_principal','')}</td>
          <td><span style="color:{couleur};font-size:.75rem;padding:2px 8px;border:1px solid {couleur}33;
              background:{couleur}14;border-radius:4px">{statut}</span></td>
          <td style="text-align:right">{pct}%</td>
          <td style="color:#6b7280;font-size:.8rem">{row.get('decisions','') or '—'}</td>
        </tr>"""

    return f"""
    <h2 style="font-family:'Syne',sans-serif;color:#1abc9c;margin-bottom:16px">
      Rapport Quinzaine — {quinzaine}
    </h2>
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px">
      <div style="background:#1e2230;border-radius:8px;padding:12px;border-left:3px solid #1abc9c">
        <div style="font-size:.65rem;color:#6b7280;text-transform:uppercase;letter-spacing:.1em">Actifs</div>
        <div style="font-size:1.6rem;font-weight:800;color:#1abc9c">{kpis.get('nb_projets_actifs','—')}</div>
      </div>
      <div style="background:#1e2230;border-radius:8px;padding:12px;border-left:3px solid #ef4444">
        <div style="font-size:.65rem;color:#6b7280;text-transform:uppercase;letter-spacing:.1em">En retard</div>
        <div style="font-size:1.6rem;font-weight:800;color:#ef4444">{kpis.get('nb_en_retard','—')}</div>
      </div>
      <div style="background:#1e2230;border-radius:8px;padding:12px;border-left:3px solid #f59e0b">
        <div style="font-size:.65rem;color:#6b7280;text-transform:uppercase;letter-spacing:.1em">À risque</div>
        <div style="font-size:1.6rem;font-weight:800;color:#f59e0b">{kpis.get('nb_at_risk','—')}</div>
      </div>
      <div style="background:#1e2230;border-radius:8px;padding:12px;border-left:3px solid #1abc9c">
        <div style="font-size:.65rem;color:#6b7280;text-transform:uppercase;letter-spacing:.1em">Avancement</div>
        <div style="font-size:1.6rem;font-weight:800;color:#e8eaf0">{kpis.get('avancement_moyen','—')}%</div>
      </div>
    </div>
    <table style="width:100%;border-collapse:collapse;font-size:.78rem;color:#9ca3af">
      <thead>
        <tr style="border-bottom:1px solid #2a2f40;font-size:.65rem;text-transform:uppercase;letter-spacing:.08em;color:#4b5563">
          <th style="text-align:left;padding:8px 6px">ID</th>
          <th style="text-align:left;padding:8px 6px">Projet</th>
          <th style="text-align:left;padding:8px 6px">Responsable</th>
          <th style="text-align:left;padding:8px 6px">Statut</th>
          <th style="text-align:right;padding:8px 6px">Avanc.</th>
          <th style="text-align:left;padding:8px 6px">Décisions</th>
        </tr>
      </thead>
      <tbody>{lignes_projets}</tbody>
    </table>"""


def _html_rapport_projet(projet_id: str, df: pd.DataFrame) -> str:
    """Génère le HTML d'un rapport historique projet."""
    nom = df["projet_nom"].iloc[0] if "projet_nom" in df.columns else projet_id

    lignes = ""
    for _, row in df.iterrows():
        pct = row.get("avancement_pct", 0)
        lignes += f"""
        <tr style="border-bottom:1px solid #2a2f40">
          <td style="padding:8px 6px;color:#9ca3af">{row.get('quinzaine','')}</td>
          <td style="padding:8px 6px">{row.get('statut','')}</td>
          <td style="padding:8px 6px;text-align:right">{pct}%</td>
          <td style="padding:8px 6px;color:#6b7280;font-size:.78rem">{row.get('livrable_quinzaine','') or '—'}</td>
          <td style="padding:8px 6px;color:#6b7280;font-size:.78rem">{row.get('decisions','') or '—'}</td>
        </tr>"""

    return f"""
    <h2 style="font-family:'Syne',sans-serif;color:#1abc9c;margin-bottom:4px">{nom}</h2>
    <p style="color:#6b7280;font-size:.78rem;margin-bottom:20px">{projet_id} — {len(df)} quinzaine(s)</p>
    <table style="width:100%;border-collapse:collapse;font-size:.8rem;color:#e8eaf0">
      <thead>
        <tr style="border-bottom:1px solid #2a2f40;font-size:.65rem;text-transform:uppercase;
                   letter-spacing:.08em;color:#4b5563">
          <th style="text-align:left;padding:8px 6px">Quinzaine</th>
          <th style="text-align:left;padding:8px 6px">Statut</th>
          <th style="text-align:right;padding:8px 6px">Avanc.</th>
          <th style="text-align:left;padding:8px 6px">Livrable</th>
          <th style="text-align:left;padding:8px 6px">Décisions</th>
        </tr>
      </thead>
      <tbody>{lignes}</tbody>
    </table>"""


def _html_rapport_delta(q_avant: str, q_apres: str, df: pd.DataFrame) -> str:
    """Génère le HTML d'un rapport de comparaison deux quinzaines."""
    if df.empty:
        return f"<p>Impossible de comparer {q_avant} et {q_apres}</p>"

    lignes = ""
    for _, row in df.iterrows():
        delta = row.get("delta_avancement", 0)
        if pd.isna(delta):
            delta = 0
        couleur_delta = "#1abc9c" if delta > 0 else ("#ef4444" if delta < 0 else "#6b7280")
        signe = "+" if delta > 0 else ""
        lignes += f"""
        <tr style="border-bottom:1px solid #2a2f40">
          <td style="padding:8px 6px;color:#e8eaf0;font-weight:500">{row.get('projet_nom','')}</td>
          <td style="padding:8px 6px;color:#9ca3af">{row.get('statut_avant','') or '—'}</td>
          <td style="padding:8px 6px;color:#9ca3af">{row.get('statut_apres','') or '—'}</td>
          <td style="padding:8px 6px;text-align:right">{int(row.get('avancement_avant',0) or 0)}%</td>
          <td style="padding:8px 6px;text-align:right">{int(row.get('avancement_apres',0) or 0)}%</td>
          <td style="padding:8px 6px;text-align:right;color:{couleur_delta};font-weight:500">
            {signe}{int(delta)}%
          </td>
        </tr>"""

    return f"""
    <h2 style="font-family:'Syne',sans-serif;color:#1abc9c;margin-bottom:4px">
      Comparaison quinzaines
    </h2>
    <p style="color:#6b7280;font-size:.78rem;margin-bottom:20px">{q_avant} → {q_apres}</p>
    <table style="width:100%;border-collapse:collapse;font-size:.8rem;color:#9ca3af">
      <thead>
        <tr style="border-bottom:1px solid #2a2f40;font-size:.65rem;text-transform:uppercase;
                   letter-spacing:.08em;color:#4b5563">
          <th style="text-align:left;padding:8px 6px">Projet</th>
          <th style="text-align:left;padding:8px 6px">Statut avant</th>
          <th style="text-align:left;padding:8px 6px">Statut après</th>
          <th style="text-align:right;padding:8px 6px">Avant</th>
          <th style="text-align:right;padding:8px 6px">Après</th>
          <th style="text-align:right;padding:8px 6px">Delta</th>
        </tr>
      </thead>
      <tbody>{lignes}</tbody>
    </table>"""
