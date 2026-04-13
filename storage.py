"""
storage.py
==========
Persistance Parquet des données issues du pipeline Excel.

Rôle : excel_parser.py → storage.py → query/ + api/

Note : la validation des données est assurée en amont par Excel (listes
déroulantes, protections VBA). Ce module fait confiance aux données reçues.
"""

import pandas as pd
import yaml
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _cfg(config_path="config.yaml") -> dict:
    p = Path(config_path)
    return yaml.safe_load(p.read_text()) if p.exists() else {}


class StorageManager:
    """
    Interface unique pour lire et écrire les données du projet en Parquet.

    Utilisation :
        from storage.storage import StorageManager
        sm = StorageManager()
        sm.sauvegarder_quinzaine(df, "Q1_2025_S1")
        kpis = sm.kpis()
    """

    def __init__(self, config_path="config.yaml"):
        cfg         = _cfg(config_path)
        paths       = cfg.get("paths", {})
        storage_cfg = cfg.get("storage", {})

        self.parquet_dir = Path(paths.get("parquet_dir", "storage/parquet"))
        self.parquet_dir.mkdir(parents=True, exist_ok=True)

        self.fq = self.parquet_dir / storage_cfg.get("fichier_quinzaines", "quinzaines.parquet")
        self.fm = self.parquet_dir / storage_cfg.get("fichier_meta",       "meta_projets.parquet")

        log.info(f"StorageManager — Parquet — dossier : {self.parquet_dir}")

    # ── Écriture ──────────────────────────────────────────────────────────────

    def sauvegarder_quinzaine(self, df: pd.DataFrame, nom_quinzaine: str) -> bool:
        """
        Ajoute ou remplace une quinzaine dans le fichier Parquet principal.
        Idempotent : relancer deux fois ne duplique pas les données.
        """
        if df is None or df.empty:
            log.warning(f"DataFrame vide pour '{nom_quinzaine}'")
            return False
        try:
            if self.fq.exists():
                existant = pd.read_parquet(self.fq)
                existant = existant[existant["quinzaine"] != nom_quinzaine]
                df_final = pd.concat([existant, df], ignore_index=True)
            else:
                df_final = df.copy()
            df_final.to_parquet(self.fq, index=False)
            log.info(f"'{nom_quinzaine}' — {len(df)} ligne(s) sauvegardées")
            return True
        except Exception as e:
            log.error(f"Erreur sauvegarde '{nom_quinzaine}' : {e}")
            return False

    def sauvegarder_meta(self, df: pd.DataFrame) -> bool:
        """Sauvegarde le référentiel META (liste des projets)."""
        if df is None or df.empty:
            log.warning("DataFrame META vide")
            return False
        try:
            df.to_parquet(self.fm, index=False)
            log.info(f"META — {len(df)} projet(s) sauvegardés")
            return True
        except Exception as e:
            log.error(f"Erreur sauvegarde META : {e}")
            return False

    # ── Lecture ───────────────────────────────────────────────────────────────

    def charger_quinzaines(self, quinzaines=None, projets=None) -> pd.DataFrame:
        """
        Charge les données avec filtres optionnels.
            quinzaines : ["Q1_2025_S1", ...]  — None = toutes
            projets    : ["PROJ-001", ...]    — None = tous
        """
        if not self.fq.exists():
            log.warning("Aucune donnée — lance run_pipeline.py d'abord")
            return pd.DataFrame()
        df = pd.read_parquet(self.fq)
        if quinzaines:
            df = df[df["quinzaine"].isin(quinzaines)]
        if projets:
            df = df[df["projet_id"].isin(projets)]
        return df.reset_index(drop=True)

    def charger_meta(self) -> pd.DataFrame:
        """Charge le référentiel META."""
        if not self.fm.exists():
            log.warning("META absent")
            return pd.DataFrame()
        return pd.read_parquet(self.fm)

    # ── Requêtes analytiques ──────────────────────────────────────────────────

    def projet(self, projet_id: str) -> pd.DataFrame:
        """Historique complet d'un projet, trié par quinzaine."""
        df = self.charger_quinzaines(projets=[projet_id])
        return df.sort_values("quinzaine").reset_index(drop=True) if not df.empty else df

    def derniere_quinzaine(self) -> pd.DataFrame:
        """Données de la quinzaine la plus récente."""
        df = self.charger_quinzaines()
        if df.empty:
            return df
        return df[df["quinzaine"] == df["quinzaine"].max()].reset_index(drop=True)

    def kpis(self, quinzaine=None) -> dict:
        """KPIs pour le dashboard."""
        df = self.charger_quinzaines(quinzaines=[quinzaine]) if quinzaine \
             else self.derniere_quinzaine()
        if df.empty:
            return {}
        pct = pd.to_numeric(df["avancement_pct"], errors="coerce")
        return {
            "quinzaine":         str(df["quinzaine"].iloc[0]),
            "nb_projets_actifs": int((~df["statut"].isin(["DONE", "ON_HOLD"])).sum()),
            "nb_done":           int((df["statut"] == "DONE").sum()),
            "nb_on_hold":        int((df["statut"] == "ON_HOLD").sum()),
            "nb_en_retard":      int((df["statut"] == "LATE").sum()),
            "nb_at_risk":        int((df["statut"] == "AT_RISK").sum()),
            "avancement_moyen":  round(float(pct.mean()), 1),
            "nb_decisions":      int(df["decisions"].apply(lambda x: bool(str(x).strip())).sum()),
            "nb_blocages":       int(df["points_blocage"].apply(lambda x: bool(str(x).strip())).sum()),
        }

    def projets_par_statut(self, quinzaine=None) -> dict:
        """Nombre de projets par statut."""
        df = self.charger_quinzaines(quinzaines=[quinzaine]) if quinzaine \
             else self.derniere_quinzaine()
        return {} if df.empty else df["statut"].value_counts().to_dict()

    def delta_quinzaines(self, q_avant: str, q_apres: str) -> pd.DataFrame:
        """Compare deux quinzaines — changements par projet."""
        df = self.charger_quinzaines(quinzaines=[q_avant, q_apres])
        if df.empty:
            return pd.DataFrame()
        avant = df[df["quinzaine"] == q_avant][
            ["projet_id", "projet_nom", "statut", "avancement_pct"]
        ].rename(columns={"statut": "statut_avant", "avancement_pct": "avancement_avant"})
        apres = df[df["quinzaine"] == q_apres][
            ["projet_id", "statut", "avancement_pct"]
        ].rename(columns={"statut": "statut_apres", "avancement_pct": "avancement_apres"})
        m = avant.merge(apres, on="projet_id", how="outer")
        m["avancement_avant"] = pd.to_numeric(m["avancement_avant"], errors="coerce")
        m["avancement_apres"] = pd.to_numeric(m["avancement_apres"], errors="coerce")
        m["delta_avancement"] = m["avancement_apres"] - m["avancement_avant"]
        return m.sort_values("delta_avancement").reset_index(drop=True)

    def lister_quinzaines(self) -> list:
        """Liste toutes les quinzaines stockées, triées."""
        df = self.charger_quinzaines()
        return sorted(df["quinzaine"].unique().tolist()) if not df.empty else []

    def lister_projets(self) -> list:
        """Projets avec leur dernier statut connu."""
        df = self.derniere_quinzaine()
        if df.empty:
            return []
        cols = ["projet_id", "projet_nom", "statut", "avancement_pct", "responsable_principal"]
        cols = [c for c in cols if c in df.columns]
        return df[cols].to_dict(orient="records")

    def infos(self) -> dict:
        """État du stockage."""
        r = {
            "moteur":  "Parquet",
            "dossier": str(self.parquet_dir),
            "quinzaines_existe": self.fq.exists(),
            "meta_existe":       self.fm.exists(),
            "quinzaines": [], "nb_lignes": 0, "nb_projets": 0,
        }
        if self.fq.exists():
            df = pd.read_parquet(self.fq)
            r["quinzaines"]  = sorted(df["quinzaine"].unique().tolist())
            r["nb_lignes"]   = len(df)
            r["nb_projets"]  = df["projet_id"].nunique()
        return r
