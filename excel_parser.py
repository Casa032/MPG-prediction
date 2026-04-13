"""
excel_parser.py
===============
Lit le fichier Excel consolidé (produit par merger.py / VBA)
et charge les données en Parquet via StorageManager.

Rôle dans le pipeline :
    monito.xlsx → excel_parser.py → storage.py → query/ + api/

Note : la validation des données est assurée par Excel (listes déroulantes VBA).
Ce module lit et fait confiance aux données sans re-valider.
"""

import pandas as pd
import yaml
import logging
from pathlib import Path

log = logging.getLogger(__name__)

SHEETS_IGNOREES = {"META", "DICTIONNAIRE", "TEMPLATE", "NOTES"}

# Colonnes attendues dans les sheets de quinzaine
COLONNES_QUINZAINE = [
    "projet_id", "projet_nom", "domaine", "phase", "effectifs",
    "responsable_principal", "statut", "avancement_pct",
    "livrable_quinzaine", "livrable_statut", "decisions",
    "actions_a_mener", "actions_responsable", "actions_echeance",
    "risques", "risque_niveau", "points_blocage", "commentaire_libre",
]

COLONNES_META = [
    "projet_id", "projet_nom", "domaine",
    "date_debut", "date_fin_prevue", "budget_jours",
    "client_interne", "description",
]


def _cfg(config_path="config.yaml") -> dict:
    p = Path(config_path)
    return yaml.safe_load(p.read_text()) if p.exists() else {}


def _trouver_ligne_headers(ws_df: pd.DataFrame) -> int:
    """
    Cherche la ligne contenant 'projet_id' dans le DataFrame brut.
    Retourne l'index (0-based) ou -1 si introuvable.
    Gère les cas où il y a un titre en ligne 1 avant les en-têtes.
    """
    for i, row in ws_df.iterrows():
        if any(str(v).strip().lower() == "projet_id" for v in row if pd.notna(v)):
            return i
    return -1


def lire_sheet(chemin: Path, sheet_name: str) -> pd.DataFrame:
    """
    Lit une sheet Excel et retourne un DataFrame propre.
    Détecte automatiquement la ligne d'en-têtes (ignore le titre fusionné).
    """
    # header=None pour lire toutes les lignes brutes
    df_brut = pd.read_excel(chemin, sheet_name=sheet_name, header=None, dtype=str)
    df_brut = df_brut.fillna("")

    idx_header = _trouver_ligne_headers(df_brut)
    if idx_header == -1:
        log.warning(f"Sheet '{sheet_name}' : colonne 'projet_id' introuvable — ignorée")
        return pd.DataFrame()

    # Reconstruire avec les bonnes en-têtes
    headers = df_brut.iloc[idx_header].tolist()
    headers = [str(h).strip() for h in headers]
    df = df_brut.iloc[idx_header + 1:].copy()
    df.columns = headers
    df = df.reset_index(drop=True)

    # Retirer les lignes vides et les lignes sans projet_id
    df = df[df["projet_id"].str.strip() != ""].reset_index(drop=True)

    return df


def parser_excel(chemin_excel: str | Path, config_path="config.yaml") -> dict:
    """
    Parse le fichier Excel consolidé et retourne un dict avec :
        - "quinzaines" : dict { nom_sheet: DataFrame }
        - "meta"       : DataFrame (sheet META si présente)
        - "erreurs"    : liste des problèmes rencontrés
        - "stats"      : résumé chiffré

    Usage :
        from ingestion.excel_parser import parser_excel
        resultat = parser_excel("data/monito.xlsx")
        df_q1 = resultat["quinzaines"]["Q1_2025_S1"]
    """
    chemin = Path(chemin_excel)
    resultat = {"quinzaines": {}, "meta": pd.DataFrame(), "erreurs": [], "stats": {}}

    if not chemin.exists():
        resultat["erreurs"].append(f"Fichier introuvable : {chemin}")
        return resultat

    # Lister les sheets du fichier
    try:
        xl = pd.ExcelFile(chemin)
        sheets = xl.sheet_names
    except Exception as e:
        resultat["erreurs"].append(f"Impossible d'ouvrir le fichier : {e}")
        return resultat

    log.info(f"Fichier : {chemin.name} — {len(sheets)} sheet(s) : {sheets}")

    nb_lignes_total = 0

    for sheet_name in sheets:
        # Sheet META
        if sheet_name == "META":
            try:
                df_meta = lire_sheet(chemin, sheet_name)
                if not df_meta.empty:
                    resultat["meta"] = df_meta
                    log.info(f"META — {len(df_meta)} projet(s)")
            except Exception as e:
                resultat["erreurs"].append(f"Erreur lecture META : {e}")
            continue

        # Sheets à ignorer
        if sheet_name in SHEETS_IGNOREES:
            log.info(f"Sheet '{sheet_name}' ignorée")
            continue

        # Sheet de quinzaine
        try:
            df = lire_sheet(chemin, sheet_name)
            if df.empty:
                log.info(f"Sheet '{sheet_name}' vide — ignorée")
                continue

            # Ajouter la colonne quinzaine pour la traçabilité
            df["quinzaine"] = sheet_name

            # Nettoyer avancement_pct (convertir en numérique)
            if "avancement_pct" in df.columns:
                df["avancement_pct"] = pd.to_numeric(
                    df["avancement_pct"], errors="coerce"
                ).fillna(0).astype(int)

            resultat["quinzaines"][sheet_name] = df
            nb_lignes_total += len(df)
            log.info(f"Sheet '{sheet_name}' — {len(df)} projet(s)")

        except Exception as e:
            resultat["erreurs"].append(f"Erreur lecture '{sheet_name}' : {e}")

    resultat["stats"] = {
        "nb_sheets_quinzaine": len(resultat["quinzaines"]),
        "nb_lignes_total":     nb_lignes_total,
        "meta_presente":       not resultat["meta"].empty,
        "erreurs":             len(resultat["erreurs"]),
    }

    return resultat


def pipeline_complet(config_path="config.yaml") -> bool:
    """
    Pipeline principal : lit le fichier Excel et stocke tout en Parquet.

    Usage :
        from ingestion.excel_parser import pipeline_complet
        succes = pipeline_complet()

    Retourne True si au moins une quinzaine a été stockée.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from storage.storage import StorageManager

    cfg = _cfg(config_path)
    chemin_excel = cfg.get("paths", {}).get("excel_source", "data/monito.xlsx")

    print(f"\nPipeline — lecture de : {chemin_excel}")

    resultat = parser_excel(chemin_excel, config_path)

    # Afficher les erreurs
    if resultat["erreurs"]:
        print(f"\nErreurs ({len(resultat['erreurs'])}) :")
        for e in resultat["erreurs"]:
            print(f"  ✗ {e}")

    if not resultat["quinzaines"]:
        print("Aucune quinzaine trouvée — vérifier le fichier Excel.")
        return False

    sm = StorageManager(config_path)

    # Sauvegarder chaque quinzaine
    nb_ok = 0
    for nom_q, df in resultat["quinzaines"].items():
        if sm.sauvegarder_quinzaine(df, nom_q):
            nb_ok += 1

    # Sauvegarder la META si présente
    if not resultat["meta"].empty:
        sm.sauvegarder_meta(resultat["meta"])

    # Résumé
    stats = resultat["stats"]
    print(f"\nRésumé :")
    print(f"  Quinzaines chargées : {nb_ok} / {stats['nb_sheets_quinzaine']}")
    print(f"  Lignes totales      : {stats['nb_lignes_total']}")
    print(f"  META                : {'oui' if stats['meta_presente'] else 'non'}")

    infos = sm.infos()
    print(f"\nStockage Parquet :")
    print(f"  Quinzaines en base  : {infos['quinzaines']}")
    print(f"  Projets uniques     : {infos['nb_projets']}")

    return nb_ok > 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    pipeline_complet()
