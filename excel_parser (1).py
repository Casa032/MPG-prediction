"""
excel_parser.py
===============
Lecture des fiches individuelles Excel et consolidation vers Parquet.

Architecture :
    - META         : lu depuis le RÉFÉRENTIEL SUJET (feuille "Référentiel")
                     source unique de vérité, toujours à jour
    - ARCHIVAGE    : lu depuis les fiches individuelles
    - AGENDA       : lu depuis les fiches individuelles
    - faits_marquants : colonne du référentiel, fusionnée dans agenda
    - Feuilles quinzaine (T1_2026_R1 etc.) : fiches individuelles uniquement

Nouveaux projets :
    - Projets de type PROJET dont date_debut dans les 30 derniers jours
    - Calculé dans preparer_donnees() de html_generator.py

Stratégie incrémentale :
    - Quinzaines passées → Parquet figé, jamais relu sauf --force
    - Quinzaine courante → toujours relue

Usage :
    python excel_parser.py
    python excel_parser.py --force
    python excel_parser.py --quinzaine T1_2026_R1
    python excel_parser.py --config config.yaml
"""

import re
import logging
import argparse
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime

log = logging.getLogger(__name__)

PATTERN_QUINZAINE = re.compile(r"^T[1-4]_\d{4}_R[1-6]$")

COLONNES_FEUILLE = [
    "ref_sujet", "sujet", "phase", "statut", "avancement_pct",
    "actions_realises", "actions_a_mener", "actions_echeance",
    "charge_a_prevoir", "points_blocage", "commentaire",
    "budget_jours",
]

COLONNES_REFERENTIEL = [
    "type", "ref_sujet", "sujet", "domaine", "entite_concerne",
    "effectifs", "responsable_principal",
    "date_debut", "date_fin_prevue",
    "priorite", "budget_jours", "description",
    "collaborateurs_temporaires", "eta_intervention", "eta_projet",
    "faits_marquants",
]

COLONNES_ARCHIVAGE = COLONNES_REFERENTIEL

COLONNES_AGENDA = ["date", "titre", "type", "description", "projet_ref"]
TYPES_AGENDA_VALIDES = {
    "REUNION", "LIVRAISON", "ACTUALITE", "JALON", "EVENEMENT", "AUTRE"
}

RENAME_REF = {
    "sujet":     "projet_nom",
    "ref_sujet": "projet_id",
}
RENAME_FEUILLE = {
    "sujet":       "projet_nom",
    "ref_sujet":   "projet_id",
    "commentaire": "commentaire_libre",
}


def _charger_config(config_path: str) -> dict:
    p = Path(config_path)
    return yaml.safe_load(p.read_text(encoding="utf-8")) if p.exists() else {}


def _quinzaine_courante(config: dict) -> str | None:
    q = config.get("quinzaine_courante")
    if q:
        return q
    now = datetime.now()
    trimestre = (now.month - 1) // 3 + 1
    mois_dans_trim = (now.month - 1) % 3 + 1
    rang = 1 if mois_dans_trim == 1 else (3 if mois_dans_trim == 2 else 5)
    if now.day > 15:
        rang += 1
    return f"T{trimestre}_{now.year}_R{min(rang, 6)}"


def _est_feuille_quinzaine(nom: str) -> bool:
    return bool(PATTERN_QUINZAINE.match(nom))


def _normaliser_colonnes(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _valeur_ok(v) -> bool:
    return bool(v) and str(v).strip() not in ("", "nan", "None", "NaN")


def _parse_date_str(v) -> datetime | None:
    v = str(v).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue
    return None


# ── Lecture référentiel sujet ──────────────────────────────────────────

def lire_referentiel(referentiel_path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lit la feuille 'Référentiel' du fichier référentiel sujet.
    Retourne un tuple (df_actifs, df_archives) :
        - df_actifs  : projets dont eta_projet != "Terminé"
        - df_archives: projets dont eta_projet == "Terminé"
    """
    path = Path(referentiel_path)
    vide = pd.DataFrame(), pd.DataFrame()

    if not path.exists():
        log.error(f"Référentiel introuvable : {path}")
        return vide

    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
    except Exception as e:
        log.error(f"Impossible d'ouvrir le référentiel {path.name} : {e}")
        return vide

    # Chercher la feuille (insensible à la casse et aux accents)
    nom_feuille = None
    for s in xl.sheet_names:
        s_norm = s.strip().lower()
        if s_norm in ("référentiel", "referentiel", "ref", "sujet", "projets"):
            nom_feuille = s
            break

    if nom_feuille is None:
        log.error(
            f"Feuille 'Référentiel' introuvable dans {path.name}. "
            f"Feuilles disponibles : {xl.sheet_names}"
        )
        return vide

    try:
        df = xl.parse(nom_feuille, dtype=str)
    except Exception as e:
        log.error(f"Erreur lecture feuille '{nom_feuille}' : {e}")
        return vide

    if df.empty:
        log.warning(f"Feuille '{nom_feuille}' vide")
        return vide

    df = _normaliser_colonnes(df)

    if "ref_sujet" not in df.columns:
        log.error("Colonne 'ref_sujet' absente du référentiel")
        return vide

    df = df[df["ref_sujet"].notna() & (df["ref_sujet"].str.strip() != "")]
    if df.empty:
        return vide

    cols = [c for c in COLONNES_REFERENTIEL if c in df.columns]
    df = df[cols].copy()
    df = df.rename(columns=RENAME_REF)
    df["source_fichier"] = path.name

    # Séparer actifs et archivés sur eta_projet
    VALEURS_TERMINE = {"terminé", "termine", "terminee", "terminée", "done", "archivé", "archive"}
    if "eta_projet" in df.columns:
        masque_archive = df["eta_projet"].apply(
            lambda v: str(v).strip().lower() in VALEURS_TERMINE if pd.notna(v) else False
        )
        df_actifs   = df[~masque_archive].copy()
        df_archives = df[masque_archive].copy()
    else:
        df_actifs   = df.copy()
        df_archives = pd.DataFrame()

    log.info(
        f"Référentiel '{nom_feuille}' → "
        f"{len(df_actifs)} actif(s) · {len(df_archives)} archivé(s)"
    )
    return df_actifs, df_archives


# ── ARCHIVAGE : lu depuis le référentiel (eta_projet == Terminé) ─────────
# La fonction _lire_archivage depuis les fiches est supprimée.
# L'archivage est extrait dans lire_referentiel() via eta_projet.

# ── AGENDA depuis fiches ───────────────────────────────────────────────

def _lire_agenda(wb_path: Path, xl: pd.ExcelFile) -> pd.DataFrame:
    if "AGENDA" not in xl.sheet_names:
        return pd.DataFrame()
    try:
        df = xl.parse("AGENDA", dtype=str)
    except Exception as e:
        log.error(f"{wb_path.name} — erreur AGENDA : {e}")
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    df = _normaliser_colonnes(df)

    if "date" not in df.columns or "titre" not in df.columns:
        log.warning(f"{wb_path.name} — AGENDA sans colonnes date/titre")
        return pd.DataFrame()

    df = df[df["date"].notna() & df["titre"].notna()].copy()
    df = df[df["date"].str.strip() != ""]
    df = df[df["titre"].str.strip() != ""]
    if df.empty:
        return pd.DataFrame()

    if "type" in df.columns:
        df["type"] = df["type"].apply(
            lambda v: str(v).strip().upper() if pd.notna(v) else "AUTRE"
        )
        df["type"] = df["type"].apply(
            lambda v: v if v in TYPES_AGENDA_VALIDES else "AUTRE"
        )
    else:
        df["type"] = "AUTRE"

    def _parse_date(v):
        dt = _parse_date_str(v)
        return dt.strftime("%Y-%m-%d") if dt else None

    df["date"] = df["date"].apply(_parse_date)
    df = df[df["date"].notna()].copy()

    cols = [c for c in COLONNES_AGENDA if c in df.columns]
    df = df[cols].copy()
    for col in COLONNES_AGENDA:
        if col not in df.columns:
            df[col] = ""

    df["source_fichier"] = wb_path.name
    log.info(f"{wb_path.name} — AGENDA : {len(df)} événement(s)")
    return df


# ── faits_marquants depuis référentiel ────────────────────────────────

def _extraire_faits_marquants(df_meta: pd.DataFrame) -> list[dict]:
    """
    Parse faits_marquants du référentiel.
    Format : JJ/MM/AAAA:description;JJ/MM/AAAA:description
    """
    if df_meta.empty or "faits_marquants" not in df_meta.columns:
        return []

    col_id  = "projet_id" if "projet_id" in df_meta.columns else "ref_sujet"
    col_nom = "projet_nom" if "projet_nom" in df_meta.columns else "sujet"
    entries = []

    for _, row in df_meta.iterrows():
        raw = str(row.get("faits_marquants", "") or "").strip()
        if not _valeur_ok(raw):
            continue

        nom_proj = str(row.get(col_nom, "") or "")
        pid      = str(row.get(col_id,  "") or "")

        for paire in raw.split(";"):
            paire = paire.strip()
            if ":" not in paire:
                continue
            date_str, desc = paire.split(":", 1)
            dt = _parse_date_str(date_str.strip())
            if dt is None:
                log.warning(f"faits_marquants date non parsée : '{date_str.strip()}'")
                continue
            entries.append({
                "date":          dt.strftime("%Y-%m-%d"),
                "titre":         f"Événement — {nom_proj}" if nom_proj else "Événement projet",
                "type":          "EVENEMENT",
                "description":   desc.strip(),
                "projet_ref":    pid,
                "source_fichier":"faits_marquants_referentiel",
            })

    if entries:
        log.info(f"faits_marquants → {len(entries)} événement(s)")
    return entries


# ── Feuille quinzaine ──────────────────────────────────────────────────

def _lire_feuille_quinzaine(wb_path: Path, xl: pd.ExcelFile,
                             nom_feuille: str, responsable: str) -> pd.DataFrame:
    try:
        df = xl.parse(nom_feuille, dtype=str)
    except Exception as e:
        log.error(f"{wb_path.name}/{nom_feuille} — erreur : {e}")
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    df = _normaliser_colonnes(df)

    if "ref_sujet" not in df.columns:
        log.warning(f"{wb_path.name}/{nom_feuille} — ref_sujet absent")
        return pd.DataFrame()

    df = df[df["ref_sujet"].notna() & (df["ref_sujet"].str.strip() != "")].copy()
    if df.empty:
        return pd.DataFrame()

    cols = [c for c in COLONNES_FEUILLE if c in df.columns]
    df = df[cols].copy()

    if "avancement_pct" in df.columns:
        def _norm_pct(v):
            try:
                f = float(str(v).replace(",", ".").replace("%", "").strip())
                return round(f * 100 if f <= 1.0 else f)
            except (ValueError, TypeError):
                return None
        df["avancement_pct"] = df["avancement_pct"].apply(_norm_pct)

    if "statut" in df.columns:
        STATUT_MAP = {
            "en cours": "ON_TRACK", "a risque": "AT_RISK", "à risque": "AT_RISK",
            "en retard": "LATE", "terminé": "DONE", "termine": "DONE",
            "stand by": "ON_HOLD", "on hold": "ON_HOLD",
            "on_track": "ON_TRACK", "at_risk": "AT_RISK",
            "late": "LATE", "done": "DONE", "on_hold": "ON_HOLD",
        }
        df["statut"] = df["statut"].apply(
            lambda v: STATUT_MAP.get(
                str(v).strip().lower(),
                str(v).strip().upper() if pd.notna(v) else "ON_TRACK"
            )
        )

    df = df.rename(columns=RENAME_FEUILLE)
    df["quinzaine"]             = nom_feuille
    df["responsable_principal"] = responsable
    df["source_fichier"]        = wb_path.name
    return df


def _extraire_responsable(wb_path: Path) -> str:
    stem = re.sub(r"(?i)^fiches?_", "", wb_path.stem)
    return stem.replace("_", " ").strip()


def _consolider_quinzaine(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Consolide plusieurs DataFrames d'une même quinzaine (un par collaborateur).

    Pour chaque projet (projet_id) :
    - Champs texte libres (actions, blocages, commentaires) : concaténés
      avec séparateur " | " en supprimant les doublons et valeurs vides.
    - Champs numériques (avancement_pct) : moyenne pondérée.
    - Champs de statut (statut, phase) : valeur du responsable principal
      ou valeur la plus fréquente.
    - Champs d'identité (projet_nom, responsable_principal) : premier non vide.
    """
    if not dfs:
        return pd.DataFrame()
    if len(dfs) == 1:
        return dfs[0].copy()

    df_all = pd.concat(dfs, ignore_index=True)
    if df_all.empty:
        return df_all

    # Colonnes à concaténer (texte libre, plusieurs contributeurs possibles)
    COLS_CONCAT = [
        "actions_realises", "actions_a_mener", "actions_echeance",
        "points_blocage", "commentaire_libre", "charge_a_prevoir",
    ]
    # Colonnes à prendre en premier non-vide
    COLS_FIRST = ["projet_nom", "phase", "statut", "avancement_pct",
                  "responsable_principal", "quinzaine", "source_fichier"]

    def _concat_valeurs(series: pd.Series) -> str:
        vals = []
        seen = set()
        for v in series:
            v_str = str(v).strip() if pd.notna(v) else ""
            if v_str and v_str not in ("nan", "None") and v_str not in seen:
                seen.add(v_str)
                vals.append(v_str)
        return " | ".join(vals) if vals else ""

    def _premier_non_vide(series: pd.Series):
        for v in series:
            if pd.notna(v) and str(v).strip() not in ("", "nan", "None"):
                return v
        return None

    resultats = []
    col_id = "projet_id" if "projet_id" in df_all.columns else "ref_sujet"

    for pid, groupe in df_all.groupby(col_id, sort=False):
        row = {col_id: pid}

        # Champs texte : concaténer
        for col in COLS_CONCAT:
            if col in groupe.columns:
                row[col] = _concat_valeurs(groupe[col])

        # Champs first : premier non vide
        for col in COLS_FIRST:
            if col in groupe.columns:
                row[col] = _premier_non_vide(groupe[col])

        # avancement_pct : moyenne
        if "avancement_pct" in groupe.columns:
            vals_num = pd.to_numeric(groupe["avancement_pct"], errors="coerce").dropna()
            row["avancement_pct"] = round(vals_num.mean()) if not vals_num.empty else None

        resultats.append(row)

    df_result = pd.DataFrame(resultats)

    # Réintégrer les colonnes restantes non traitées
    autres_cols = [c for c in df_all.columns
                   if c not in df_result.columns and c != col_id]
    for col in autres_cols:
        df_result[col] = df_all.groupby(col_id)[col].first().reindex(
            df_result[col_id].values
        ).values

    log.debug(f"Consolidation : {len(df_all)} lignes → {len(df_result)} projets uniques")
    return df_result


# ── Pipeline principal ─────────────────────────────────────────────────

def parser_fiches(
    dossier_fiches:     str | Path,
    dossier_parquet:    str | Path,
    referentiel_path:   str | Path | None = None,
    quinzaine_courante: str | None = None,
    force:              bool = False,
    quinzaine_unique:   str | None = None,
) -> dict[str, int]:
    """
    Pipeline complet :
        1. Lit META depuis le référentiel sujet
        2. Lit ARCHIVAGE + AGENDA + quinzaines depuis les fiches
        3. Joint META sur chaque quinzaine
        4. Produit les Parquets
    """
    dossier_fiches  = Path(dossier_fiches)
    dossier_parquet = Path(dossier_parquet)
    dossier_parquet.mkdir(parents=True, exist_ok=True)

    if not dossier_fiches.exists():
        log.error(f"Dossier fiches introuvable : {dossier_fiches}")
        return {}

    # ── 1. META + ARCHIVAGE depuis référentiel ───────────────────────
    df_meta_global  = pd.DataFrame()
    df_arch_ref     = pd.DataFrame()
    if referentiel_path:
        df_meta_global, df_arch_ref = lire_referentiel(referentiel_path)
    else:
        log.warning("Aucun référentiel configuré — META vide. "
                    "Ajoute 'referentiel_sujet' dans config.yaml > paths")

    if not df_meta_global.empty:
        df_meta_global.to_parquet(dossier_parquet / "meta.parquet", index=False)

    # Sauvegarder l'archivage depuis le référentiel
    if not df_arch_ref.empty:
        df_arch_ref.to_parquet(dossier_parquet / "archivage.parquet", index=False)
        log.info(f"ARCHIVAGE → {len(df_arch_ref)} projets archivés (eta_projet=Terminé)")

    # ── 2. faits_marquants → entrées agenda ───────────────────────────
    faits_entries = _extraire_faits_marquants(df_meta_global)

    # ── 3. Parcourir fiches individuelles ─────────────────────────────
    fichiers = [
        f for f in dossier_fiches.rglob("*.xls*")
        if not f.name.startswith("~$") and f.suffix in (".xlsx", ".xlsm", ".xls")
    ]
    if not fichiers:
        log.warning(f"Aucun fichier Excel dans {dossier_fiches}")
        return {}

    log.info(f"{len(fichiers)} fiche(s) trouvée(s)")

    donnees_par_q: dict[str, list[pd.DataFrame]] = {}
    agendas:       list[pd.DataFrame] = []

    for fichier in sorted(fichiers):
        responsable = _extraire_responsable(fichier)
        log.info(f"  Lecture : {fichier.name} ({responsable})")
        try:
            xl = pd.ExcelFile(fichier, engine="openpyxl")
        except Exception as e:
            log.error(f"  Impossible d'ouvrir {fichier.name} : {e}")
            continue

        # Plus de lecture ARCHIVAGE depuis les fiches — géré par le référentiel
        df_agenda = _lire_agenda(fichier, xl)
        if not df_agenda.empty:
            agendas.append(df_agenda)

        for nom_feuille in [s for s in xl.sheet_names if _est_feuille_quinzaine(s)]:
            if quinzaine_unique and nom_feuille != quinzaine_unique:
                continue
            df_q = _lire_feuille_quinzaine(fichier, xl, nom_feuille, responsable)
            if not df_q.empty:
                donnees_par_q.setdefault(nom_feuille, []).append(df_q)

    # ── 4. AGENDA consolidé (AGENDA fiches + faits_marquants référentiel) ─
    if agendas or faits_entries:
        frames = agendas + ([pd.DataFrame(faits_entries)] if faits_entries else [])
        df_ag = pd.concat(frames, ignore_index=True).sort_values("date").reset_index(drop=True)
        for col in ["date", "titre", "type", "description", "projet_ref", "source_fichier"]:
            if col not in df_ag.columns:
                df_ag[col] = ""
        df_ag.to_parquet(dossier_parquet / "agenda.parquet", index=False)
        log.info(f"AGENDA → {len(df_ag)} événements")

    # ── 5. Parquets quinzaine ─────────────────────────────────────────
    COLS_JOIN = [
        "projet_id", "domaine", "entite_concerne", "effectifs",
        "date_debut", "date_fin_prevue", "priorite", "budget_jours",
        "description", "type", "collaborateurs_temporaires",
        "eta_intervention", "eta_projet", "responsable_principal",
    ]

    resultats = {}
    for quinzaine, dfs in sorted(donnees_par_q.items()):
        chemin_q     = dossier_parquet / f"{quinzaine}.parquet"
        est_courante = (quinzaine == quinzaine_courante)

        if chemin_q.exists() and not force and not est_courante:
            log.info(f"  {quinzaine} — skip")
            continue

        df_c = _consolider_quinzaine(dfs)

        if not df_meta_global.empty:
            cols_dispo = [c for c in COLS_JOIN if c in df_meta_global.columns]
            df_join = df_meta_global[cols_dispo].drop_duplicates(subset=["projet_id"])
            df_c = df_c.merge(df_join, on="projet_id", how="left", suffixes=("", "_meta"))

        df_c.to_parquet(chemin_q, index=False)
        resultats[quinzaine] = len(df_c)
        log.info(
            f"  {quinzaine} [{'COURANTE' if est_courante else 'nouvelle'}]"
            f" → {len(df_c)} lignes"
        )

    return resultats


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",    default="config.yaml")
    parser.add_argument("--force",     action="store_true")
    parser.add_argument("--quinzaine", default=None)
    args = parser.parse_args()

    config = _charger_config(args.config)
    paths  = config.get("paths", {})

    resultats = parser_fiches(
        dossier_fiches=paths.get("fiches_individuelles", "Monitoring/Fiches_individuelles"),
        dossier_parquet=paths.get("parquet_dir") or paths.get("parquet", "storage/parquet"),
        referentiel_path=paths.get("referentiel_sujet"),
        quinzaine_courante=_quinzaine_courante(config),
        force=args.force,
        quinzaine_unique=args.quinzaine,
    )

    if resultats:
        print(f"\n✓ {len(resultats)} quinzaine(s) :")
        for q, n in sorted(resultats.items()):
            print(f"  {q} → {n} ligne(s)")
    else:
        print("\nAucune quinzaine nouvelle.")
    print()


if __name__ == "__main__":
    main()
