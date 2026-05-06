"""
run_pipeline.py
===============
Point d'entrée unique du pipeline Project Intelligence.

Pipeline :
    Référentiel_sujet.xlsx + Fiches_individuelles/ → Parquet → dashboard.html

Usage :
    python run_pipeline.py
    python run_pipeline.py --quinzaine T1_2026_R1
    python run_pipeline.py --skip-ingest
    python run_pipeline.py --force
    python run_pipeline.py --llm
    python run_pipeline.py --only-ingest
"""

import sys
import argparse
import logging
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


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


def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Project Intelligence"
    )
    parser.add_argument("--config",      default="config.yaml")
    parser.add_argument("--quinzaine",   default=None,        help="Ex: T1_2026_R1")
    parser.add_argument("--skip-ingest", action="store_true", help="Sauter l'ingestion")
    parser.add_argument("--force",       action="store_true", help="Réingérer toutes les quinzaines")
    parser.add_argument("--llm",         action="store_true", help="Pré-générer réponses LLM")
    parser.add_argument("--only-ingest", action="store_true", help="Ingestion seulement")
    args = parser.parse_args()

    config     = args.config
    cfg        = _charger_config(config)
    q_courante = args.quinzaine or _quinzaine_courante(cfg)

    print("\n" + "="*60)
    print(f"  PROJECT INTELLIGENCE — Pipeline")
    print(f"  Config    : {config}")
    print(f"  Quinzaine : {q_courante or 'auto'}")
    print("="*60)

    # ── Étape 1 : Ingestion → Parquet ────────────────────────────────
    if not args.skip_ingest:
        print("\n▶  ÉTAPE 1 — Ingestion Excel → Parquet")
        print("-"*60)
        try:
            from excel_parser import parser_fiches

            paths            = cfg.get("paths", {})
            dossier_f        = paths.get("fiches_individuelles", "Monitoring/Fiches_individuelles")
            dossier_p        = paths.get("parquet_dir") or paths.get("parquet", "storage/parquet")
            referentiel_path = paths.get("referentiel_sujet")

            if not referentiel_path:
                print("  ⚠  'referentiel_sujet' absent de config.yaml — META sera vide")
            else:
                print(f"  Référentiel : {referentiel_path}")

            resultats = parser_fiches(
                dossier_fiches=dossier_f,
                dossier_parquet=dossier_p,
                referentiel_path=referentiel_path,
                quinzaine_courante=q_courante,
                force=args.force,
                quinzaine_unique=args.quinzaine,
            )

            if not resultats:
                print("  → Aucune nouvelle quinzaine (déjà à jour)")
            else:
                for q, n in sorted(resultats.items()):
                    print(f"  ✓ {q} — {n} ligne(s)")

            # Consolider dans StorageManager
            print("\n  Consolidation → StorageManager...")
            from storage import StorageManager

            sm = StorageManager(config)

            for q in resultats:
                p = Path(dossier_p) / f"{q}.parquet"
                if p.exists():
                    df_q = pd.read_parquet(p)
                    if "quinzaine" not in df_q.columns:
                        df_q["quinzaine"] = q
                    sm.sauvegarder_quinzaine(df_q, q)

            # META depuis référentiel
            for nom in ["meta.parquet", "meta_projets.parquet"]:
                mp = Path(dossier_p) / nom
                if mp.exists():
                    sm.sauvegarder_meta(pd.read_parquet(mp))
                    break

            # ARCHIVAGE depuis référentiel (eta_projet=Terminé)
            arch_p = Path(dossier_p) / "archivage.parquet"
            if arch_p.exists():
                df_arch = pd.read_parquet(arch_p)
                sm.sauvegarder_archivage(df_arch)
                print(f"  ✓ ARCHIVAGE : {len(df_arch)} projets terminés")

            # AGENDA (fiches + faits_marquants)
            ap = Path(dossier_p) / "agenda.parquet"
            if ap.exists():
                sm.sauvegarder_agenda(pd.read_parquet(ap))
                print(f"  ✓ AGENDA chargé")

            # Entités détectées
            entites = sm.lister_entites()
            if entites:
                print(f"  ✓ Entités : {', '.join(entites)}")

            infos = sm.infos()
            print(
                f"  ✓ Storage : {infos['nb_projets']} projets · "
                f"{infos['nb_lignes']} lignes · "
                f"{len(infos['quinzaines'])} quinzaine(s) · "
                f"archivage={'oui' if infos['archivage_existe'] else 'non'}"
            )
            print("✓ Ingestion terminée")

        except ImportError as e:
            print(f"✗ Import impossible : {e}")
            sys.exit(1)
        except Exception as e:
            print(f"✗ Erreur ingestion : {e}")
            log.exception(e)
            sys.exit(1)
    else:
        print("\n  Ingestion ignorée (--skip-ingest)")

    if args.only_ingest:
        print("\n" + "="*60)
        print("  Pipeline terminé (--only-ingest) ✓")
        print("="*60 + "\n")
        return

    # ── Étape 2 : Pré-génération LLM (optionnel) ─────────────────────
    llm_cache = {}
    if args.llm:
        print("\n▶  ÉTAPE 2 — Pré-génération LLM")
        print("-"*60)
        try:
            from rag_engine import enrichir_html_generator
            llm_cache = enrichir_html_generator(config, quinzaine=args.quinzaine)
            print(f"✓ {len(llm_cache)} réponses LLM générées")
        except Exception as e:
            print(f"⚠  LLM non disponible : {e}")

    # ── Étape 3 : Dashboard HTML ──────────────────────────────────────
    print("\n▶  ÉTAPE 3 — Génération du dashboard HTML")
    print("-"*60)
    try:
        from html_generator import generer_dashboard
        chemin = generer_dashboard(
            config_path=config,
            quinzaine=args.quinzaine,
            llm_reponses=llm_cache,
        )
        if chemin:
            print(f"✓ Dashboard : {chemin}")
        else:
            print("⚠  Dashboard non généré")
    except Exception as e:
        print(f"✗ Erreur dashboard : {e}")
        log.exception(e)

    print("\n" + "="*60)
    print("  Pipeline terminé ✓")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
