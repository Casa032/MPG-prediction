"""
scheduler.py
============
Automatise le pipeline Project Intelligence.
Lance run_pipeline.py selon une fréquence configurable
et envoie le dashboard HTML + rapports PDF par mail.

Configuration dans config.yaml :
    scheduler:
      heure:        "10:00"          # heure de déclenchement quotidien
      frequence:    "quotidien"      # quotidien | lundi | vendredi | lundi,vendredi
      pdf:          true             # générer les PDFs
      llm:          false            # activer le LLM
      mail:
        actif:      true
        smtp_host:  "smtp.entreprise.fr"
        smtp_port:  587
        smtp_tls:   true
        expediteur: "pipeline@entreprise.fr"
        login:      "pipeline@entreprise.fr"
        mot_de_passe: "xxxx"
        destinataires:
          - "chef.projet@entreprise.fr"
          - "direction@entreprise.fr"
        objet:      "📊 Dashboard Projets — {quinzaine}"
        corps: |
          Bonjour,

          Le dashboard de la quinzaine {quinzaine} est disponible.

          Résumé :
          - Projets actifs : {nb_projets_actifs}
          - En retard      : {nb_en_retard}
          - À risque       : {nb_at_risk}
          - Avancement moy.: {avancement_moyen}%

          Cordialement,
          Project Intelligence

Usage :
    python scheduler.py                  # démarre le planificateur (tourne en continu)
    python scheduler.py --now            # exécute immédiatement sans attendre l'heure
    python scheduler.py --test-mail      # envoie un mail de test sans lancer le pipeline
    python scheduler.py --config config.yaml
"""

import sys
import time
import logging
import argparse
import smtplib
import yaml
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ── Config ────────────────────────────────────────────────────────────────────

def _charger_config(config_path: str) -> dict:
    p = Path(config_path)
    return yaml.safe_load(p.read_text(encoding="utf-8")) if p.exists() else {}


def _quinzaine_courante(cfg: dict) -> str:
    q = cfg.get("quinzaine_courante")
    if q:
        return q
    now = datetime.now()
    trimestre = (now.month - 1) // 3 + 1
    mois_dans_trim = (now.month - 1) % 3 + 1
    rang = 1 if mois_dans_trim == 1 else (3 if mois_dans_trim == 2 else 5)
    if now.day > 15:
        rang += 1
    return f"T{trimestre}_{now.year}_R{min(rang, 6)}"


# ── Pipeline ──────────────────────────────────────────────────────────────────

def _lancer_pipeline(config_path: str, cfg: dict) -> dict:
    """
    Lance run_pipeline.py en sous-processus.
    Retourne un dict avec les résultats : chemin_html, chemin_pdf, kpis.
    """
    sched_cfg = cfg.get("scheduler", {})
    avec_pdf  = sched_cfg.get("pdf", True)
    avec_llm  = sched_cfg.get("llm", False)

    cmd = [sys.executable, "run_pipeline.py", "--config", config_path]
    if avec_llm:
        cmd.append("--llm")

    log.info(f"Lancement pipeline : {' '.join(cmd)}")
    debut = datetime.now()

    result = subprocess.run(cmd, capture_output=True, text=True)

    duree = (datetime.now() - debut).seconds
    log.info(f"Pipeline terminé en {duree}s (code retour : {result.returncode})")

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.info(f"  [pipeline] {line}")
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            log.warning(f"  [pipeline] {line}")

    if result.returncode != 0:
        raise RuntimeError(f"Pipeline échoué (code {result.returncode})")

    # Collecter les fichiers générés
    paths      = cfg.get("paths", {})
    chemin_html = Path(paths.get("dashboard_out", "frontend/dashboard.html"))
    pdf_out     = Path(paths.get("pdf_out", "reporting/output"))
    q_active    = _quinzaine_courante(cfg)

    chemins_pdf = []
    if avec_pdf:
        try:
            from reporting.pdf_builder import PdfBuilder
            builder = PdfBuilder(config_path)
            p_q = builder.rapport_quinzaine(q_active)
            if p_q:
                chemins_pdf.append(p_q)
                log.info(f"PDF quinzaine : {p_q}")

            # Delta si quinzaine précédente disponible
            try:
                from storage.storage import StorageManager
                sm = StorageManager(config_path)
                quinzaines = sorted(sm.lister_quinzaines())
                if q_active in quinzaines:
                    idx = quinzaines.index(q_active)
                    if idx > 0:
                        p_d = builder.rapport_delta(quinzaines[idx - 1], q_active)
                        if p_d:
                            chemins_pdf.append(p_d)
                            log.info(f"PDF delta : {p_d}")
            except Exception as e:
                log.warning(f"PDF delta ignoré : {e}")

        except Exception as e:
            log.warning(f"Génération PDF échouée : {e}")

    # Lire les KPIs pour le corps du mail
    kpis = {}
    try:
        from storage.storage import StorageManager
        sm   = StorageManager(config_path)
        kpis = sm.kpis(quinzaine=q_active) or {}
    except Exception as e:
        log.warning(f"KPIs non disponibles : {e}")

    return {
        "chemin_html":  chemin_html,
        "chemins_pdf":  chemins_pdf,
        "quinzaine":    q_active,
        "kpis":         kpis,
        "duree_s":      duree,
    }


# ── Mail ──────────────────────────────────────────────────────────────────────

def _envoyer_mail(cfg: dict, resultat: dict, erreur: str | None = None) -> bool:
    """
    Envoie le dashboard HTML et les PDFs par mail.
    Retourne True si succès.
    """
    mail_cfg = cfg.get("scheduler", {}).get("mail", {})

    if not mail_cfg.get("actif", False):
        log.info("Envoi mail désactivé (scheduler.mail.actif: false)")
        return False

    smtp_host  = mail_cfg.get("smtp_host", "localhost")
    smtp_port  = int(mail_cfg.get("smtp_port", 587))
    smtp_tls   = mail_cfg.get("smtp_tls", True)
    expediteur = mail_cfg.get("expediteur", "")
    login      = mail_cfg.get("login", expediteur)
    mdp        = mail_cfg.get("mot_de_passe", "")
    destins    = mail_cfg.get("destinataires", [])

    if not destins:
        log.warning("Aucun destinataire configuré")
        return False

    q         = resultat.get("quinzaine", "—")
    kpis      = resultat.get("kpis", {})
    duree     = resultat.get("duree_s", 0)

    # Objet
    tpl_objet = mail_cfg.get("objet", "Dashboard Projets — {quinzaine}")
    objet     = tpl_objet.format(quinzaine=q)
    if erreur:
        objet = f"⚠ ERREUR — {objet}"

    # Corps texte
    if erreur:
        corps_txt = (
            f"Le pipeline a rencontré une erreur :\n\n"
            f"{erreur}\n\n"
            f"Quinzaine : {q}\n"
            f"Date      : {datetime.now().strftime('%d/%m/%Y à %H:%M')}\n"
        )
    else:
        tpl_corps = mail_cfg.get("corps", "Dashboard {quinzaine} généré.")
        corps_txt = tpl_corps.format(
            quinzaine           = q,
            nb_projets_actifs   = kpis.get("nb_projets_actifs", "—"),
            nb_en_retard        = kpis.get("nb_en_retard", "—"),
            nb_at_risk          = kpis.get("nb_at_risk", "—"),
            avancement_moyen    = kpis.get("avancement_moyen", "—"),
        )
        corps_txt += f"\n\nGénéré en {duree}s — {datetime.now().strftime('%d/%m/%Y à %H:%M')}"

    # Construction du message
    msg = MIMEMultipart()
    msg["From"]    = expediteur
    msg["To"]      = ", ".join(destins)
    msg["Subject"] = objet
    msg.attach(MIMEText(corps_txt, "plain", "utf-8"))

    # Pièces jointes
    pieces = []
    if not erreur:
        chemin_html = resultat.get("chemin_html")
        if chemin_html and Path(chemin_html).exists():
            pieces.append(Path(chemin_html))
        for pdf in resultat.get("chemins_pdf", []):
            if Path(pdf).exists():
                pieces.append(Path(pdf))

    for piece in pieces:
        try:
            with open(piece, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={piece.name}",
            )
            msg.attach(part)
            log.info(f"  Pièce jointe : {piece.name}")
        except Exception as e:
            log.warning(f"  Impossible d'attacher {piece.name} : {e}")

    # Envoi SMTP
    try:
        if smtp_tls:
            server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
            server.ehlo()
            server.starttls()
            server.ehlo()
        else:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30)

        if login and mdp:
            server.login(login, mdp)

        server.sendmail(expediteur, destins, msg.as_string())
        server.quit()

        log.info(f"Mail envoyé → {', '.join(destins)}")
        return True

    except Exception as e:
        log.error(f"Erreur envoi mail : {e}")
        return False


# ── Planificateur ─────────────────────────────────────────────────────────────

def _doit_lancer(cfg: dict, maintenant: datetime) -> bool:
    """
    Retourne True si on doit lancer le pipeline maintenant.
    Vérifie l'heure et le(s) jour(s) configurés.
    """
    sched_cfg  = cfg.get("scheduler", {})
    heure_str  = sched_cfg.get("heure", "10:00")
    frequence  = sched_cfg.get("frequence", "quotidien").lower()

    # Vérifier l'heure (à la minute près)
    try:
        h, m = map(int, heure_str.split(":"))
    except ValueError:
        log.error(f"Heure invalide dans config : '{heure_str}' — format attendu HH:MM")
        return False

    if maintenant.hour != h or maintenant.minute != m:
        return False

    # Vérifier le jour
    JOURS = {
        "lundi":    0, "mardi":    1, "mercredi": 2,
        "jeudi":    3, "vendredi": 4, "samedi":   5, "dimanche": 6,
    }
    jour_actuel = maintenant.weekday()

    if frequence == "quotidien":
        return True

    # Supporte plusieurs jours séparés par virgule : "lundi,vendredi"
    jours_voulus = [j.strip() for j in frequence.split(",")]
    return any(JOURS.get(j) == jour_actuel for j in jours_voulus)


def _boucle_planificateur(config_path: str):
    """
    Boucle principale — vérifie toutes les 30 secondes si l'heure est venue.
    Evite les doubles déclenchements dans la même minute.
    """
    log.info("Planificateur démarré — Ctrl+C pour arrêter")
    derniere_execution = None

    while True:
        try:
            cfg        = _charger_config(config_path)  # rechargé à chaque tour
            maintenant = datetime.now().replace(second=0, microsecond=0)

            if _doit_lancer(cfg, maintenant) and maintenant != derniere_execution:
                derniere_execution = maintenant
                log.info(f"Déclenchement planifié — {maintenant.strftime('%d/%m/%Y %H:%M')}")
                _executer(config_path, cfg)

        except KeyboardInterrupt:
            log.info("Planificateur arrêté")
            break
        except Exception as e:
            log.error(f"Erreur planificateur : {e}")

        time.sleep(30)


def _executer(config_path: str, cfg: dict):
    """Lance le pipeline et envoie le mail — gère les erreurs."""
    try:
        resultat = _lancer_pipeline(config_path, cfg)
        _envoyer_mail(cfg, resultat)
        log.info("Cycle complet terminé ✓")

    except Exception as e:
        log.error(f"Échec du cycle : {e}")
        _envoyer_mail(cfg, {"quinzaine": _quinzaine_courante(cfg), "kpis": {}}, erreur=str(e))


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Planificateur Project Intelligence")
    parser.add_argument("--config",     default="config.yaml")
    parser.add_argument("--now",        action="store_true",
                        help="Exécuter immédiatement sans attendre l'heure planifiée")
    parser.add_argument("--test-mail",  action="store_true",
                        help="Envoyer un mail de test sans lancer le pipeline")
    args = parser.parse_args()

    cfg = _charger_config(args.config)

    sched_cfg = cfg.get("scheduler", {})
    log.info(f"Configuration planificateur :")
    log.info(f"  Heure      : {sched_cfg.get('heure', '10:00')}")
    log.info(f"  Fréquence  : {sched_cfg.get('frequence', 'quotidien')}")
    log.info(f"  PDF        : {sched_cfg.get('pdf', True)}")
    log.info(f"  Mail actif : {sched_cfg.get('mail', {}).get('actif', False)}")

    if args.test_mail:
        log.info("Test mail...")
        resultat = {
            "quinzaine":   _quinzaine_courante(cfg),
            "kpis":        {"nb_projets_actifs": "TEST", "nb_en_retard": "—",
                            "nb_at_risk": "—", "avancement_moyen": "—"},
            "chemin_html": Path(cfg.get("paths", {}).get("dashboard_out", "frontend/dashboard.html")),
            "chemins_pdf": [],
            "duree_s":     0,
        }
        ok = _envoyer_mail(cfg, resultat)
        print("Mail de test envoyé ✓" if ok else "Échec envoi mail ✗")
        return

    if args.now:
        log.info("Exécution immédiate (--now)")
        _executer(args.config, cfg)
        return

    _boucle_planificateur(args.config)


if __name__ == "__main__":
    main()
