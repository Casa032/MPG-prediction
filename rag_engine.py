"""
rag_engine.py
=============
Moteur de requêtes en langage naturel sur les données de monitoring.

Stratégie : RAG sans base vectorielle
    Au lieu de ChromaDB (qui nécessite une installation), ce module
    construit dynamiquement un contexte textuel depuis le Parquet
    et l'envoie au LLM interne avec la question.

    Avantage : zéro dépendance supplémentaire, fonctionne immédiatement.
    Le contexte est toujours à jour car lu depuis le Parquet en temps réel.

Rôle dans le pipeline :
    storage.py → rag_engine.py → html_generator.py (--llm)
                              → api/main.py (POST /api/chat)

Usage standalone :
    python query/rag_engine.py
    python query/rag_engine.py --question "quels projets sont en retard ?"
    python query/rag_engine.py --quinzaine Q1_2025_S2 --question "résume l'avancement"

Intégration dans html_generator.py :
    from query.rag_engine import RagEngine
    rag = RagEngine()
    reponses = rag.pre_generer(questions=[...], quinzaine="Q1_2025_S2")
"""

import sys
import json
import yaml
import logging
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from storage.storage import StorageManager

log = logging.getLogger(__name__)

# ── Questions pré-générées pour le dashboard HTML ─────────────────────────────
# Ces questions sont posées automatiquement par html_generator.py --llm
# pour pré-remplir le cache du Chat sans serveur.
QUESTIONS_STANDARD = [
    "quels projets sont en retard ?",
    "quels projets sont à risque ?",
    "quelles décisions ont été prises ?",
    "y a-t-il des blocages actifs ?",
    "résume l'avancement global de la quinzaine",
    "quelles actions sont à mener en priorité ?",
    "quel est le projet le plus avancé ?",
    "quel est le projet le plus en difficulté ?",
]


# ── Classe principale ──────────────────────────────────────────────────────────

class RagEngine:
    """
    Moteur de requêtes LLM sur les données de monitoring.

    Utilisation :
        from query.rag_engine import RagEngine
        rag = RagEngine()
        reponse = rag.query("quels projets sont en retard ?")
    """

    def __init__(self, config_path="config.yaml"):
        self.sm     = StorageManager(config_path)
        self.config = self._charger_config(config_path)
        self.llm_cfg = self.config.get("llm", {})

        # Endpoint LLM interne (compatible OpenAI /v1/chat/completions)
        self.endpoint    = self.llm_cfg.get("endpoint", "http://localhost:11434/v1")
        self.model       = self.llm_cfg.get("model", "mistral")
        self.max_tokens  = self.llm_cfg.get("max_tokens", 1000)
        self.temperature = self.llm_cfg.get("temperature", 0.2)

        log.info(f"RagEngine — endpoint : {self.endpoint} — modèle : {self.model}")

    def _charger_config(self, path) -> dict:
        p = Path(path)
        return yaml.safe_load(p.read_text()) if p.exists() else {}

    # ── Construction du contexte ───────────────────────────────────────────────

    def construire_contexte(self, quinzaine: str | None = None) -> str:
        """
        Construit un contexte textuel structuré depuis le Parquet.
        Ce contexte est injecté dans le prompt LLM avec la question.

        Format : texte compact lisible par le LLM, ~1000-2000 tokens selon
        le nombre de projets.
        """
        q = quinzaine or (self.sm.lister_quinzaines() or [""])[-1]
        df = self.sm.charger_quinzaines(quinzaines=[q])

        if df.empty:
            return f"Aucune donnée disponible pour la quinzaine {q}."

        kpis = self.sm.kpis(quinzaine=q)
        lignes = []

        # En-tête avec KPIs
        lignes.append(f"=== MONITORING PROJETS — {q} ===")
        lignes.append(
            f"Résumé : {kpis.get('nb_projets_actifs',0)} projets actifs, "
            f"{kpis.get('nb_en_retard',0)} en retard, "
            f"{kpis.get('nb_at_risk',0)} à risque, "
            f"avancement moyen {kpis.get('avancement_moyen',0)}%"
        )
        lignes.append("")

        # Détail par projet
        for _, row in df.iterrows():
            bloc = [
                f"PROJET : {row.get('projet_nom','')} ({row.get('projet_id','')})",
                f"  Statut       : {row.get('statut','')}",
                f"  Avancement   : {row.get('avancement_pct',0)}%",
                f"  Responsable  : {row.get('responsable_principal','')}",
                f"  Effectifs    : {row.get('effectifs','')}",
                f"  Phase        : {row.get('phase','')}",
                f"  Livrable     : {row.get('livrable_quinzaine','')} → {row.get('livrable_statut','')}",
            ]
            # Ajouter les champs optionnels s'ils ne sont pas vides
            for champ, label in [
                ("decisions",      "  Décisions     :"),
                ("actions_a_mener","  Actions        :"),
                ("risques",        "  Risques        :"),
                ("risque_niveau",  "  Niveau risque  :"),
                ("points_blocage", "  Blocages       :"),
                ("commentaire_libre","  Commentaire   :"),
            ]:
                val = str(row.get(champ, "") or "").strip()
                if val:
                    bloc.append(f"{label} {val}")

            lignes.extend(bloc)
            lignes.append("")

        return "\n".join(lignes)

    def construire_contexte_historique(self, projet_id: str) -> str:
        """
        Contexte historique d'un projet sur toutes les quinzaines.
        Utilisé pour les questions portant sur l'évolution d'un projet.
        """
        df = self.sm.projet(projet_id)
        if df.empty:
            return f"Aucune donnée pour le projet {projet_id}."

        nom = df["projet_nom"].iloc[0] if "projet_nom" in df.columns else projet_id
        lignes = [f"=== HISTORIQUE : {nom} ({projet_id}) ===", ""]

        for _, row in df.iterrows():
            lignes.append(f"[{row.get('quinzaine','')}]")
            lignes.append(f"  Statut : {row.get('statut','')} — {row.get('avancement_pct',0)}%")
            for champ, label in [
                ("livrable_quinzaine", "  Livrable   :"),
                ("decisions",          "  Décisions  :"),
                ("actions_a_mener",    "  Actions    :"),
                ("points_blocage",     "  Blocages   :"),
            ]:
                val = str(row.get(champ, "") or "").strip()
                if val:
                    lignes.append(f"{label} {val}")
            lignes.append("")

        return "\n".join(lignes)

    # ── Appel LLM ─────────────────────────────────────────────────────────────

    def _appeler_llm(self, systeme: str, utilisateur: str) -> str:
        """
        Appelle le LLM interne via l'API compatible OpenAI.
        Retourne la réponse textuelle ou un message d'erreur.
        """
        try:
            import urllib.request
            import urllib.error

            payload = json.dumps({
                "model":       self.model,
                "max_tokens":  self.max_tokens,
                "temperature": self.temperature,
                "messages": [
                    {"role": "system",    "content": systeme},
                    {"role": "user",      "content": utilisateur},
                ],
            }).encode("utf-8")

            req = urllib.request.Request(
                f"{self.endpoint}/chat/completions",
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    # Ajouter ici la clé API si nécessaire :
                    # "Authorization": f"Bearer {self.llm_cfg.get('api_key', '')}",
                },
                method="POST",
            )

            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                return data["choices"][0]["message"]["content"].strip()

        except Exception as e:
            log.error(f"Erreur LLM : {e}")
            return f"[LLM indisponible : {e}]"

    # ── Requête principale ────────────────────────────────────────────────────

    def query(self, question: str, quinzaine: str | None = None) -> str:
        """
        Répond à une question en langage naturel sur les projets.

        Étapes :
            1. Construire le contexte textuel depuis le Parquet
            2. Détecter si la question porte sur un projet spécifique
            3. Envoyer contexte + question au LLM
            4. Retourner la réponse

        Paramètres :
            question  : question en langage naturel
            quinzaine : quinzaine ciblée (None = dernière)
        """
        q = quinzaine or (self.sm.lister_quinzaines() or [""])[-1]

        # Détecter si la question mentionne un projet_id spécifique
        projets = self.sm.lister_projets()
        projet_mentionne = None
        for p in projets:
            pid = str(p.get("projet_id", "")).lower()
            nom = str(p.get("projet_nom", "")).lower()
            if pid in question.lower() or nom in question.lower():
                projet_mentionne = p.get("projet_id")
                break

        # Construire le contexte approprié
        if projet_mentionne:
            contexte = self.construire_contexte_historique(projet_mentionne)
        else:
            contexte = self.construire_contexte(q)

        # Prompt système
        systeme = (
            "Tu es un assistant de monitoring de projets. "
            "Tu réponds en français, de manière concise et structurée. "
            "Tu bases tes réponses uniquement sur les données fournies. "
            "Si une information n'est pas dans les données, tu le dis clairement. "
            "Tu utilises des tirets pour les listes, jamais de markdown complexe."
        )

        # Prompt utilisateur avec contexte injecté
        utilisateur = (
            f"Données de monitoring :\n\n{contexte}\n\n"
            f"Question : {question}"
        )

        log.info(f"LLM query — quinzaine: {q} — question: {question[:60]}...")
        return self._appeler_llm(systeme, utilisateur)

    # ── Pré-génération pour le dashboard HTML ─────────────────────────────────

    def pre_generer(self,
                    questions: list | None = None,
                    quinzaine: str | None  = None) -> dict:
        """
        Pré-génère les réponses LLM pour le cache du dashboard HTML.
        Appelé par html_generator.py --llm avant la génération du fichier.

        Retourne un dict { question: réponse } prêt à être injecté en JSON.
        """
        qs = questions or QUESTIONS_STANDARD
        q  = quinzaine or (self.sm.lister_quinzaines() or [""])[-1]

        cache = {}
        total = len(qs)

        print(f"\nPré-génération LLM — {total} questions — quinzaine : {q}")
        print("-" * 55)

        for i, question in enumerate(qs, 1):
            print(f"[{i}/{total}] {question[:60]}...")
            try:
                reponse = self.query(question, quinzaine=q)
                cache[question.lower().strip()] = reponse
                print(f"       → {reponse[:80]}...")
            except Exception as e:
                cache[question.lower().strip()] = f"[Erreur : {e}]"
                log.error(f"Erreur pré-génération '{question}' : {e}")

        print("-" * 55)
        print(f"Cache LLM généré : {len(cache)} réponses\n")
        return cache

    def tester_connexion(self) -> bool:
        """
        Vérifie que le LLM interne répond.
        Affiche un message clair selon le résultat.
        """
        print(f"Test connexion LLM : {self.endpoint} — modèle : {self.model}")
        reponse = self._appeler_llm(
            systeme="Tu es un assistant. Réponds en un mot.",
            utilisateur="Dis juste 'OK'."
        )
        ok = not reponse.startswith("[LLM indisponible")
        print(f"Résultat : {'✓ Connecté' if ok else '✗ Échec — ' + reponse}")
        return ok


# ── Intégration dans html_generator ───────────────────────────────────────────

def enrichir_html_generator(config_path="config.yaml",
                             quinzaine: str | None = None,
                             questions: list | None = None) -> dict:
    """
    Fonction d'intégration appelée par html_generator.py --llm.
    Retourne le cache LLM à injecter dans le HTML.

    Dans html_generator.py, ajouter :
        if args.llm:
            from query.rag_engine import enrichir_html_generator
            llm_cache = enrichir_html_generator(args.config, quinzaine=args.quinzaine)
        else:
            llm_cache = {}
        html = generer_html(donnees, llm_reponses=llm_cache)
    """
    rag = RagEngine(config_path)
    return rag.pre_generer(questions=questions, quinzaine=quinzaine)


# ── Point d'entrée ─────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Moteur RAG — requêtes LLM sur les projets.")
    parser.add_argument("--question",  "-q", default=None, help="Question à poser")
    parser.add_argument("--quinzaine", "-Q", default=None, help="Quinzaine ciblée")
    parser.add_argument("--pre-gen",   action="store_true", help="Pré-générer toutes les questions standard")
    parser.add_argument("--test",      action="store_true", help="Tester la connexion LLM")
    parser.add_argument("--contexte",  action="store_true", help="Afficher le contexte construit")
    parser.add_argument("--config",    default="config.yaml")
    args = parser.parse_args()

    rag = RagEngine(args.config)

    if args.test:
        rag.tester_connexion()
        return

    if args.contexte:
        print(rag.construire_contexte(args.quinzaine))
        return

    if args.pre_gen:
        cache = rag.pre_generer(quinzaine=args.quinzaine)
        # Sauvegarder le cache dans un fichier JSON
        chemin = Path("storage/llm_cache.json")
        chemin.parent.mkdir(exist_ok=True)
        chemin.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Cache sauvegardé : {chemin}")
        return

    if args.question:
        print(f"\nQuestion : {args.question}")
        print("-" * 55)
        reponse = rag.query(args.question, quinzaine=args.quinzaine)
        print(reponse)
        print()
        return

    # Mode interactif si aucun argument
    print("\nMode interactif — tape 'exit' pour quitter")
    print(f"Quinzaine : {args.quinzaine or 'dernière disponible'}")
    print("-" * 55)
    while True:
        try:
            question = input("\nQuestion : ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if question.lower() in ("exit", "quit", "q"):
            break
        if not question:
            continue
        reponse = rag.query(question, quinzaine=args.quinzaine)
        print(f"\nRéponse : {reponse}")


if __name__ == "__main__":
    main()
