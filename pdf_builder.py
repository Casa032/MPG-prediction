"""
pdf_builder.py
==============
Génère des rapports PDF professionnels depuis les données Parquet.
Utilise ReportLab — aucun serveur, aucune dépendance externe.

Types de rapports disponibles :
    quinzaine  → synthèse complète d'une quinzaine (KPIs + tableau projets)
    projet     → historique complet d'un projet sur toutes les quinzaines
    delta      → comparaison entre deux quinzaines

Usage :
    python reporting/pdf_builder.py --type quinzaine --quinzaine Q1_2025_S2
    python reporting/pdf_builder.py --type projet --projet PROJ-001
    python reporting/pdf_builder.py --type delta --q-avant Q1_2025_S1 --q-apres Q1_2025_S2

Intégration dans html_generator.py / api :
    from reporting.pdf_builder import PdfBuilder
    builder = PdfBuilder()
    chemin = builder.rapport_quinzaine("Q1_2025_S2")
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from storage.storage import StorageManager

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether,
)
from reportlab.graphics.shapes import Drawing, Rect
from reportlab.graphics import renderPDF

log = logging.getLogger(__name__)

# ── Palette couleurs (cohérente avec le dashboard HTML) ───────────────────────
C_ACCENT    = colors.HexColor("#1abc9c")   # teal
C_BG_DARK   = colors.HexColor("#0d0f14")
C_SURFACE   = colors.HexColor("#1e2230")
C_BORDER    = colors.HexColor("#2a2f40")
C_TEXT      = colors.HexColor("#e8eaf0")
C_MUTED     = colors.HexColor("#6b7280")
C_ON_TRACK  = colors.HexColor("#1abc9c")
C_AT_RISK   = colors.HexColor("#f59e0b")
C_LATE      = colors.HexColor("#ef4444")
C_DONE      = colors.HexColor("#6366f1")
C_ON_HOLD   = colors.HexColor("#6b7280")
C_WHITE     = colors.white
C_LIGHT_BG  = colors.HexColor("#f8fafc")   # fond tableau clair
C_ROW_ALT   = colors.HexColor("#f1f5f9")   # ligne alternée
C_HEADER_BG = colors.HexColor("#1e293b")   # en-tête tableau

STATUT_COULEURS = {
    "ON_TRACK": C_ON_TRACK,
    "AT_RISK":  C_AT_RISK,
    "LATE":     C_LATE,
    "DONE":     C_DONE,
    "ON_HOLD":  C_ON_HOLD,
}

W, H = A4
MARGE = 18 * mm


# ── Styles typographiques ──────────────────────────────────────────────────────

def _styles():
    base = getSampleStyleSheet()
    return {
        "titre_doc": ParagraphStyle(
            "titre_doc", parent=base["Normal"],
            fontSize=22, textColor=C_WHITE, fontName="Helvetica-Bold",
            leading=28, alignment=TA_LEFT,
        ),
        "sous_titre": ParagraphStyle(
            "sous_titre", parent=base["Normal"],
            fontSize=10, textColor=colors.HexColor("#9ca3af"),
            fontName="Helvetica", leading=14, alignment=TA_LEFT,
        ),
        "section": ParagraphStyle(
            "section", parent=base["Normal"],
            fontSize=9, textColor=C_MUTED, fontName="Helvetica-Bold",
            leading=12, spaceBefore=14, spaceAfter=6,
            textTransform="uppercase", letterSpacing=1.2,
        ),
        "body": ParagraphStyle(
            "body", parent=base["Normal"],
            fontSize=9, textColor=colors.HexColor("#334155"),
            fontName="Helvetica", leading=13,
        ),
        "body_small": ParagraphStyle(
            "body_small", parent=base["Normal"],
            fontSize=8, textColor=C_MUTED,
            fontName="Helvetica", leading=11,
        ),
        "kpi_val": ParagraphStyle(
            "kpi_val", parent=base["Normal"],
            fontSize=24, textColor=C_WHITE, fontName="Helvetica-Bold",
            leading=28, alignment=TA_CENTER,
        ),
        "kpi_label": ParagraphStyle(
            "kpi_label", parent=base["Normal"],
            fontSize=7, textColor=colors.HexColor("#94a3b8"),
            fontName="Helvetica", leading=10, alignment=TA_CENTER,
            textTransform="uppercase", letterSpacing=0.8,
        ),
        "cell": ParagraphStyle(
            "cell", parent=base["Normal"],
            fontSize=8, textColor=colors.HexColor("#334155"),
            fontName="Helvetica", leading=11,
        ),
        "cell_bold": ParagraphStyle(
            "cell_bold", parent=base["Normal"],
            fontSize=8, textColor=colors.HexColor("#0f172a"),
            fontName="Helvetica-Bold", leading=11,
        ),
        "pied": ParagraphStyle(
            "pied", parent=base["Normal"],
            fontSize=7, textColor=C_MUTED, fontName="Helvetica",
            leading=10, alignment=TA_CENTER,
        ),
    }


# ── Composants réutilisables ───────────────────────────────────────────────────

def _bandeau_header(titre: str, sous_titre: str, date_gen: str) -> list:
    """Bandeau d'en-tête coloré avec titre et métadonnées."""
    st = _styles()
    elements = []

    # Fond teal en dessin
    d = Drawing(W - 2 * MARGE, 52)
    d.add(Rect(0, 0, W - 2 * MARGE, 52, fillColor=C_ACCENT, strokeColor=None))
    elements.append(d)

    # Titre par-dessus (astuce : tableau sur fond transparent)
    data = [[
        Paragraph(titre, st["titre_doc"]),
        Paragraph(
            f'<font size="7" color="#ffffff">{date_gen}</font>',
            ParagraphStyle("r", parent=st["sous_titre"], alignment=TA_RIGHT,
                           textColor=C_WHITE)
        ),
    ]]
    t = Table(data, colWidths=[W - 2 * MARGE - 80, 80])
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, -1), C_ACCENT),
        ("TOPPADDING",  (0, 0), (-1, -1), 14),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LEFTPADDING", (0, 0), (0, 0), 14),
        ("RIGHTPADDING", (-1, 0), (-1, 0), 14),
        ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
    ]))
    # On remplace le Drawing par le tableau coloré
    elements[-1] = t

    elements.append(Paragraph(sous_titre, st["sous_titre"]))
    elements.append(Spacer(1, 8))
    return elements


def _kpi_row(kpis: dict) -> list:
    """Rangée de 4 cartes KPI côte à côte."""
    st = _styles()
    items = [
        ("Projets actifs",   str(kpis.get("nb_projets_actifs", "—")),  C_ON_TRACK),
        ("En retard",        str(kpis.get("nb_en_retard", "—")),        C_LATE),
        ("À risque",         str(kpis.get("nb_at_risk", "—")),          C_AT_RISK),
        ("Avancement moy.",  f"{kpis.get('avancement_moyen', '—')}%",  C_ACCENT),
    ]
    largeur_carte = (W - 2 * MARGE - 9 * mm) / 4

    cellules = []
    for label, val, couleur in items:
        cellule = Table(
            [[Paragraph(val,   ParagraphStyle("v", parent=st["kpi_val"],   textColor=couleur))],
             [Paragraph(label, ParagraphStyle("l", parent=st["kpi_label"], textColor=C_MUTED))]],
            colWidths=[largeur_carte],
        )
        cellule.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
            ("BOX",           (0, 0), (-1, -1), 0.5, colors.HexColor("#e2e8f0")),
            ("LEFTLINEWIDTH", (0, 0), (0, -1), 3),
            ("LINECOLOR",     (0, 0), (0, -1), couleur),
            ("TOPPADDING",    (0, 0), (-1, -1), 12),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ("ROUNDEDCORNERS", [4]),
        ]))
        cellules.append(cellule)

    grille = Table([cellules], colWidths=[largeur_carte] * 4,
                   hAlign="LEFT", spaceBefore=0)
    grille.setStyle(TableStyle([
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3 * mm),
        ("TOPPADDING",   (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
        ("VALIGN",       (0, 0), (-1, -1), "TOP"),
    ]))
    return [grille, Spacer(1, 10)]


def _badge_statut(statut: str) -> Paragraph:
    """Petit badge coloré pour le statut projet."""
    couleurs_texte = {
        "ON_TRACK": "#065f46", "AT_RISK": "#92400e",
        "LATE": "#991b1b", "DONE": "#3730a3", "ON_HOLD": "#374151",
    }
    couleurs_fond = {
        "ON_TRACK": "#d1fae5", "AT_RISK": "#fef3c7",
        "LATE": "#fee2e2", "DONE": "#e0e7ff", "ON_HOLD": "#f3f4f6",
    }
    tc = couleurs_texte.get(statut, "#374151")
    bc = couleurs_fond.get(statut, "#f3f4f6")
    st = ParagraphStyle(
        "badge", fontSize=7, fontName="Helvetica-Bold",
        textColor=colors.HexColor(tc),
        backColor=colors.HexColor(bc),
        leading=10, alignment=TA_CENTER,
        borderPadding=(2, 4, 2, 4),
    )
    return Paragraph(statut, st)


def _barre_avancement(pct: float, couleur: colors.Color, largeur=40 * mm) -> Drawing:
    """Barre de progression horizontale."""
    h = 5
    d = Drawing(largeur, h)
    d.add(Rect(0, 0, largeur, h,
               fillColor=colors.HexColor("#e2e8f0"), strokeColor=None))
    fill_w = max(0, min(largeur, largeur * float(pct or 0) / 100))
    if fill_w > 0:
        d.add(Rect(0, 0, fill_w, h, fillColor=couleur, strokeColor=None))
    return d


def _tableau_projets(projets: list, st: dict,
                     colonnes=None, largeurs=None) -> Table:
    """Tableau principal des projets."""
    if colonnes is None:
        colonnes = ["ID", "Projet", "Responsable", "Statut", "%", "Livrable", "Décisions"]
    if largeurs is None:
        w = W - 2 * MARGE
        largeurs = [16*mm, 42*mm, 26*mm, 20*mm, 25*mm, 35*mm, None]
        largeurs[-1] = w - sum(x for x in largeurs[:-1])

    # En-têtes
    entetes = [Paragraph(c, ParagraphStyle(
        "th", fontSize=7, fontName="Helvetica-Bold",
        textColor=C_WHITE, leading=9, alignment=TA_LEFT,
    )) for c in colonnes]

    lignes = [entetes]
    for i, p in enumerate(projets):
        statut = str(p.get("statut", ""))
        pct    = float(p.get("avancement_pct", 0) or 0)
        couleur_statut = STATUT_COULEURS.get(statut, C_MUTED)

        ligne = [
            Paragraph(str(p.get("projet_id", "")),           st["cell_bold"]),
            Paragraph(str(p.get("projet_nom", "")),           st["cell_bold"]),
            Paragraph(str(p.get("responsable_principal", "")),st["cell"]),
            _badge_statut(statut),
            _barre_avancement(pct, couleur_statut),
            Paragraph(str(p.get("livrable_quinzaine", "") or "—"), st["cell"]),
            Paragraph(str(p.get("decisions", "") or "—"),    st["cell"]),
        ]
        lignes.append(ligne)

    t = Table(lignes, colWidths=largeurs, repeatRows=1)
    style = [
        # En-tête
        ("BACKGROUND",    (0, 0), (-1, 0), C_HEADER_BG),
        ("TOPPADDING",    (0, 0), (-1, 0), 6),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        # Lignes alternées
        *[("BACKGROUND", (0, i), (-1, i), C_ROW_ALT)
          for i in range(2, len(lignes), 2)],
        # Bordures légères
        ("LINEBELOW",  (0, 0), (-1, -1), 0.3, colors.HexColor("#e2e8f0")),
        ("TOPPADDING", (0, 1), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 5),
        ("ROUNDEDCORNERS", [3]),
    ]
    t.setStyle(TableStyle(style))
    return t


def _pied_de_page(doc_title: str, date_gen: str) -> list:
    """Pied de page standard."""
    st = _styles()
    return [
        Spacer(1, 8),
        HRFlowable(width="100%", thickness=0.3,
                   color=colors.HexColor("#e2e8f0")),
        Spacer(1, 4),
        Paragraph(
            f"Project Intelligence · {doc_title} · Généré le {date_gen}",
            st["pied"]
        ),
    ]


# ── Classe principale ──────────────────────────────────────────────────────────

class PdfBuilder:
    """
    Génère des rapports PDF depuis les données Parquet.

    Utilisation :
        from reporting.pdf_builder import PdfBuilder
        builder = PdfBuilder()

        # Rapport quinzaine
        chemin = builder.rapport_quinzaine("Q1_2025_S2")

        # Rapport projet
        chemin = builder.rapport_projet("PROJ-001")

        # Rapport delta
        chemin = builder.rapport_delta("Q1_2025_S1", "Q1_2025_S2")
    """

    def __init__(self, config_path="config.yaml"):
        self.sm       = StorageManager(config_path)
        self.dossier  = Path("reporting/output")
        self.dossier.mkdir(parents=True, exist_ok=True)
        self.date_gen = datetime.now().strftime("%d/%m/%Y à %H:%M")

    def _doc(self, chemin: Path) -> SimpleDocTemplate:
        return SimpleDocTemplate(
            str(chemin),
            pagesize=A4,
            leftMargin=MARGE, rightMargin=MARGE,
            topMargin=MARGE,  bottomMargin=MARGE,
            title="Project Intelligence",
            author="Project Intelligence",
        )

    # ── Rapport quinzaine ──────────────────────────────────────────────────────

    def rapport_quinzaine(self, quinzaine: str | None = None) -> Path:
        """
        Génère un rapport PDF complet d'une quinzaine.
        Contient : KPIs, tableau des projets, décisions, blocages.

        Retourne le chemin du fichier PDF généré.
        """
        q = quinzaine or (self.sm.lister_quinzaines() or [""])[-1]
        df = self.sm.charger_quinzaines(quinzaines=[q])
        kpis = self.sm.kpis(quinzaine=q)

        if df.empty:
            log.error(f"Aucune donnée pour la quinzaine '{q}'")
            return None

        chemin = self.dossier / f"rapport_{q}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        doc    = self._doc(chemin)
        st     = _styles()
        story  = []

        # En-tête
        story += _bandeau_header(
            titre=f"Rapport de Quinzaine — {q}",
            sous_titre=f"Synthèse du monitoring projets · {len(df)} projet(s)",
            date_gen=self.date_gen,
        )

        # KPIs
        story.append(Paragraph("Indicateurs clés", st["section"]))
        story += _kpi_row(kpis)

        # Tableau projets
        story.append(Paragraph("Projets", st["section"]))
        projets = df.where(df.notna(), None).to_dict(orient="records")
        story.append(_tableau_projets(projets, st))
        story.append(Spacer(1, 12))

        # Section décisions
        dec = [(p["projet_nom"], p["decisions"])
               for p in projets
               if p.get("decisions") and str(p["decisions"]).strip()]
        if dec:
            story.append(KeepTogether([
                Paragraph("Décisions prises", st["section"]),
                *[Paragraph(f"<b>{nom}</b> — {d}", st["body"])
                  for nom, d in dec],
                Spacer(1, 4),
            ]))

        # Section blocages
        bloc = [(p["projet_nom"], p["points_blocage"])
                for p in projets
                if p.get("points_blocage") and str(p["points_blocage"]).strip()]
        if bloc:
            story.append(KeepTogether([
                Paragraph("Points de blocage", st["section"]),
                *[Paragraph(
                    f'<font color="#ef4444"><b>{nom}</b></font> — {b}',
                    st["body"])
                  for nom, b in bloc],
                Spacer(1, 4),
            ]))

        # Actions à mener
        actions = [(p["projet_nom"], p["actions_a_mener"], p.get("actions_responsable",""),
                    p.get("actions_echeance",""))
                   for p in projets
                   if p.get("actions_a_mener") and str(p["actions_a_mener"]).strip()]
        if actions:
            story.append(KeepTogether([
                Paragraph("Actions à mener", st["section"]),
                *[Paragraph(
                    f"<b>{nom}</b> — {a}"
                    + (f" <font color='#6b7280'>(resp: {r})</font>" if r else "")
                    + (f" <font color='#f59e0b'>→ {e}</font>" if e else ""),
                    st["body"])
                  for nom, a, r, e in actions],
                Spacer(1, 4),
            ]))

        story += _pied_de_page(f"Quinzaine {q}", self.date_gen)
        doc.build(story)
        log.info(f"PDF généré : {chemin}")
        return chemin

    # ── Rapport projet ─────────────────────────────────────────────────────────

    def rapport_projet(self, projet_id: str) -> Path:
        """
        Génère un rapport PDF de l'historique complet d'un projet.
        Contient : fiche projet, évolution quinzaine par quinzaine.

        Retourne le chemin du fichier PDF généré.
        """
        df = self.sm.projet(projet_id)
        if df.empty:
            log.error(f"Projet '{projet_id}' introuvable")
            return None

        nom = df["projet_nom"].iloc[0] if "projet_nom" in df.columns else projet_id
        chemin = self.dossier / f"rapport_{projet_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        doc    = self._doc(chemin)
        st     = _styles()
        story  = []

        # En-tête
        story += _bandeau_header(
            titre=f"{nom}",
            sous_titre=f"{projet_id} · {len(df)} quinzaine(s) · "
                       f"Responsable : {df['responsable_principal'].iloc[0] if 'responsable_principal' in df.columns else '—'}",
            date_gen=self.date_gen,
        )

        # Fiche synthèse (dernière quinzaine)
        last = df.iloc[-1]
        story.append(Paragraph("Situation actuelle", st["section"]))
        infos = [
            ("Domaine",   last.get("domaine", "—")),
            ("Phase",     last.get("phase", "—")),
            ("Effectifs", last.get("effectifs", "—")),
            ("Statut",    last.get("statut", "—")),
            ("Avancement",f"{last.get('avancement_pct', 0)}%"),
        ]
        tbl_infos = Table(
            [[Paragraph(k, st["body_small"]), Paragraph(str(v), st["cell_bold"])]
             for k, v in infos],
            colWidths=[35 * mm, W - 2 * MARGE - 35 * mm],
        )
        tbl_infos.setStyle(TableStyle([
            ("LINEBELOW",    (0, 0), (-1, -1), 0.3, colors.HexColor("#e2e8f0")),
            ("TOPPADDING",   (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
            ("LEFTPADDING",  (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ]))
        story.append(tbl_infos)
        story.append(Spacer(1, 10))

        # Historique quinzaine par quinzaine
        story.append(Paragraph("Historique", st["section"]))
        projets = df.where(df.notna(), None).to_dict(orient="records")

        colonnes = ["Quinzaine", "Statut", "%", "Livrable", "Décisions", "Blocages"]
        w = W - 2 * MARGE
        largeurs = [30*mm, 22*mm, 22*mm, 38*mm, None, 38*mm]
        largeurs[4] = w - sum(x for x in largeurs if x)

        entetes = [Paragraph(c, ParagraphStyle(
            "th", fontSize=7, fontName="Helvetica-Bold",
            textColor=C_WHITE, leading=9,
        )) for c in colonnes]

        lignes = [entetes]
        for i, row in enumerate(projets):
            statut  = str(row.get("statut", ""))
            pct     = float(row.get("avancement_pct", 0) or 0)
            couleur = STATUT_COULEURS.get(statut, C_MUTED)
            lignes.append([
                Paragraph(str(row.get("quinzaine", "")), st["cell_bold"]),
                _badge_statut(statut),
                _barre_avancement(pct, couleur, 20 * mm),
                Paragraph(str(row.get("livrable_quinzaine", "") or "—"), st["cell"]),
                Paragraph(str(row.get("decisions", "") or "—"), st["cell"]),
                Paragraph(str(row.get("points_blocage", "") or ""), ParagraphStyle(
                    "bloc", parent=st["cell"],
                    textColor=colors.HexColor("#ef4444") if row.get("points_blocage") else C_MUTED,
                )),
            ])

        t = Table(lignes, colWidths=largeurs, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), C_HEADER_BG),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 5),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            *[("BACKGROUND", (0, i), (-1, i), C_ROW_ALT)
              for i in range(2, len(lignes), 2)],
            ("LINEBELOW",  (0, 0), (-1, -1), 0.3, colors.HexColor("#e2e8f0")),
        ]))
        story.append(t)

        story += _pied_de_page(nom, self.date_gen)
        doc.build(story)
        log.info(f"PDF généré : {chemin}")
        return chemin

    # ── Rapport delta ──────────────────────────────────────────────────────────

    def rapport_delta(self, q_avant: str, q_apres: str) -> Path:
        """
        Génère un rapport PDF de comparaison entre deux quinzaines.
        Met en évidence les projets qui ont progressé, régressé ou bloqué.

        Retourne le chemin du fichier PDF généré.
        """
        df = self.sm.delta_quinzaines(q_avant, q_apres)
        if df.empty:
            log.error(f"Impossible de comparer {q_avant} et {q_apres}")
            return None

        chemin = self.dossier / f"delta_{q_avant}_{q_apres}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
        doc    = self._doc(chemin)
        st     = _styles()
        story  = []

        story += _bandeau_header(
            titre=f"Comparaison Quinzaines",
            sous_titre=f"{q_avant}  →  {q_apres} · {len(df)} projet(s)",
            date_gen=self.date_gen,
        )

        story.append(Paragraph("Évolution par projet", st["section"]))

        colonnes  = ["Projet", "Statut avant", "Statut après",
                     "Avant", "Après", "Progression"]
        w         = W - 2 * MARGE
        largeurs  = [48*mm, 24*mm, 24*mm, 18*mm, 18*mm, None]
        largeurs[-1] = w - sum(x for x in largeurs[:-1])

        entetes = [Paragraph(c, ParagraphStyle(
            "th", fontSize=7, fontName="Helvetica-Bold", textColor=C_WHITE, leading=9,
        )) for c in colonnes]

        lignes = [entetes]
        records = df.where(df.notna(), None).to_dict(orient="records")
        for i, row in enumerate(records):
            delta_v = float(row.get("delta_avancement", 0) or 0)
            av_av   = float(row.get("avancement_avant", 0) or 0)
            av_ap   = float(row.get("avancement_apres", 0) or 0)
            signe   = "+" if delta_v > 0 else ""
            hex_delta = ("#1abc9c" if delta_v > 0
                         else "#ef4444" if delta_v < 0
                         else "#6b7280")
            lignes.append([
                Paragraph(str(row.get("projet_nom", "")),  st["cell_bold"]),
                _badge_statut(str(row.get("statut_avant", "") or "")),
                _badge_statut(str(row.get("statut_apres", "") or "")),
                Paragraph(f"{int(av_av)}%", st["cell"]),
                Paragraph(f"{int(av_ap)}%", st["cell"]),
                Paragraph(
                    f'<font color="{hex_delta}"><b>{signe}{int(delta_v)}%</b></font>',
                    st["cell"],
                ),
            ])

        t = Table(lignes, colWidths=largeurs, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), C_HEADER_BG),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 5),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 5),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            *[("BACKGROUND", (0, i), (-1, i), C_ROW_ALT)
              for i in range(2, len(lignes), 2)],
            ("LINEBELOW",  (0, 0), (-1, -1), 0.3, colors.HexColor("#e2e8f0")),
        ]))
        story.append(t)

        story += _pied_de_page(f"Delta {q_avant} → {q_apres}", self.date_gen)
        doc.build(story)
        log.info(f"PDF généré : {chemin}")
        return chemin


# ── Point d'entrée ─────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Génère des rapports PDF.")
    parser.add_argument("--type",       default="quinzaine",
                        choices=["quinzaine", "projet", "delta"],
                        help="Type de rapport")
    parser.add_argument("--quinzaine",  default=None, help="Quinzaine (ex: Q1_2025_S2)")
    parser.add_argument("--projet",     default=None, help="ID projet (ex: PROJ-001)")
    parser.add_argument("--q-avant",    default=None, help="Quinzaine de référence (delta)")
    parser.add_argument("--q-apres",    default=None, help="Quinzaine à comparer (delta)")
    parser.add_argument("--config",     default="config.yaml")
    args = parser.parse_args()

    builder = PdfBuilder(args.config)

    if args.type == "quinzaine":
        chemin = builder.rapport_quinzaine(args.quinzaine)
    elif args.type == "projet":
        if not args.projet:
            print("--projet requis pour le type 'projet'")
            return
        chemin = builder.rapport_projet(args.projet)
    elif args.type == "delta":
        if not args.q_avant or not args.q_apres:
            print("--q-avant et --q-apres requis pour le type 'delta'")
            return
        chemin = builder.rapport_delta(args.q_avant, args.q_apres)

    if chemin:
        print(f"\nRapport généré : {chemin.resolve()}\n")


if __name__ == "__main__":
    main()
