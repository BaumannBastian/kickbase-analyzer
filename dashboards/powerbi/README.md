# ------------------------------------
# README.md
#
# Dieser Ordner enthaelt versionierte Power-BI-Desktop-Templates
# (M, DAX, TMDL) fuer den lokalen Dashboard-Workflow auf Basis
# der BigQuery MARTS Views.
#
# Usage
# ------------------------------------
# 1) Asset-Pack exportieren:
#    python -m scripts.powerbi_desktop.export_desktop_assets --project kickbase-analyzer
# 2) Inhalte aus dashboards/powerbi/local/desktop_pack_*/ in Power BI Desktop verwenden.
# ------------------------------------

# Power BI Desktop Workflow (ohne Service API)

## Ziel
Reproduzierbare Desktop-Dashboard-Arbeit ohne Power-BI-Service-API:
- Datenquelle: `kickbase-analyzer.kickbase_marts`
- Modellierung: M + DAX
- Optional: TMDL/PBIP-Dateien fuer modellgetriebene Versionierung

## Struktur
- `templates/bigquery_marts_queries.pq`
  M-Queries fuer MARTS-Views (`mart_player_leaderboard`, `mart_points_breakdown`, `mart_risk_overview`, `mart_marketvalue_dashboard`)
- `templates/measures.dax`
  Basis-Measures fuer KPI Cards und Visuals
- `templates/model.tmdl`
  TMDL-Template mit denselben Measures
- `local/`
  Lokale, nicht versionierte Desktop-Packs (durch `.gitignore` ausgeschlossen)

## Empfohlener Ablauf
1) BigQuery MARTS aktualisieren:
   `python -m bigquery.core_transform.apply_views_with_bq_cli --project kickbase-analyzer`
2) Desktop-Pack exportieren:
   `python -m scripts.powerbi_desktop.export_desktop_assets --project kickbase-analyzer`
3) In Power BI Desktop:
   - Leere Abfrage erstellen und `bigquery_marts_queries.pq` einfuellen.
   - Measures aus `measures.dax` in die jeweiligen Tabellen eintragen.
4) Optional:
   - PBIP-Projekt lokal in `dashboards/powerbi/local/` speichern.

