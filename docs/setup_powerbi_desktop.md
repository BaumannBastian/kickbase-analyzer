# Power BI Desktop Setup (Lokal, ohne Service API)

## Ziel
Power-BI-Dashboards lokal entwickeln, auf BigQuery MARTS aufsetzen und M/DAX/TMDL-Artefakte reproduzierbar im Repo pflegen, ohne Power-BI-Service-API.

## 1) Voraussetzungen
1) BigQuery MARTS sind gebaut (`kickbase_marts` Dataset vorhanden).
2) Power BI Desktop ist lokal installiert.
3) Zugriff auf das GCP-Projekt ist eingerichtet (Google Login in Desktop bei der BigQuery-Verbindung).

## 2) Lokales Asset-Pack exportieren
Im Repo:

```bash
python -m scripts.powerbi_desktop.export_desktop_assets --project kickbase-analyzer
```

Output:
- `dashboards/powerbi/local/desktop_pack_<timestamp>/bigquery_marts_queries.pq`
- `dashboards/powerbi/local/desktop_pack_<timestamp>/measures.dax`
- `dashboards/powerbi/local/desktop_pack_<timestamp>/model.tmdl`

Hinweis: `dashboards/powerbi/local/` ist in `.gitignore`.

## 3) Power BI Desktop mit BigQuery MARTS verbinden
1) Power BI Desktop -> `Daten abrufen` -> `Google BigQuery`.
2) Mit demselben Google-Konto anmelden, das Zugriff auf `kickbase-analyzer` hat.
3) Entweder:
   - Tabellen im Navigator direkt waehlen (`kickbase_marts.*`), oder
   - leere Abfrage anlegen und Inhalt aus `bigquery_marts_queries.pq` in den erweiterten Editor uebernehmen.
4) `Schließen & laden`.

## 4) DAX-Measures anwenden
1) Modellansicht oeffnen.
2) Fuer passende Tabellen neue Measures anlegen.
3) Ausdrucke aus `measures.dax` uebernehmen.

## 5) Optional: TMDL/PBIP Workflow
Wenn du mit PBIP/TMDL arbeiten willst:
1) Projekt als PBIP speichern.
2) `model.tmdl` als Ausgangsbasis fuer modellierte Measures/Annotationen nutzen.
3) PBIP-Quellen koennen versioniert werden; lokale Tests weiterhin in Desktop.

## 6) Empfohlene Repo-Strategie
- Versioniert:
  - `dashboards/powerbi/templates/*` (generische M/DAX/TMDL-Templates)
- Nicht versioniert:
  - `dashboards/powerbi/local/*` (dein lokaler, iterativer Desktop-Stand)
  - `.pbix` Dateien

