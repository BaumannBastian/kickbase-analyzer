# ml_workflow.md

# Kickbase Analyzer - ML Workflow (v0.9 -> v1.0)

## 1) Zielbild
Wir bauen eine hierarchische Prognosepipeline, die fuer jeden Spieler das naechste Spiel bewertet.

Geplante Dashboard-Werte pro Spieler:
- Startelf-Wahrscheinlichkeit
- Einwechsel-Wahrscheinlichkeit
- Erwartete Minuten
- Punktprognose naechstes Spiel
- Punkte bei vollem Spiel
- Davon Rohpunkte
- Davon On-Top-Punkte

Hierarchie-Idee:
1. Team-Kontext (Odds, Matchup) beeinflusst Spielausgangsraum.
2. Spieler-Verfuegbarkeit (Kickbase + LigaInsider) beeinflusst Start/Bench/Sub.
3. Spielzeit beeinflusst Basispunkte.
4. Punkte werden in Rohpunkte + On-Top-Punkte getrennt prognostiziert.
5. Gesamtpunkte ergeben sich aus diesen Teilmodellen.

## 2) Datenfluss Ende-zu-Ende
```
Bronze (Kickbase, LigaInsider, Odds)
  -> Silver.player_snapshot
  -> Silver.team_matchup_snapshot
  -> Gold feature tables (baseline)
  -> Postgres RAW history (market value, match, events)
  -> ML Training/CV (sklearn + torch auf gleichem Preprocessing)
  -> Predictions + Components (lokal erzeugt)
  -> BigQuery MARTS
  -> Power BI
```

## 3) Aktueller Implementierungsstand (dieser Schritt)
Neu umgesetzt:
- `src/preprocessing.py`
- `src/features_engineering.py`
- `src/models.py`
- `src/nn_models.py`

Diese Module liefern zusammen:
- gemeinsame Input-Matrix fuer sklearn und PyTorch
- Event-Split in Rohpunkte vs On-Top-Punkte
- sklearn-Hierarchie mit Modellwahl per Time-CV
- PyTorch-Multitask-Modell mit denselben Inputs und Zielen

## 4) Modulverantwortung (klar getrennt)

### `src/preprocessing.py`
Verantwortung:
- laden der neuesten Silver-Snapshots
- Zusammenfuehren von Spieler- und Team-Snapshot
- Ableitung von Priors aus Kickbase/LigaInsider (Start/Sub/Minuten)
- Erzeugung der gemeinsamen Feature-Transformation (ColumnTransformer)
- Time-based Splits fuer CV

Wichtig fuer Vergleichbarkeit:
- derselbe Preprocessor wird fuer sklearn und torch verwendet
- keine getrennten Feature-Pfade pro Modellfamilie

### `src/features_engineering.py`
Verantwortung:
- Erzeugung domaenenspezifischer Features (Availability, Odds-Umfeld, Value-Signale)
- Ableitung historischer Targets aus RAW-History
- eventbasierter Split: `target_raw_points` und `target_on_top_points`
- Team-Konsistenzfunktionen (z. B. Summe Startwahrscheinlichkeiten)

Historische Zielvariablen:
- `target_started`
- `target_subbed_in`
- `target_minutes`
- `target_raw_points`
- `target_on_top_points`
- `target_points_total`

### `src/models.py`
Verantwortung:
- sklearn-Training pro Zielvariable
- Kandidatenvergleich per Time-CV (RMSE fuer Regression, Brier fuer Wahrscheinlichkeiten)
- Auswahl des besten Modells je Target
- hierarchische Endvorhersage fuer naechstes Spiel
- Evaluationsfunktionen auf Holdout/CV

Model-Kandidaten (sklearn):
- Regression: `Ridge`, `RandomForestRegressor`, `HistGradientBoostingRegressor`
- Klassifikation: `LogisticRegression`, `RandomForestClassifier`, `HistGradientBoostingClassifier`

### `src/nn_models.py`
Verantwortung:
- PyTorch-Multitask-Netz mit Shared Backbone und 5 Heads
- Heads: Start, Sub, Minuten, Rohpunkte, On-Top-Punkte
- Early Stopping und Loss-Historie
- Vorhersageformat analog zu sklearn fuer 1:1 Vergleich

## 5) Historische CV-Logik

### Trainingsmodus (echte Historie)
Wir trainieren auf historischen Reihen aus Postgres RAW.

### Bewertungsmodus (walk-forward)
Time-based CV ohne Leakage:
- alte Daten = Train
- spaetere Daten = Validation
- mehrere Rolling-Folds

### Oracle-Regeln fuer fairen Rueckblick
Fuer historische Simulationen duerfen wir Prognoseinputs ersetzen durch:
- Lineup-Prognose -> tatsaechlich gestartet/eingewechselt
- Betting-Odds -> reales Match-Resultat (nur fuer retrospektive Gueteanalyse)

Damit messen wir sauber:
- Modellfehler
- Datenfehler
- Unsicherheitsanteil aus ex-ante Inputs

## 6) Aktuelle Baseline-Performance (bereits vorhanden)
Quelle: `data/backtesting/backtest_summary_2026-02-23T082437Z.csv`

Baseline-Gesamt (heuristische Gold-Praediktion):
- Sample: 337
- MAE: 16.2105
- RMSE: 23.8240
- Bias: -13.9256

Interpretation:
- System unterschaetzt aktuell im Schnitt (negativer Bias)
- naechster Schritt ist kalibrierte hierarchische ML-Pipeline (dieses Dokument)

## 7) Konsistenzregeln fuer realistische Outputs
Nach Modellpredictions wird eine Konsistenzschicht angewendet:
- Team/Match: Startwahrscheinlichkeiten so skalieren, dass Summe <= 11
- Team/Match: Sub-Wahrscheinlichkeiten so skalieren, dass Summe <= 5
- pro Spieler: `start_probability + sub_probability <= 1`
- Teamebene: optional Goal-Cap anhand Odds-Kontext

Hinweis:
Das ist Post-Processing auf Prediction-Ebene, keine Veraenderung der RAW-Daten.

## 8) Wie wir morgen messen und berichten
Pro Trainingslauf speichern wir:
- Split-Definition (Datum/Fold)
- Modellname je Target
- CV-Metriken je Target
- Holdout-Metriken je Target
- Vergleich sklearn vs torch je Target

Metriken je Target:
- Wahrscheinlichkeiten: Brier Score
- Punkte/Minuten: MAE, RMSE, Bias

Zusaetzlich fuer Produktsicht:
- MAE/RMSE auf `expected_points_next_match`
- Anteil Top-N Treffer (Ranking-Qualitaet)

## 9) Geplanter Integrationspfad in Pipeline
1. Postgres RAW stabil halten (keine Aggregationstabellen).
2. Silver/Gold weiterhin als produktionsnahe Inferenzdaten nutzen.
3. ML-Training auf historischen RAW-Exports + Silver-Features.
4. Modelloutputs ausschliesslich nach BigQuery RAW schreiben (kein Rueckschreiben nach Databricks Gold):
   - `start_probability`
   - `sub_probability`
   - `expected_minutes`
   - `expected_points_next_match`
   - `expected_raw_points_next_match`
   - `expected_on_top_points_next_match`
5. Power BI konsumiert nur MARTS/serving Tabellen.

## 10) To-do (naechster konkreter Schritt)
Erledigt:
- Trainingsscript mit sklearn + torch Vergleich je Fold ist umgesetzt (`scripts/ml/train_hierarchical_models.py`).
- Champion-Selektion aus CV-Metriken ist umgesetzt (`champion_selection.json`, `live_predictions_champion.csv`).
- ML->BigQuery RAW Export/Load ist umgesetzt (`scripts/ml/run_ml_bigquery_pipeline.py`).
- Scheduler fuer regelmaessige ML-Runs ist umgesetzt (`scripts/ml/run_ml_pipeline_scheduler.py`).

Offen:
- Feature-Gruppen fuer Teammodell (Siegwahrscheinlichkeit) explizit erweitern.
- Calibration-Layer fuer Wahrscheinlichkeiten (isotonic/platt) evaluieren.
- Champion-Auswahl optional pro Target statt global nur ueber `metric_points_total_rmse`.
- Monitoring-Report um Drift-/Coverage-Checks erweitern.
