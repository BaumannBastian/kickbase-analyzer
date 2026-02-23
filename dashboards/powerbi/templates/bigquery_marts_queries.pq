// ------------------------------------
// bigquery_marts_queries.pq
//
// Power Query M Abfragen fuer BigQuery MARTS.
// Projekt und Dataset werden ueber Template-Platzhalter gesetzt.
// ------------------------------------

section KickbaseMarts;

shared ProjectId = "{project}";
shared MartsDataset = "{marts}";

shared Source = GoogleBigQuery.Database([BillingProject = ProjectId]);
shared ProjectData = Source{[Name = ProjectId]}[Data];
shared DatasetData = ProjectData{[Name = MartsDataset, Kind = "Schema"]}[Data];

shared mart_player_leaderboard =
    let
        viewOrTable =
            try DatasetData{[Name = "mart_player_leaderboard", Kind = "View"]}[Data]
            otherwise DatasetData{[Name = "mart_player_leaderboard", Kind = "Table"]}[Data]
    in
        viewOrTable;

shared mart_points_breakdown =
    let
        viewOrTable =
            try DatasetData{[Name = "mart_points_breakdown", Kind = "View"]}[Data]
            otherwise DatasetData{[Name = "mart_points_breakdown", Kind = "Table"]}[Data]
    in
        viewOrTable;

shared mart_risk_overview =
    let
        viewOrTable =
            try DatasetData{[Name = "mart_risk_overview", Kind = "View"]}[Data]
            otherwise DatasetData{[Name = "mart_risk_overview", Kind = "Table"]}[Data]
    in
        viewOrTable;

shared mart_marketvalue_dashboard =
    let
        viewOrTable =
            try DatasetData{[Name = "mart_marketvalue_dashboard", Kind = "View"]}[Data]
            otherwise DatasetData{[Name = "mart_marketvalue_dashboard", Kind = "Table"]}[Data]
    in
        viewOrTable;

shared mart_ml_player_predictions =
    let
        viewOrTable =
            try DatasetData{[Name = "mart_ml_player_predictions", Kind = "View"]}[Data]
            otherwise DatasetData{[Name = "mart_ml_player_predictions", Kind = "Table"]}[Data]
    in
        viewOrTable;

shared mart_ml_model_monitoring =
    let
        viewOrTable =
            try DatasetData{[Name = "mart_ml_model_monitoring", Kind = "View"]}[Data]
            otherwise DatasetData{[Name = "mart_ml_model_monitoring", Kind = "Table"]}[Data]
    in
        viewOrTable;
